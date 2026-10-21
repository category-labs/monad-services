// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::time::Instant;

use alloy_primitives::B256;
use monad_query_schema::{
    family::FamilyMetadata,
    tables::{
        bitmap::BitmapTables,
        metadata::{block_hash::MetadataBlockHashKey, MetadataTables},
        payload::PayloadTables,
        state::index::{StateIndex, StateIndexKey},
        QueryTables,
    },
};
use monad_query_types::{
    store::{blob::table::BlobTable, meta::MetaStore},
    PrimaryId, QueryError, QueryResult,
};

pub use self::block::{EvmBlock, EvmTx};

mod bitmap_index;
mod block;
mod metadata;
mod payload;

pub struct QueryWriter<BM, MM, PM, PB, SM>
where
    BM: BlobTable,
    MM: MetaStore,
    PM: MetaStore,
    PB: BlobTable,
    SM: MetaStore,
{
    tables: QueryTables<BM, MM, PM, PB, SM>,

    state: StateIndex,
    last_block_hash: Option<B256>,

    pending: Vec<EvmBlock>,
}

impl<BM, MM, PM, PB, SM> QueryWriter<BM, MM, PM, PB, SM>
where
    BM: BlobTable,
    MM: MetaStore,
    PM: MetaStore,
    PB: BlobTable,
    SM: MetaStore,
{
    pub async fn open(tables: QueryTables<BM, MM, PM, PB, SM>) -> QueryResult<Self> {
        tables.init().await?;
        let state = tables
            .state
            .index
            .get(&StateIndexKey)
            .await?
            .unwrap_or(StateIndex {
                indexed_start: None,
                next_block_id: 0,
                next_tx_id: 0,
                next_log_id: 0,
                next_trace_id: 0,
            });

        let last_block_hash = match state.next_block_id.checked_sub(1) {
            Some(block_height) => Some(
                tables
                    .metadata
                    .block_hash
                    .get(&MetadataBlockHashKey { block_height })
                    .await?
                    .ok_or(QueryError::MissingData(
                        "indexed head is missing block hash",
                    ))?
                    .0,
            ),
            None => None,
        };

        Ok(Self {
            tables,
            state,
            last_block_hash,
            pending: Vec::new(),
        })
    }

    pub fn indexed_start(&self) -> Option<u64> {
        self.state.indexed_start
    }

    pub fn next_block_number(&self) -> QueryResult<u64> {
        self.state
            .next_block_id
            .checked_add(self.pending.len() as u64)
            .ok_or(QueryError::InvalidBlock("block number overflow"))
    }

    pub async fn flush(&mut self) -> QueryResult<()> {
        let Some(batch) = self.prepare_flush()? else {
            return Ok(());
        };

        let QueryTables {
            bitmap: bitmap_index,
            metadata,
            payload,
            state: _,
        } = &self.tables;
        let MetadataTables {
            block_hash,
            family,
            family_block_index,
        } = metadata;
        let PayloadTables {
            payload,
            payload_location,
        } = payload;
        let BitmapTables {
            index,
            page_stats: stats,
        } = bitmap_index;

        let flush_started = Instant::now();
        let block_count = batch.blocks.len();

        let started = Instant::now();
        let result = metadata::flush(block_hash, family, family_block_index, &batch).await;
        tracing::info!(
            stage = "metadata",
            blocks = block_count,
            elapsed_ms = started.elapsed().as_millis() as u64,
            success = result.is_ok(),
            "indexer flush stage complete"
        );
        result?;

        let started = Instant::now();
        let result = payload::flush(payload, payload_location, &self.state, &batch).await;
        tracing::info!(
            stage = "payload",
            blocks = block_count,
            elapsed_ms = started.elapsed().as_millis() as u64,
            success = result.is_ok(),
            "indexer flush stage complete"
        );
        result?;

        let started = Instant::now();
        let result = bitmap_index::flush(index, stats, &batch).await;
        tracing::info!(
            stage = "bitmap",
            blocks = block_count,
            elapsed_ms = started.elapsed().as_millis() as u64,
            success = result.is_ok(),
            "indexer flush stage complete"
        );
        result?;

        tracing::info!(
            blocks = block_count,
            elapsed_ms = flush_started.elapsed().as_millis() as u64,
            "indexer flush complete"
        );

        self.tables
            .state
            .index
            .put(&StateIndexKey, &batch.state)
            .await?;
        self.last_block_hash = batch
            .blocks
            .last()
            .map(|assigned| assigned.block.block_hash());
        self.state = batch.state;
        self.pending.clear();
        Ok(())
    }

    pub fn start_at(&mut self, block_number: u64) -> QueryResult<()> {
        if self.state.indexed_start.is_some() || !self.pending.is_empty() {
            return Err(QueryError::InvalidBlock("writer already contains blocks"));
        }
        self.state.indexed_start = Some(block_number);
        self.state.next_block_id = block_number;
        Ok(())
    }

    pub fn append(&mut self, block: EvmBlock) -> QueryResult<()> {
        if block.block_number() != self.next_block_number()? {
            return Err(QueryError::InvalidBlock(
                "finalized blocks must be appended in order",
            ));
        }

        if let Some(previous) = self
            .pending
            .last()
            .map(EvmBlock::block_hash)
            .or(self.last_block_hash)
        {
            if block.parent_hash() != previous {
                return Err(QueryError::InvalidBlock(
                    "finalized block does not extend the indexed chain",
                ));
            }
        }

        self.pending.push(block);

        Ok(())
    }

    fn prepare_flush(&self) -> QueryResult<Option<FlushBatch>> {
        if self.pending.is_empty() {
            return Ok(None);
        }

        let next_block = self.next_block_number()?;
        let mut next_tx = self.state.next_tx_id;
        let mut next_log = self.state.next_log_id;
        let mut next_trace = self.state.next_trace_id;

        let mut blocks = Vec::with_capacity(self.pending.len());

        for block in &self.pending {
            let txs = range(next_tx, block.txs.len())?;
            let logs = range(next_log, block.txs.iter().map(|tx| tx.logs.len()).sum())?;
            let traces = range(next_trace, block.txs.iter().map(|tx| tx.traces.len()).sum())?;

            next_tx = next_tx
                .checked_add(u64::from(txs.count))
                .ok_or(QueryError::InvalidBlock("transaction ID overflow"))?;
            next_log = next_log
                .checked_add(u64::from(logs.count))
                .ok_or(QueryError::InvalidBlock("log ID overflow"))?;
            next_trace = next_trace
                .checked_add(u64::from(traces.count))
                .ok_or(QueryError::InvalidBlock("trace ID overflow"))?;

            blocks.push(AssignedBlock {
                block: block.clone(),
                logs,
                txs,
                traces,
            });
        }

        Ok(Some(FlushBatch {
            blocks,
            state: StateIndex {
                indexed_start: self
                    .state
                    .indexed_start
                    .or_else(|| self.pending.first().map(EvmBlock::block_number)),
                next_block_id: next_block,
                next_tx_id: next_tx,
                next_log_id: next_log,
                next_trace_id: next_trace,
            },
        }))
    }
}

pub(crate) struct FlushBatch {
    pub(crate) blocks: Vec<AssignedBlock>,
    pub(crate) state: StateIndex,
}

pub(crate) struct AssignedBlock {
    pub(crate) block: EvmBlock,
    pub(crate) logs: FamilyMetadata,
    pub(crate) txs: FamilyMetadata,
    pub(crate) traces: FamilyMetadata,
}

fn range(first: u64, count: usize) -> QueryResult<FamilyMetadata> {
    Ok(FamilyMetadata {
        first_primary_id: PrimaryId::new(first),
        count: count
            .try_into()
            .map_err(|_| QueryError::InvalidBlock("block contains too many records"))?,
    })
}
