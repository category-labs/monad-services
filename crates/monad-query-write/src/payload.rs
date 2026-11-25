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
use std::{collections::BTreeMap, time::Instant};

use alloy_rlp::encode;
use futures::future::LocalBoxFuture;
use monad_query_schema::{
    family::Family,
    tables::{
        payload::{
            location::{PayloadLocation, PayloadLocationKey, PayloadLocationSchema},
            payload::{PayloadItem, PayloadKey},
        },
        state::index::StateIndex,
    },
};
use monad_query_types::{
    store::{
        blob::{
            partition::{PartitionSchemaBlobTable, PartitionedBlobTable},
            table::BlobTable,
        },
        meta::{
            table::{MetaTable, MetaTablePut},
            MetaStore,
        },
    },
    PrimaryId, QueryError, QueryResult,
};
use strum::IntoEnumIterator;

use crate::{EvmBlock, FlushBatch};

pub(crate) fn flush<'a, M, B>(
    payload: &'a PartitionSchemaBlobTable<B, PayloadKey, PayloadItem>,
    location: &'a MetaTable<M, PayloadLocationSchema>,
    committed_state: &'a StateIndex,
    batch: &'a FlushBatch,
) -> LocalBoxFuture<'a, QueryResult<()>>
where
    M: MetaStore,
    B: BlobTable,
{
    Box::pin(async move {
        let mut family_locations = Vec::new();
        for family in Family::iter() {
            let items = batch
                .blocks
                .iter()
                .flat_map(|a| {
                    let (first, count) = match family {
                        Family::BlockHeader => (PrimaryId::new(a.block.block_number()), 1),
                        Family::Transaction => (a.txs.first_primary_id, a.txs.count),
                        Family::Log => (a.logs.first_primary_id, a.logs.count),
                        Family::Trace => (a.traces.first_primary_id, a.traces.count),
                    };
                    (0..count).map(move |i| {
                        let id = PrimaryId::new(first.as_u64() + u64::from(i));
                        (
                            PayloadKey::from_id(family, id.as_u64()),
                            PayloadItem {
                                bytes: row_bytes(&a.block, family, i as usize).into(),
                            },
                            id,
                        )
                    })
                })
                .collect::<Vec<_>>();
            let item_count = items.len();
            let partition_count = items
                .iter()
                .map(|(key, _, _)| key.clone())
                .collect::<std::collections::HashSet<_>>()
                .len();
            let started = Instant::now();
            let mut groups: BTreeMap<_, (Vec<_>, Vec<_>)> = BTreeMap::new();
            for (key, value, id) in items {
                let (ids, values) = groups
                    .entry(key)
                    .or_insert_with(|| (Vec::new(), Vec::new()));
                ids.push(id);
                values.push(value);
            }

            let mut grouped_items = BTreeMap::new();
            let mut grouped_ids = BTreeMap::new();
            for (key, (ids, values)) in groups {
                let first_id = ids[0];
                let committed_id = match family {
                    Family::BlockHeader => {
                        committed_state.next_block_id.checked_sub(1).filter(|head| {
                            committed_state
                                .indexed_start
                                .is_none_or(|start| *head >= start)
                        })
                    }
                    Family::Transaction => committed_state.next_tx_id.checked_sub(1),
                    Family::Log => committed_state.next_log_id.checked_sub(1),
                    Family::Trace => committed_state.next_trace_id.checked_sub(1),
                };
                let committed_end = match committed_id {
                    Some(committed_id)
                        if PayloadKey::from_id(family, committed_id)
                            == PayloadKey::from_id(family, first_id.as_u64()) =>
                    {
                        match location
                            .get(&PayloadLocationKey {
                                family,
                                id: PrimaryId::new(committed_id),
                            })
                            .await?
                        {
                            Some(location) => location.payload_end(),
                            None => {
                                tracing::error!(
                                    ?family,
                                    committed_id,
                                    "committed payload location missing: metadata cursor \
                                     is ahead of durable payload; halting for repair"
                                );
                                return Err(QueryError::Diverged(
                                    "committed payload location missing; cursor ahead of durable payload",
                                ));
                            }
                        }
                    }
                    _ => 0,
                };
                grouped_ids.insert(key.clone(), ids.into_boxed_slice());
                grouped_items.insert(key, (committed_end, values.into_boxed_slice()));
            }

            let ends = payload.append_batch(grouped_items).await?;
            for (key, end_values) in ends {
                let ids = grouped_ids.remove(&key).expect("payload group has IDs");
                if ids.len() != end_values.len() {
                    return Err(QueryError::Internal(
                        "payload append returned mismatched item counts",
                    ));
                }
                family_locations.extend(ids.into_vec().into_iter().zip(end_values).map(
                    |(id, end)| MetaTablePut {
                        key: PayloadLocationKey { family, id },
                        value: PayloadLocation::new(end),
                    },
                ));
            }

            tracing::info!(
                ?family,
                items = item_count,
                partitions = partition_count,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "payload partitions flush complete"
            );
        }
        let location_count = family_locations.len();
        let started = Instant::now();
        location
            .put_batch(family_locations.into_boxed_slice())
            .await?;
        tracing::info!(
            rows = location_count,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "payload locations flush complete"
        );
        Ok(())
    })
}

fn row_bytes(block: &EvmBlock, family: Family, index: usize) -> Vec<u8> {
    match family {
        Family::BlockHeader => encode(&block.header),
        Family::Transaction => encode(&block.txs[index].envelope),
        Family::Log => {
            let mut remaining = index;
            for (tx_index, tx) in block.txs.iter().enumerate() {
                if remaining < tx.logs.len() {
                    let log = &tx.logs[remaining];
                    return encode(log);
                }
                remaining -= tx.logs.len();
            }
            unreachable!("log index")
        }
        Family::Trace => {
            let trace = block
                .txs
                .iter()
                .flat_map(|tx| tx.traces.iter())
                .nth(index)
                .expect("trace index");
            encode(trace)
        }
    }
}
