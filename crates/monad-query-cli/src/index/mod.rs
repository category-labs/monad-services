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

use std::path::PathBuf;

use alloy_consensus::Transaction;
use alloy_eips::eip2718::Typed2718;
use alloy_primitives::U256;
use eyre::{ensure, Result, WrapErr};
use futures::{StreamExt, TryStreamExt};
use monad_archive::{metrics::Metrics, model::BlockDataReader, prelude::LatestKind};
use monad_query_schema::family::{
    StoredBlockQueryItem, StoredLogQueryItem, StoredTraceQueryItem, StoredTxQueryItem,
};
use monad_query_store::{blob::BlobStoreBackend, meta::MetaStoreBackend};
use monad_query_types::{store::blob::table::CodecBlobTable, QueryError, QueryResult};
use monad_query_write::{EvmBlock, EvmTx, QueryWriter};
use tokio::time::{sleep, Duration};
use tracing::{debug, info};

pub(super) use self::cli::MonadQueryCliIndexArgs;
use crate::shared::{self, config::MonadQueryServiceConfig, with_async_runtime, with_tracing};

mod cli;

type Writer = QueryWriter<
    CodecBlobTable<BlobStoreBackend>,
    MetaStoreBackend,
    MetaStoreBackend,
    CodecBlobTable<BlobStoreBackend>,
    MetaStoreBackend,
>;

pub(super) fn run(config: PathBuf, args: MonadQueryCliIndexArgs) -> Result<()> {
    with_tracing(|| with_async_runtime(|| run_async(config, args)))
}

async fn run_async(
    config: PathBuf,
    MonadQueryCliIndexArgs { block_data_source }: MonadQueryCliIndexArgs,
) -> Result<()> {
    let MonadQueryServiceConfig {
        storage,
        start_block,
        poll_ms,
        fetch_concurrency,
        fetch_batch_size,
        flush_every_blocks,
    } = MonadQueryServiceConfig::load(&config).await?;

    let mut writer = QueryWriter::open(shared::open_tables(storage, false).await?)
        .await
        .map_err(|error| eyre::eyre!("open query writer: {error}"))?;

    let reader = block_data_source
        .build(&Metrics::none())
        .await
        .wrap_err("build block data reader")?;

    if let Some(indexed_start) = writer.indexed_start() {
        if let Some(start_block) = start_block {
            ensure!(
                start_block == indexed_start,
                "configured start block {start_block} does not match indexed start block {indexed_start}"
            );
        }
    } else {
        writer
            .start_at(start_block.unwrap_or(0))
            .map_err(|error| eyre::eyre!("set index start block: {error}"))?;
    }

    let mut next_block = writer
        .next_block_number()
        .map_err(|error| eyre::eyre!("read next index block: {error}"))?;

    loop {
        let Some(latest_block) = reader
            .get_latest(LatestKind::Uploaded)
            .await
            .wrap_err("read latest uploaded block")?
        else {
            sleep(Duration::from_millis(poll_ms)).await;
            continue;
        };

        if next_block <= latest_block {
            next_block = index_range(
                &reader,
                &mut writer,
                next_block,
                latest_block,
                fetch_concurrency,
                fetch_batch_size,
                flush_every_blocks,
            )
            .await?;
            info!(
                indexed_through = next_block - 1,
                latest_block, "indexed blocks"
            );
        } else {
            sleep(Duration::from_millis(poll_ms)).await;
        }
    }
}

async fn index_range(
    reader: &impl BlockDataReader,
    writer: &mut Writer,
    mut next_block: u64,
    latest_block: u64,
    fetch_concurrency: usize,
    fetch_batch_size: usize,
    flush_every_blocks: usize,
) -> Result<u64> {
    ensure!(
        fetch_batch_size > 0,
        "fetch batch size must be greater than zero"
    );
    ensure!(
        flush_every_blocks > 0,
        "flush interval must be greater than zero"
    );
    let batch_size = u64::try_from(fetch_batch_size)
        .map_err(|_| eyre::eyre!("fetch batch size does not fit in a block number"))?;
    let mut pending_blocks = 0;

    while next_block <= latest_block {
        let batch_end = next_block
            .checked_add(batch_size - 1)
            .unwrap_or(latest_block)
            .min(latest_block);
        info!(
            first_block = next_block,
            last_block = batch_end,
            count = batch_end - next_block + 1,
            "fetching block batch"
        );
        let blocks = futures::stream::iter(next_block..=batch_end)
            .map(|block_number| read_finalized_block(reader, block_number))
            .buffered(fetch_concurrency)
            .try_collect::<Vec<_>>()
            .await?;

        for block in blocks {
            let block_number = block.block_number();
            writer
                .append(block)
                .map_err(|error| eyre::eyre!("append block {block_number}: {error}"))?;
            next_block = next_block
                .checked_add(1)
                .ok_or_else(|| eyre::eyre!("block number overflow"))?;
            pending_blocks += 1;
            debug!(block_number, pending_blocks, "appended block");
            if pending_blocks >= flush_every_blocks {
                info!(pending_blocks, "flushing indexed blocks");
                writer
                    .flush()
                    .await
                    .map_err(|error| eyre::eyre!("flush query writer: {error}"))?;
                pending_blocks = 0;
            }
        }
    }

    if pending_blocks != 0 {
        writer
            .flush()
            .await
            .map_err(|error| eyre::eyre!("flush query writer: {error}"))?;
    }
    Ok(next_block)
}

async fn read_finalized_block(
    reader: &impl BlockDataReader,
    block_number: u64,
) -> Result<EvmBlock> {
    debug!(block_number, "reading block");
    let data = reader
        .get_block_data_with_offsets(block_number)
        .await
        .wrap_err_with(|| format!("read block {block_number} data"))?;
    let mut block_for_size = data.block.clone().map_transactions(|tx| tx.tx);
    block_for_size.body.withdrawals = None;
    let block_size = alloy_rlp::encode(&block_for_size).len() as u64;
    let traces = monad_archive::model::block_data_archive::decode_traces(&data.traces)
        .wrap_err("decode block traces")?;

    ensure!(
        data.block.body.transactions.len() == data.receipts.len(),
        "block transaction and receipt counts differ"
    );
    ensure!(
        traces.len() == data.block.body.transactions.len(),
        "block transaction and trace counts differ"
    );

    let mut previous_cumulative_gas_used = 0u64;
    let mut next_log_index = 0u32;
    let txs = data
        .block
        .body
        .transactions
        .into_iter()
        .zip(data.receipts)
        .zip(traces)
        .enumerate()
        .map(|(tranasction_index, ((tx, receipt), trace_groups))| {
            let transaction_index = u32::try_from(tranasction_index)
                .map_err(|_| eyre::eyre!("transaction index exceeds u32"))?;
            let cumulative_gas_used = receipt.receipt.cumulative_gas_used();
            let gas_used = cumulative_gas_used
                .checked_sub(previous_cumulative_gas_used)
                .ok_or_else(|| eyre::eyre!("receipt cumulative gas used decreases"))?;
            previous_cumulative_gas_used = cumulative_gas_used;
            let signature = tx.tx.signature();
            let frames = trace_groups.into_iter().flatten().collect::<Vec<_>>();
            let depths = frames
                .iter()
                .map(|frame| frame.depth.to::<u32>())
                .collect::<Vec<_>>();
            let trace_addresses = compute_trace_addresses(&depths)?;
            let statuses = frames
                .iter()
                .map(|frame| frame.status.to::<u8>())
                .collect::<Vec<_>>();
            let reverted =
                compute_trace_reverted(&statuses, receipt.receipt.status(), &trace_addresses);

            let traces = frames
                .into_iter()
                .zip(trace_addresses)
                .zip(reverted)
                .map(|((frame, trace_address), reverted)| {
                    let gas = u64::try_from(frame.gas)
                        .map_err(|_| eyre::eyre!("trace gas exceeds u64"))?;
                    let gas_used = u64::try_from(frame.gas_used)
                        .map_err(|_| eyre::eyre!("trace gas used exceeds u64"))?;
                    Ok(StoredTraceQueryItem {
                        transaction_hash: *tx.tx.tx_hash(),
                        transaction_index,
                        trace_address: trace_address.into_boxed_slice(),
                        r#type: match frame.typ {
                            monad_archive::model::block_data_archive::CallKind::Call => {
                                monad_query_types::CallKind::Call
                            }
                            monad_archive::model::block_data_archive::CallKind::StaticCall => {
                                monad_query_types::CallKind::StaticCall
                            }
                            monad_archive::model::block_data_archive::CallKind::DelegateCall => {
                                monad_query_types::CallKind::DelegateCall
                            }
                            monad_archive::model::block_data_archive::CallKind::CallCode => {
                                monad_query_types::CallKind::CallCode
                            }
                            monad_archive::model::block_data_archive::CallKind::Create => {
                                monad_query_types::CallKind::Create
                            }
                            monad_archive::model::block_data_archive::CallKind::Create2 => {
                                monad_query_types::CallKind::Create2
                            }
                            monad_archive::model::block_data_archive::CallKind::SelfDestruct => {
                                monad_query_types::CallKind::SelfDestruct
                            }
                        },
                        from: frame.from,
                        to: frame.to,
                        value: frame.value,
                        gas,
                        gas_used,
                        input: frame.input,
                        output: frame.output,
                        status: frame.status.to::<u8>(),
                        reverted,
                    })
                })
                .collect::<Result<Vec<_>>>()?
                .into_boxed_slice();

            Ok(EvmTx {
                envelope: StoredTxQueryItem {
                    block_timestamp: data.block.header.timestamp,
                    transaction_hash: *tx.tx.tx_hash(),
                    sender: tx.sender,
                    nonce: tx.tx.nonce(),
                    tx_type: tx.tx.tx_type().ty() as u8,
                    input: tx.tx.input().clone(),
                    value: tx.tx.value(),
                    gas: tx.tx.gas_limit(),
                    v: U256::from(alloy_consensus::transaction::to_eip155_value(
                        signature.v(),
                        tx.tx.chain_id(),
                    )),
                    r: signature.r(),
                    s: signature.s(),
                    logs_bloom: *receipt.receipt.logs_bloom(),
                    status: receipt.receipt.status(),
                    gas_used,
                    cumulative_gas_used,
                    effective_gas_price: U256::from(
                        tx.tx
                            .effective_gas_price(data.block.header.base_fee_per_gas),
                    ),
                    chain_id: tx.tx.chain_id(),
                    to: tx.tx.to(),
                    gas_price: Some(U256::from(
                        tx.tx
                            .effective_gas_price(data.block.header.base_fee_per_gas),
                    )),
                    max_fee_per_gas: Some(U256::from(tx.tx.max_fee_per_gas())),
                    max_priority_fee_per_gas: tx.tx.max_priority_fee_per_gas().map(U256::from),
                    max_fee_per_blob_gas: tx.tx.max_fee_per_blob_gas().map(U256::from),
                    access_list: tx.tx.access_list().cloned(),
                    authorization_list: tx.tx.authorization_list().map(ToOwned::to_owned),
                    blob_versioned_hashes: tx.tx.blob_versioned_hashes().map(ToOwned::to_owned),
                    contract_address: matches!(tx.tx.kind(), alloy_primitives::TxKind::Create)
                        .then(|| tx.sender.create(tx.tx.nonce())),
                },
                logs: receipt
                    .receipt
                    .logs()
                    .iter()
                    .map(|log| {
                        let log_index = next_log_index;
                        next_log_index = next_log_index
                            .checked_add(1)
                            .ok_or_else(|| eyre::eyre!("log index exceeds u32"))?;
                        Ok(StoredLogQueryItem {
                            block_timestamp: data.block.header.timestamp,
                            transaction_hash: *tx.tx.tx_hash(),
                            transaction_index,
                            log_index,
                            address: log.address,
                            topics: log.data.topics().to_vec(),
                            data: log.data.data.clone(),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?
                    .into_boxed_slice(),
                traces,
            })
        })
        .collect::<Result<Vec<_>>>()?
        .into_boxed_slice();

    let block = EvmBlock {
        header: StoredBlockQueryItem {
            header: data.block.header,
            size: block_size,
            total_difficulty: U256::ZERO,
        },
        txs,
    };
    debug!(
        block_number,
        transactions = block.txs.len(),
        "decoded block"
    );
    Ok(block)
}

fn compute_trace_reverted(
    statuses: &[u8],
    transaction_status: bool,
    trace_addresses: &[Vec<u32>],
) -> Vec<bool> {
    let mut failed_ancestors = Vec::new();

    statuses
        .iter()
        .zip(trace_addresses)
        .map(|(&status, trace_address)| {
            let depth = trace_address.len();
            let ancestor_reverted = depth
                .checked_sub(1)
                .and_then(|index| failed_ancestors.get(index).copied())
                .unwrap_or(false);

            failed_ancestors.truncate(depth);
            failed_ancestors.resize(depth + 1, false);
            failed_ancestors[depth] = ancestor_reverted || status != 0;

            ancestor_reverted || status != 0 || !transaction_status
        })
        .collect()
}

fn compute_trace_addresses(depths: &[u32]) -> QueryResult<Vec<Vec<u32>>> {
    struct State {
        path: Vec<u32>,
        sibling_counters: Vec<u32>,
        addresses: Vec<Vec<u32>>,
        seen_root: bool,
    }

    depths
        .iter()
        .try_fold(
            State {
                path: Vec::new(),
                sibling_counters: Vec::new(),
                addresses: Vec::new(),
                seen_root: false,
            },
            |mut state, &depth| {
                let depth = depth as usize;
                if depth == 0 {
                    state.path.clear();
                    state.sibling_counters.clear();
                    state.addresses.push(Vec::new());
                    state.seen_root = true;
                    return Ok(state);
                }
                if !state.seen_root {
                    return Err(QueryError::InvalidBlock(
                        "trace_address: first frame must have depth 0",
                    ));
                }
                if depth > state.path.len() + 1 {
                    return Err(QueryError::InvalidBlock(
                        "trace_address: depth skipped a level",
                    ));
                }

                state.path.truncate(depth - 1);
                state.sibling_counters.truncate(depth);
                if state.sibling_counters.len() < depth {
                    state.sibling_counters.resize(depth, 0);
                }
                state.path.push(state.sibling_counters[depth - 1]);
                state.sibling_counters[depth - 1] += 1;
                state.addresses.push(state.path.clone());
                Ok(state)
            },
        )
        .map(|state| state.addresses)
}
