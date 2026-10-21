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
use std::{collections::HashMap, time::Instant};

use futures::{future::LocalBoxFuture, StreamExt, TryStreamExt};
use monad_query_schema::{
    bitmap::{BitmapFilterKey, BitmapFilterValue},
    family::{BitmapIndexable, Family},
    tables::bitmap::{
        index::{BitmapIndexKey, BitmapIndexValue},
        page_stats::{BitmapPageStatsKey, BitmapPageStatsSchema},
    },
};
use monad_query_types::{
    store::{
        blob::table::{BlobTable, BlobTablePutBatch, SchemaBlobTable},
        meta::{
            table::{MetaTable, MetaTablePut},
            MetaStore,
        },
    },
    PrimaryId, QueryResult,
};
use roaring::RoaringBitmap;
use strum::IntoEnumIterator;

use crate::{AssignedBlock, FlushBatch};

pub(crate) fn flush<'a, M, S>(
    table: &'a SchemaBlobTable<M, BitmapIndexKey, BitmapIndexValue>,
    stats: &'a MetaTable<S, BitmapPageStatsSchema>,
    batch: &'a FlushBatch,
) -> LocalBoxFuture<'a, QueryResult<()>>
where
    M: BlobTable,
    S: MetaStore,
{
    Box::pin(async move {
        let mut pages: HashMap<(Family, BitmapFilterValue, u64), RoaringBitmap> = HashMap::new();
        for assigned in &batch.blocks {
            accumulate(assigned, &mut pages)?;
        }

        tracing::info!(pages = pages.len(), "bitmap updates accumulated");
        let frontier = Family::iter()
            .map(|family| (family, batch_state_frontier(batch, family)))
            .collect::<HashMap<_, _>>();
        let updates = futures::stream::iter(pages)
            .map(|((family, filter, page), pending)| {
                let committed = frontier[&family];
                async move {
                    let key = BitmapIndexKey {
                        family,
                        filter: BitmapFilterKey {
                            value: filter.clone(),
                        },
                        page,
                    };
                    let mut bitmap = if committed <= page {
                        RoaringBitmap::new()
                    } else {
                        let mut bitmap = table
                            .get(&key)
                            .await?
                            .map(|value| value.bitmap)
                            .unwrap_or_default();
                        if committed < page.saturating_add(BitmapIndexValue::PAGE_SPAN) {
                            bitmap.remove_range((committed - page) as u32..);
                        }
                        bitmap
                    };
                    bitmap |= pending;
                    let stats = (!bitmap.is_empty()).then(|| MetaTablePut {
                        key: BitmapPageStatsKey {
                            family,
                            filter: filter.clone(),
                            page,
                        },
                        value: bitmap.len() as u32,
                    });
                    Ok::<_, monad_query_types::QueryError>((
                        BlobTablePutBatch {
                            key,
                            value: BitmapIndexValue {
                                id_start: PrimaryId::new(page),
                                id_end: PrimaryId::new(page + BitmapIndexValue::PAGE_SPAN),
                                bitmap,
                            },
                        },
                        stats,
                    ))
                }
            })
            .buffer_unordered(32)
            .try_collect::<Vec<_>>()
            .await?;
        let (index_writes, stats_writes): (Vec<_>, Vec<_>) = updates
            .into_iter()
            .map(|(index, stats)| (index, stats.into_iter()))
            .unzip();
        let index_count = index_writes.len();
        let mut stats_writes: Vec<MetaTablePut<BitmapPageStatsSchema>> =
            stats_writes.into_iter().flatten().collect();
        stats_writes.sort_unstable_by(|left, right| {
            left.key
                .family
                .cmp(&right.key.family)
                .then_with(|| left.key.filter.cmp(&right.key.filter))
                .then_with(|| left.key.page.cmp(&right.key.page))
        });
        let stats_count = stats_writes.len();
        let started = Instant::now();
        table.put_batch(index_writes.into_boxed_slice()).await?;
        tracing::info!(
            rows = index_count,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "bitmap index flush complete"
        );
        let started = Instant::now();
        stats.put_batch(stats_writes.into_boxed_slice()).await?;
        tracing::info!(
            rows = stats_count,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "bitmap stats flush complete"
        );
        Ok(())
    })
}

fn batch_state_frontier(batch: &FlushBatch, family: Family) -> u64 {
    let (next, count) = match family {
        Family::BlockHeader => (
            batch
                .blocks
                .first()
                .map_or(0, |block| block.block.block_number()),
            0,
        ),
        Family::Transaction => (
            batch.state.next_tx_id,
            batch
                .blocks
                .iter()
                .map(|block| u64::from(block.txs.count))
                .sum::<u64>(),
        ),
        Family::Log => (
            batch.state.next_log_id,
            batch
                .blocks
                .iter()
                .map(|block| u64::from(block.logs.count))
                .sum::<u64>(),
        ),
        Family::Trace => (
            batch.state.next_trace_id,
            batch
                .blocks
                .iter()
                .map(|block| u64::from(block.traces.count))
                .sum::<u64>(),
        ),
    };
    next - count
}

fn accumulate(
    assigned: &AssignedBlock,
    pages: &mut HashMap<(Family, BitmapFilterValue, u64), RoaringBitmap>,
) -> QueryResult<()> {
    for filter in assigned.block.header.bitmap_filters()? {
        add(
            pages,
            Family::BlockHeader,
            filter,
            assigned.block.block_number(),
        );
    }
    let mut log_id = assigned.logs.first_primary_id.as_u64();
    let mut trace_id = assigned.traces.first_primary_id.as_u64();

    for (tx_index, tx) in assigned.block.txs.iter().enumerate() {
        for filter in tx.envelope.bitmap_filters()? {
            add(
                pages,
                Family::Transaction,
                filter,
                assigned.txs.first_primary_id.as_u64() + tx_index as u64,
            );
        }
        for log in &tx.logs {
            for filter in log.bitmap_filters()? {
                add(pages, Family::Log, filter, log_id);
            }
            log_id += 1;
        }
        for trace in &tx.traces {
            for filter in trace.bitmap_filters()? {
                add(pages, Family::Trace, filter, trace_id);
            }
            trace_id += 1;
        }
    }
    Ok(())
}

fn add(
    pages: &mut HashMap<(Family, BitmapFilterValue, u64), RoaringBitmap>,
    family: Family,
    filter: BitmapFilterValue,
    id: u64,
) {
    let page = id / BitmapIndexValue::PAGE_SPAN * BitmapIndexValue::PAGE_SPAN;
    pages
        .entry((family, filter, page))
        .or_default()
        .insert((id - page) as u32);
}
