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

use futures::future::try_join_all;
use monad_query_schema::{
    bitmap::{BitmapFilterKey, BitmapFilterValue},
    family::{BitmapIndexable, Family, StoredTraceQueryItemWithAncestorStatus},
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

use crate::QueryWriterFlushBatch;

pub(super) async fn flush<B, M>(
    table: &SchemaBlobTable<B, BitmapIndexKey, BitmapIndexValue>,
    stats: &MetaTable<M, BitmapPageStatsSchema>,
    batch: &QueryWriterFlushBatch,
) -> QueryResult<()>
where
    B: BlobTable,
    M: MetaStore,
{
    let mut pages: HashMap<BitmapIndexKey, RoaringBitmap> = HashMap::new();
    {
        let mut add = |family: Family, filter: BitmapFilterValue, id: u64| {
            let page = id / BitmapIndexValue::PAGE_SPAN * BitmapIndexValue::PAGE_SPAN;
            pages
                .entry(BitmapIndexKey {
                    family,
                    filter: BitmapFilterKey { value: filter },
                    page,
                })
                .or_default()
                .insert((id - page) as u32);
        };

        for assigned in &batch.blocks {
            for filter in assigned.block.header.bitmap_filters()? {
                add(Family::BlockHeader, filter, assigned.block.block_number());
            }

            let mut log_id = assigned.logs.first_primary_id.as_u64();
            let mut trace_id = assigned.traces.first_primary_id.as_u64();

            for (tx_index, tx) in assigned.block.txs.iter().enumerate() {
                for filter in tx.envelope.bitmap_filters()? {
                    add(
                        Family::Transaction,
                        filter,
                        assigned.txs.first_primary_id.as_u64() + tx_index as u64,
                    );
                }

                for log in &tx.logs {
                    for filter in log.bitmap_filters()? {
                        add(Family::Log, filter, log_id);
                    }
                    log_id += 1;
                }

                for trace in StoredTraceQueryItemWithAncestorStatus::from_traces(&tx.traces) {
                    for filter in trace.bitmap_filters()? {
                        add(Family::Trace, filter, trace_id);
                    }
                    trace_id += 1;
                }
            }
        }
    }

    tracing::info!(pages = pages.len(), "bitmap updates accumulated");
    let first = batch.blocks.first();

    let updates = try_join_all(pages.into_iter().map(
        |(
            BitmapIndexKey {
                family,
                filter,
                page,
            },
            pending,
        )| {
            let committed = match family {
                Family::BlockHeader => first.map_or(0, |block| block.block.block_number()),
                Family::Transaction => first.map_or(batch.state.next_tx_id, |block| {
                    block.txs.first_primary_id.as_u64()
                }),
                Family::Log => first.map_or(batch.state.next_log_id, |block| {
                    block.logs.first_primary_id.as_u64()
                }),
                Family::Trace => first.map_or(batch.state.next_trace_id, |block| {
                    block.traces.first_primary_id.as_u64()
                }),
            };

            async move {
                let key = BitmapIndexKey {
                    family,
                    filter,
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
                        filter: key.filter.value.clone(),
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
        },
    ))
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
}
