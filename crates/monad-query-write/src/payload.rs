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
use itertools::Itertools;
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

use crate::QueryWriterFlushBatch;

pub(super) async fn flush<M, B>(
    payload: &PartitionSchemaBlobTable<B, PayloadKey, PayloadItem>,
    location: &MetaTable<M, PayloadLocationSchema>,
    committed_state: &StateIndex,
    batch: &QueryWriterFlushBatch,
) -> QueryResult<()>
where
    M: MetaStore,
    B: BlobTable,
{
    let mut family_locations = Vec::new();

    for family in Family::iter() {
        let items = batch
            .blocks
            .iter()
            .flat_map(|assigned| {
                let (first, rows) = match family {
                    Family::BlockHeader => (
                        assigned.block.block_number(),
                        vec![encode(&assigned.block.header)],
                    ),
                    Family::Transaction => (
                        assigned.txs.first_primary_id.as_u64(),
                        assigned
                            .block
                            .txs
                            .iter()
                            .map(|tx| encode(&tx.envelope))
                            .collect(),
                    ),
                    Family::Log => (
                        assigned.logs.first_primary_id.as_u64(),
                        assigned
                            .block
                            .txs
                            .iter()
                            .flat_map(|tx| tx.logs.iter())
                            .map(encode)
                            .collect(),
                    ),
                    Family::Trace => (
                        assigned.traces.first_primary_id.as_u64(),
                        assigned
                            .block
                            .txs
                            .iter()
                            .flat_map(|tx| tx.traces.iter())
                            .map(encode)
                            .collect(),
                    ),
                };

                rows.into_iter().enumerate().map(move |(index, bytes)| {
                    let id = PrimaryId::new(first + index as u64);
                    (
                        PayloadKey::from_id(family, id.as_u64()),
                        PayloadItem {
                            bytes: bytes.into(),
                        },
                        id,
                    )
                })
            })
            .collect::<Box<[_]>>();

        let item_count = items.len();

        let started = Instant::now();

        let groups = items
            .into_vec()
            .into_iter()
            .map(|(key, value, id)| (key, (id, value)))
            .into_group_map()
            .into_iter()
            .collect::<BTreeMap<_, _>>();

        let partition_count = groups.len();

        let mut grouped_items = BTreeMap::new();
        let mut grouped_ids = BTreeMap::new();

        for (key, items) in groups {
            let first_id = items.first().expect("payload group is non-empty").0;

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
                    location
                        .get(&PayloadLocationKey {
                            family,
                            id: PrimaryId::new(committed_id),
                        })
                        .await?
                        .ok_or(QueryError::MissingData(
                            "missing committed payload location",
                        ))?
                        .payload_end()
                }
                _ => 0,
            };

            let (ids, values): (Vec<_>, Vec<_>) = items.into_iter().unzip();
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

            family_locations.extend(ids.into_vec().into_iter().zip(end_values).map(|(id, end)| {
                MetaTablePut {
                    key: PayloadLocationKey { family, id },
                    value: PayloadLocation::new(end),
                }
            }));
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
}
