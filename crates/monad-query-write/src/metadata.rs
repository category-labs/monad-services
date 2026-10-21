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

use futures::future::LocalBoxFuture;
use monad_query_schema::{
    family::{Family, FamilyMetadata},
    tables::metadata::{
        block_hash::{MetadataBlockHashKey, MetadataBlockHashSchema, MetadataBlockHashValue},
        family::{MetadataFamilyKey, MetadataFamilySchema},
        family_block_index::{
            MetadataFamilyBlockIndexKey, MetadataFamilyBlockIndexSchema,
            MetadataFamilyBlockIndexValue,
        },
    },
};
use monad_query_types::{
    store::meta::{
        table::{MetaTable, MetaTablePut},
        MetaStore,
    },
    PrimaryId, QueryResult,
};
use strum::IntoEnumIterator;

use crate::FlushBatch;

pub(crate) fn flush<'a, M>(
    block_table: &'a MetaTable<M, MetadataBlockHashSchema>,
    family_table: &'a MetaTable<M, MetadataFamilySchema>,
    family_block_index: &'a MetaTable<M, MetadataFamilyBlockIndexSchema>,
    batch: &'a FlushBatch,
) -> LocalBoxFuture<'a, QueryResult<()>>
where
    M: MetaStore,
{
    Box::pin(async move {
        let mut blocks = Vec::with_capacity(batch.blocks.len());
        let mut families = Vec::with_capacity(batch.blocks.len() * 3);
        let mut family_block_index_entries = Vec::with_capacity(batch.blocks.len() * 4);

        for assigned in &batch.blocks {
            let block = &assigned.block;
            blocks.push(MetaTablePut {
                key: MetadataBlockHashKey {
                    block_height: block.block_number(),
                },
                value: MetadataBlockHashValue(block.block_hash()),
            });

            for family in Family::iter() {
                let range = match family {
                    Family::BlockHeader => FamilyMetadata {
                        first_primary_id: PrimaryId::new(block.block_number()),
                        count: 1,
                    },
                    Family::Transaction => assigned.txs,
                    Family::Log => assigned.logs,
                    Family::Trace => assigned.traces,
                };
                families.push(MetaTablePut {
                    key: MetadataFamilyKey {
                        family,
                        block_height: block.block_number(),
                    },
                    value: range,
                });
                if range.count > 0 {
                    let first_primary_id = range.first_primary_id.as_u64();
                    family_block_index_entries.push(MetaTablePut {
                        key: MetadataFamilyBlockIndexKey {
                            family,
                            first_primary_id,
                        },
                        value: MetadataFamilyBlockIndexValue {
                            block_height: block.block_number(),
                        },
                    });
                }
            }
        }

        let block_count = blocks.len();
        let family_count = families.len();
        let index_count = family_block_index_entries.len();
        let started = Instant::now();
        block_table.put_batch(blocks.into_boxed_slice()).await?;
        tracing::info!(
            table = "block_hash",
            rows = block_count,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "metadata table flush complete"
        );
        let started = Instant::now();
        family_table.put_batch(families.into_boxed_slice()).await?;
        tracing::info!(
            table = "family",
            rows = family_count,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "metadata table flush complete"
        );
        let started = Instant::now();
        family_block_index
            .put_batch(family_block_index_entries.into_boxed_slice())
            .await?;
        tracing::info!(
            table = "family_block_index",
            rows = index_count,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "metadata table flush complete"
        );
        Ok(())
    })
}
