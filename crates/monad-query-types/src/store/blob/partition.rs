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

use std::{future::Future, hash::Hash, marker::PhantomData};

use bytes::Bytes;
use itertools::Itertools;

use super::table::{BlobTable, BlobTablePutBatch, TableKey, TableValue};
use crate::{QueryError, QueryResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionItemBoundaries(Box<[(usize, usize)]>);

impl PartitionItemBoundaries {
    pub fn new(boundaries: Box<[(usize, usize)]>) -> QueryResult<Self> {
        if boundaries.is_empty()
            || boundaries.iter().any(|(start, end)| start > end)
            || boundaries
                .iter()
                .tuple_windows()
                .any(|((_, previous_end), (next_start, _))| previous_end > next_start)
        {
            return Err(QueryError::Decode("invalid partition item boundaries"));
        }

        Ok(Self(boundaries))
    }

    pub fn range(&self) -> (usize, usize) {
        (
            self.0.first().expect("boundaries is non-empty").0,
            self.0.last().expect("boundaries is non-empty").1,
        )
    }

    pub fn iter(&self) -> impl Iterator<Item = (usize, usize)> + '_ {
        self.0.iter().copied()
    }
}

pub trait PartitionedBlobTable: Send + Sync {
    type Key: TableKey + Clone + Eq + Hash;
    type Item: TableValue;

    fn get_many(
        &self,
        key: &Self::Key,
        boundaries: &PartitionItemBoundaries,
    ) -> impl Future<Output = QueryResult<Option<Box<[Self::Item]>>>> + Send;

    fn append(
        &self,
        items: Box<[(Self::Key, Self::Item)]>,
    ) -> impl Future<Output = QueryResult<Box<[u64]>>> + Send;
}

pub struct PartitionSchemaBlobTable<T, K, V>
where
    T: BlobTable,
    K: TableKey,
    V: TableValue,
{
    table: T,
    _phantom: PhantomData<(K, V)>,
}

impl<T, K, V> PartitionSchemaBlobTable<T, K, V>
where
    T: BlobTable,
    K: TableKey,
    V: TableValue,
{
    pub fn new(table: T) -> Self {
        Self {
            table,
            _phantom: PhantomData,
        }
    }
}

impl<T, K, V> PartitionedBlobTable for PartitionSchemaBlobTable<T, K, V>
where
    T: BlobTable,
    K: TableKey + Clone + Eq + Hash + Send + Sync,
    V: TableValue,
{
    type Key = K;
    type Item = V;

    async fn get_many(
        &self,
        key: &K,
        boundaries: &PartitionItemBoundaries,
    ) -> QueryResult<Option<Box<[V]>>> {
        let (start, end) = boundaries.range();

        let Some(bytes) = self.table.get_range::<_, Bytes>(key, start, end).await? else {
            return Ok(None);
        };

        boundaries
            .iter()
            .map(|(item_start, item_end)| {
                let relative_start = item_start - start;
                let relative_end = item_end - start;
                V::decode(bytes.slice(relative_start..relative_end))
            })
            .collect::<QueryResult<Box<[V]>>>()
            .map(Some)
    }

    async fn append(&self, items: Box<[(K, V)]>) -> QueryResult<Box<[u64]>> {
        let mut ends = vec![0; items.len()].into_boxed_slice();

        let write_groups = items
            .into_vec()
            .into_iter()
            .enumerate()
            .map(|(idx, (key, value))| (key, (idx, value)))
            .into_group_map();

        let mut writes = Vec::with_capacity(write_groups.len());

        for (key, values) in write_groups {
            let mut bytes = self
                .table
                .get::<_, Bytes>(&key)
                .await?
                .map(|value| value.to_vec())
                .unwrap_or_default();

            for (index, value) in values {
                bytes.extend_from_slice(&value.encode());
                ends[index] = bytes.len() as u64;
            }

            writes.push(BlobTablePutBatch {
                key,
                value: Bytes::from(bytes),
            });
        }

        self.table.put_batch(writes.into_boxed_slice()).await?;

        Ok(ends)
    }
}
