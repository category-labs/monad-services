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

use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use monad_query_types::{
    store::meta::{
        column::RowCells, table::MetaTableId, MetaKey, MetaRow, MetaScanOrder, MetaStore,
    },
    QueryResult,
};

type RowKey = (MetaTableId, MetaKey);

#[derive(Debug, Clone, Default)]
pub struct InMemoryMetaStore {
    rows: Arc<RwLock<BTreeMap<RowKey, MetaRow>>>,
}

impl MetaStore for InMemoryMetaStore {
    async fn get(&self, table: MetaTableId, key: MetaKey) -> QueryResult<Option<MetaRow>> {
        Ok(self
            .rows
            .read()
            .expect("metadata lock is not poisoned")
            .get(&(table, key))
            .cloned())
    }

    async fn get_range(
        &self,
        table: MetaTableId,
        partition: RowCells,
        start: RowCells,
        end: RowCells,
        order: MetaScanOrder,
        limit: Option<usize>,
    ) -> QueryResult<Vec<MetaRow>> {
        let start = MetaKey {
            partition: partition.clone(),
            clustering: start,
        };

        let end = MetaKey {
            partition,
            clustering: end,
        };

        let rows = self.rows.read().expect("metadata lock is not poisoned");
        let range = rows.range((table, start)..=(table, end));
        let limit = limit.unwrap_or(usize::MAX);

        Ok(match order {
            MetaScanOrder::Ascending => range.take(limit).map(|(_, row)| row.clone()).collect(),
            MetaScanOrder::Descending => range
                .rev()
                .take(limit)
                .map(|(_, row)| row.clone())
                .collect(),
        })
    }

    async fn seek_le(
        &self,
        table: MetaTableId,
        partition: RowCells,
        clustering: RowCells,
    ) -> QueryResult<Option<MetaRow>> {
        let start = MetaKey {
            partition: partition.clone(),
            clustering: Box::new([]),
        };

        let end = MetaKey {
            partition,
            clustering,
        };

        Ok(self
            .rows
            .read()
            .expect("metadata lock is not poisoned")
            .range((table, start)..=(table, end))
            .next_back()
            .map(|(_, row)| row.clone()))
    }

    async fn put(&self, table: MetaTableId, row: MetaRow) -> QueryResult<()> {
        self.rows
            .write()
            .expect("metadata lock is not poisoned")
            .insert((table, row.key.clone()), row);

        Ok(())
    }

    async fn put_batch(&self, table: MetaTableId, rows: Box<[MetaRow]>) -> QueryResult<()> {
        MetaRow::validate_batch_layout(&rows)?;
        let mut stored_rows = self.rows.write().expect("metadata lock is not poisoned");

        for row in rows {
            stored_rows.insert((table, row.key.clone()), row);
        }

        Ok(())
    }
}
