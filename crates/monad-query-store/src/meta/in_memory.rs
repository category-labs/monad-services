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
        column::RowCells, table::MetaTableId, MetaKey, MetaRow, MetaScanOrder, MetaStore, MetaWrite,
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
        let mut rows = self
            .rows
            .read()
            .expect("metadata lock is not poisoned")
            .range((table, start)..=(table, end))
            .map(|(_, row)| row.clone())
            .collect::<Vec<_>>();
        match order {
            MetaScanOrder::Ascending => {}
            MetaScanOrder::Descending => rows.reverse(),
        }
        if let Some(limit) = limit {
            rows.truncate(limit);
        }
        Ok(rows)
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

    async fn put_batch(&self, writes: Box<[MetaWrite]>) -> QueryResult<()> {
        let mut rows = self.rows.write().expect("metadata lock is not poisoned");
        for write in writes {
            rows.insert((write.table, write.row.key.clone()), write.row);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use monad_query_types::store::meta::{
        column::{Cell, CellValue},
        table::MetaTableId,
    };

    use super::{InMemoryMetaStore, MetaKey, MetaRow, MetaStore};

    fn key(id: u64) -> MetaKey {
        MetaKey {
            partition: Box::new([Cell::new_number("id", id)]),
            clustering: Box::new([]),
        }
    }

    #[tokio::test]
    async fn stores_typed_rows() {
        let store = InMemoryMetaStore::default();
        let table = MetaTableId::new("test");
        let row = MetaRow {
            key: key(1),
            columns: Box::new([Cell::new_number("value", 42)]),
        };

        store.put(table, row.clone()).await.unwrap();
        assert_eq!(store.get(table, key(1)).await.unwrap(), Some(row));
    }

    #[tokio::test]
    async fn seeks_to_predecessor() {
        let store = InMemoryMetaStore::default();
        let table = MetaTableId::new("test");
        let partition = Box::new([Cell::new_number("family", 1)]);
        let row = |id: u64| MetaRow {
            key: MetaKey {
                partition: partition.clone(),
                clustering: Box::new([Cell::new_number("first_primary_id", id)]),
            },
            columns: Box::new([]),
        };

        store.put(table, row(10)).await.unwrap();
        store.put(table, row(20)).await.unwrap();
        let predecessor = store
            .seek_le(
                table,
                partition.clone(),
                Box::new([Cell::new_number("first_primary_id", 19)]),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            predecessor.key.clustering[0].value,
            CellValue::Number("10".to_owned())
        );
        assert!(store
            .seek_le(
                table,
                partition,
                Box::new([Cell::new_number("first_primary_id", 9)]),
            )
            .await
            .unwrap()
            .is_none());
    }
}
