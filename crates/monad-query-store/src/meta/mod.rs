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

use monad_query_types::{
    store::meta::{
        column::RowCells,
        table::{MetaTableDef, MetaTableId},
        MetaKey, MetaRow, MetaScanOrder, MetaStore,
    },
    QueryResult,
};

use self::in_memory::InMemoryMetaStore;
#[cfg(feature = "sqlite")]
use self::sqlite::SqliteMetaStore;

pub mod in_memory;
#[cfg(feature = "sqlite")]
pub mod sqlite;

#[derive(Clone)]
pub enum MetaStoreBackend {
    InMemory(InMemoryMetaStore),
    #[cfg(feature = "sqlite")]
    Sqlite(SqliteMetaStore),
}

macro_rules! with_meta_backend {
    ($backend:expr, $store:ident => $operation:expr) => {
        match $backend {
            MetaStoreBackend::InMemory($store) => $operation,
            #[cfg(feature = "sqlite")]
            MetaStoreBackend::Sqlite($store) => $operation,
        }
    };
}

impl MetaStore for MetaStoreBackend {
    async fn init(&self, table: MetaTableId, definition: MetaTableDef) -> QueryResult<()> {
        with_meta_backend!(self, backend => backend.init(table, definition).await)
    }

    async fn get(&self, table: MetaTableId, key: MetaKey) -> QueryResult<Option<MetaRow>> {
        with_meta_backend!(self, backend => backend.get(table, key).await)
    }

    async fn get_many(
        &self,
        table: MetaTableId,
        keys: Box<[MetaKey]>,
    ) -> QueryResult<Box<[MetaRow]>> {
        with_meta_backend!(self, backend => backend.get_many(table, keys).await)
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
        with_meta_backend!(self, backend => {
            backend
                .get_range(table, partition, start, end, order, limit)
                .await
        })
    }

    async fn seek_le(
        &self,
        table: MetaTableId,
        partition: RowCells,
        clustering: RowCells,
    ) -> QueryResult<Option<MetaRow>> {
        with_meta_backend!(self, backend => {
            backend.seek_le(table, partition, clustering).await
        })
    }

    async fn put(&self, table: MetaTableId, row: MetaRow) -> QueryResult<()> {
        with_meta_backend!(self, backend => backend.put(table, row).await)
    }

    async fn put_batch(&self, table: MetaTableId, rows: Box<[MetaRow]>) -> QueryResult<()> {
        with_meta_backend!(self, backend => backend.put_batch(table, rows).await)
    }
}
