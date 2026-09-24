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

use std::future::Future;

use self::{
    column::RowCells,
    table::{MetaTableDef, MetaTableId},
};
use crate::{QueryError, QueryResult};

pub mod column;
pub mod table;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MetaKey {
    pub partition: RowCells,
    pub clustering: RowCells,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaRow {
    pub key: MetaKey,
    pub columns: RowCells,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetaScanOrder {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaWrite {
    pub table: MetaTableId,
    pub row: MetaRow,
}

pub trait MetaStore: Send + Sync + 'static {
    fn init(
        &self,
        _table: MetaTableId,
        _definition: MetaTableDef,
    ) -> impl Future<Output = QueryResult<()>> + Send {
        async { Ok(()) }
    }

    fn get(
        &self,
        table: MetaTableId,
        key: MetaKey,
    ) -> impl Future<Output = QueryResult<Option<MetaRow>>> + Send;

    fn get_many(
        &self,
        table: MetaTableId,
        keys: Box<[MetaKey]>,
    ) -> impl Future<Output = QueryResult<Box<[MetaRow]>>> + Send {
        async move {
            let mut rows = Vec::with_capacity(keys.len());

            for key in keys {
                if let Some(row) = self.get(table, key).await? {
                    rows.push(row);
                }
            }

            Ok(rows.into_boxed_slice())
        }
    }

    fn get_range(
        &self,
        _table: MetaTableId,
        _partition: RowCells,
        _start: RowCells,
        _end: RowCells,
        _order: MetaScanOrder,
        _limit: Option<usize>,
    ) -> impl Future<Output = QueryResult<Vec<MetaRow>>> + Send {
        async {
            Err(QueryError::Backend(
                "metadata range query is not supported".into(),
            ))
        }
    }

    fn seek_le(
        &self,
        _table: MetaTableId,
        _partition: RowCells,
        _clustering: RowCells,
    ) -> impl Future<Output = QueryResult<Option<MetaRow>>> + Send {
        async { Err(QueryError::Backend("metadata seek is not supported".into())) }
    }

    fn put(&self, table: MetaTableId, row: MetaRow)
        -> impl Future<Output = QueryResult<()>> + Send;

    fn put_batch(&self, writes: Box<[MetaWrite]>) -> impl Future<Output = QueryResult<()>> + Send {
        async move {
            for write in writes {
                self.put(write.table, write.row).await?;
            }

            Ok(())
        }
    }
}
