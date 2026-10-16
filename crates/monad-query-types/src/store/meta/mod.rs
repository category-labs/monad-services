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

impl MetaKey {
    pub fn validate_batch_layout(keys: &[Self]) -> QueryResult<()> {
        if keys
            .windows(2)
            .any(|pair| !pair[0].has_same_columns(&pair[1]))
        {
            return Err(QueryError::InvalidRequest(
                "metadata batch keys must have identical ordered columns",
            ));
        }
        Ok(())
    }

    fn has_same_columns(&self, other: &Self) -> bool {
        same_columns(&self.partition, &other.partition)
            && same_columns(&self.clustering, &other.clustering)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaRow {
    pub key: MetaKey,
    pub columns: RowCells,
}

impl MetaRow {
    pub fn validate_batch_layout(rows: &[Self]) -> QueryResult<()> {
        if rows.windows(2).any(|pair| {
            !pair[0].key.has_same_columns(&pair[1].key)
                || !same_columns(&pair[0].columns, &pair[1].columns)
        }) {
            return Err(QueryError::InvalidRequest(
                "metadata batch rows must have identical ordered columns",
            ));
        }
        Ok(())
    }
}

fn same_columns(left: &RowCells, right: &RowCells) -> bool {
    left.iter()
        .map(|cell| &cell.column)
        .eq(right.iter().map(|cell| &cell.column))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetaScanOrder {
    Ascending,
    Descending,
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
            MetaKey::validate_batch_layout(&keys)?;
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

    fn put_batch(
        &self,
        table: MetaTableId,
        rows: Box<[MetaRow]>,
    ) -> impl Future<Output = QueryResult<()>> + Send {
        async move {
            MetaRow::validate_batch_layout(&rows)?;
            for row in rows {
                self.put(table, row).await?;
            }

            Ok(())
        }
    }
}
