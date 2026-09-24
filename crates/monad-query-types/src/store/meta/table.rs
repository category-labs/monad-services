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

use std::marker::PhantomData;

use tracing::debug;

use super::{
    column::{Cell, CellType, ColumnDef, RowCells},
    MetaKey, MetaRow, MetaScanOrder, MetaStore, MetaWrite,
};
use crate::{QueryError, QueryResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MetaTableId(&'static str);

impl MetaTableId {
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetaTableDef {
    pub partition_key: &'static [ColumnDef],
    pub clustering_key: &'static [ColumnDef],
    pub columns: &'static [ColumnDef],
}

impl MetaTableDef {
    fn validate_key(&self, key: &MetaKey) -> QueryResult<()> {
        Self::validate_columns(self.partition_key, &key.partition)?;
        Self::validate_columns(self.clustering_key, &key.clustering)
    }

    fn validate_row(&self, columns: &RowCells) -> QueryResult<()> {
        Self::validate_columns(self.columns, columns)
    }

    fn validate_columns(definitions: &[ColumnDef], columns: &[Cell]) -> QueryResult<()> {
        if definitions.len() != columns.len()
            || definitions.iter().zip(columns).any(|(definition, column)| {
                definition.name != column.column || definition.kind != CellType::from(&column.value)
            })
        {
            return Err(QueryError::InvalidRequest(
                "metadata row does not match table schema",
            ));
        }
        Ok(())
    }
}

pub trait MetaTableSchema: Send + Sync + 'static {
    const TABLE: MetaTableId;

    type Key: Send + Sync + 'static;
    type Value: Send + Sync + 'static;

    const DEFINITION: MetaTableDef;

    fn key(key: &Self::Key) -> MetaKey;
    fn decode_key(_key: MetaKey) -> QueryResult<Self::Key>;

    fn columns(value: &Self::Value) -> RowCells;
    fn decode(columns: RowCells) -> QueryResult<Self::Value>;
}

pub struct MetaTablePut<S>
where
    S: MetaTableSchema,
{
    pub key: S::Key,
    pub value: S::Value,
}

pub struct MetaTable<M, S>
where
    M: MetaStore,
    S: MetaTableSchema,
{
    store: M,
    _schema: PhantomData<S>,
}

impl<M, S> MetaTable<M, S>
where
    M: MetaStore,
    S: MetaTableSchema,
{
    pub fn new(store: M) -> Self {
        Self {
            store,
            _schema: PhantomData,
        }
    }

    pub async fn init(&self) -> QueryResult<()> {
        self.store.init(S::TABLE, S::DEFINITION).await
    }

    pub async fn get(&self, key: &S::Key) -> QueryResult<Option<S::Value>> {
        debug!(
            target = "query_store::meta",
            table = S::TABLE.as_str(),
            op = "get",
            "metadata read"
        );
        let key = S::key(key);
        S::DEFINITION.validate_key(&key)?;
        self.store
            .get(S::TABLE, key)
            .await?
            .map(|row| S::decode(row.columns))
            .transpose()
    }

    pub async fn get_many(&self, keys: &[S::Key]) -> QueryResult<Box<[Option<S::Value>]>> {
        debug!(
            target = "query_store::meta",
            table = S::TABLE.as_str(),
            op = "get_many",
            keys = keys.len(),
            "metadata read"
        );
        let physical_keys = keys
            .iter()
            .map(S::key)
            .map(|key| {
                S::DEFINITION.validate_key(&key)?;
                Ok(key)
            })
            .collect::<QueryResult<Box<[_]>>>()?;
        let rows = self.store.get_many(S::TABLE, physical_keys).await?;
        let mut values = std::collections::HashMap::with_capacity(rows.len());
        for row in rows {
            values.insert(row.key, S::decode(row.columns)?);
        }
        Ok(keys
            .iter()
            .map(|key| values.remove(&S::key(key)))
            .collect::<Box<[_]>>())
    }

    pub async fn scan(
        &self,
        start: &S::Key,
        end: &S::Key,
        order: MetaScanOrder,
        limit: Option<usize>,
    ) -> QueryResult<Box<[(S::Key, S::Value)]>> {
        debug!(
            target = "query_store::meta",
            table = S::TABLE.as_str(),
            op = "scan",
            "metadata read"
        );
        let start = S::key(start);
        let end = S::key(end);
        if start.partition != end.partition
            || start.clustering.len() != 1
            || end.clustering.len() != 1
            || start.clustering[0].column != end.clustering[0].column
            || start > end
        {
            return Err(QueryError::InvalidRequest(
                "metadata range requires ordered bounds in one partition",
            ));
        }
        self.store
            .get_range(
                S::TABLE,
                start.partition,
                start.clustering,
                end.clustering,
                order,
                limit,
            )
            .await?
            .into_iter()
            .map(|row| Ok((S::decode_key(row.key)?, S::decode(row.columns)?)))
            .collect()
    }

    pub async fn seek_le(&self, key: &S::Key) -> QueryResult<Option<(S::Key, S::Value)>> {
        debug!(
            target = "query_store::meta",
            table = S::TABLE.as_str(),
            op = "seek_le",
            "metadata read"
        );
        let key = S::key(key);
        S::DEFINITION.validate_key(&key)?;
        self.store
            .seek_le(S::TABLE, key.partition, key.clustering)
            .await?
            .map(|row| Ok((S::decode_key(row.key)?, S::decode(row.columns)?)))
            .transpose()
    }

    pub async fn put(&self, key: &S::Key, value: &S::Value) -> QueryResult<()> {
        let key = S::key(key);
        let columns = S::columns(value);
        S::DEFINITION.validate_key(&key)?;
        S::DEFINITION.validate_row(&columns)?;
        self.store.put(S::TABLE, MetaRow { key, columns }).await
    }

    pub async fn put_batch(&self, writes: Box<[MetaTablePut<S>]>) -> QueryResult<()> {
        let writes = writes
            .into_vec()
            .into_iter()
            .map(|write| {
                let key = S::key(&write.key);
                let columns = S::columns(&write.value);
                S::DEFINITION.validate_key(&key)?;
                S::DEFINITION.validate_row(&columns)?;
                Ok(MetaWrite {
                    table: S::TABLE,
                    row: MetaRow { key, columns },
                })
            })
            .collect::<QueryResult<Vec<_>>>()?
            .into_boxed_slice();
        self.store.put_batch(writes).await
    }
}
