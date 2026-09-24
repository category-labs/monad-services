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

use std::{collections::HashMap, marker::PhantomData};

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
        Self::validate_row(self.partition_key, &key.partition)?;
        Self::validate_row(self.clustering_key, &key.clustering)
    }

    fn validate_row_cells(&self, row: &RowCells) -> QueryResult<()> {
        Self::validate_row(self.columns, row)
    }

    fn validate_row(row_def: &[ColumnDef], row: &[Cell]) -> QueryResult<()> {
        if row_def.len() != row.len()
            || row_def.iter().zip(row).any(|(definition, column)| {
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

pub struct MetaTablePut<TS>
where
    TS: MetaTableSchema,
{
    pub key: TS::Key,
    pub value: TS::Value,
}

pub struct MetaTable<S, TS>
where
    S: MetaStore,
    TS: MetaTableSchema,
{
    store: S,
    _schema: PhantomData<TS>,
}

impl<S, TS> MetaTable<S, TS>
where
    S: MetaStore,
    TS: MetaTableSchema,
{
    pub fn new(store: S) -> Self {
        Self {
            store,
            _schema: PhantomData,
        }
    }

    pub async fn init(&self) -> QueryResult<()> {
        self.store.init(TS::TABLE, TS::DEFINITION).await
    }

    pub async fn get(&self, key: &TS::Key) -> QueryResult<Option<TS::Value>> {
        debug!(
            target = "query_store::meta",
            table = TS::TABLE.as_str(),
            op = "get",
            "metadata read"
        );

        let key = TS::key(key);

        TS::DEFINITION.validate_key(&key)?;

        self.store
            .get(TS::TABLE, key)
            .await?
            .map(|row| TS::decode(row.columns))
            .transpose()
    }

    pub async fn get_many(&self, keys: &[TS::Key]) -> QueryResult<Box<[Option<TS::Value>]>> {
        debug!(
            target = "query_store::meta",
            table = TS::TABLE.as_str(),
            op = "get_many",
            keys = keys.len(),
            "metadata read"
        );

        let physical_keys = keys
            .iter()
            .map(TS::key)
            .map(|key| {
                TS::DEFINITION.validate_key(&key)?;
                Ok(key)
            })
            .collect::<QueryResult<Box<[_]>>>()?;

        let rows = self.store.get_many(TS::TABLE, physical_keys).await?;

        let values = rows
            .into_vec()
            .into_iter()
            .map(|row| (row.key, row.columns))
            .collect::<HashMap<_, _>>();

        keys.iter()
            .map(|key| {
                values
                    .get(&TS::key(key))
                    .cloned()
                    .map(TS::decode)
                    .transpose()
            })
            .collect()
    }

    pub async fn scan(
        &self,
        start: &TS::Key,
        end: &TS::Key,
        order: MetaScanOrder,
        limit: Option<usize>,
    ) -> QueryResult<Box<[(TS::Key, TS::Value)]>> {
        debug!(
            target = "query_store::meta",
            table = TS::TABLE.as_str(),
            op = "scan",
            "metadata read"
        );

        let start = TS::key(start);
        let end = TS::key(end);

        TS::DEFINITION.validate_key(&start)?;
        TS::DEFINITION.validate_key(&end)?;

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
                TS::TABLE,
                start.partition,
                start.clustering,
                end.clustering,
                order,
                limit,
            )
            .await?
            .into_iter()
            .map(|row| Ok((TS::decode_key(row.key)?, TS::decode(row.columns)?)))
            .collect()
    }

    pub async fn seek_le(&self, key: &TS::Key) -> QueryResult<Option<(TS::Key, TS::Value)>> {
        debug!(
            target = "query_store::meta",
            table = TS::TABLE.as_str(),
            op = "seek_le",
            "metadata read"
        );

        let key = TS::key(key);

        TS::DEFINITION.validate_key(&key)?;

        self.store
            .seek_le(TS::TABLE, key.partition, key.clustering)
            .await?
            .map(|row| Ok((TS::decode_key(row.key)?, TS::decode(row.columns)?)))
            .transpose()
    }

    pub async fn put(&self, key: &TS::Key, value: &TS::Value) -> QueryResult<()> {
        let key = TS::key(key);
        let columns = TS::columns(value);

        TS::DEFINITION.validate_key(&key)?;
        TS::DEFINITION.validate_row_cells(&columns)?;

        self.store.put(TS::TABLE, MetaRow { key, columns }).await
    }

    pub async fn put_batch(&self, writes: Box<[MetaTablePut<TS>]>) -> QueryResult<()> {
        let writes = writes
            .into_vec()
            .into_iter()
            .map(|write| {
                let key = TS::key(&write.key);
                let columns = TS::columns(&write.value);

                TS::DEFINITION.validate_key(&key)?;
                TS::DEFINITION.validate_row_cells(&columns)?;

                Ok(MetaWrite {
                    table: TS::TABLE,
                    row: MetaRow { key, columns },
                })
            })
            .collect::<QueryResult<Box<[_]>>>()?;

        self.store.put_batch(writes).await
    }
}
