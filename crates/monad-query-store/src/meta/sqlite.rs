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
    collections::HashMap,
    str::FromStr,
    sync::{Arc, RwLock},
    time::Duration,
};

use monad_query_types::{
    store::meta::{
        column::{Cell, CellType, CellValue, ColumnDef, RowCells},
        table::{MetaTableDef, MetaTableId},
        MetaKey, MetaRow, MetaScanOrder, MetaStore,
    },
    QueryError, QueryResult,
};
use sqlx::{
    sqlite::{SqliteArguments, SqliteConnectOptions, SqlitePoolOptions},
    Column as SqlxColumn, Row, Sqlite, SqlitePool, TypeInfo,
};
use tokio::sync::OnceCell;

#[derive(Debug, Clone)]
pub struct SqliteMetaStoreConfig {
    pub url: String,
    pub max_connections: u32,
}

impl SqliteMetaStoreConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            max_connections: 1,
        }
    }
}

#[derive(Clone)]
pub struct SqliteMetaStore {
    pool: SqlitePool,

    max_variable_number: Arc<OnceCell<usize>>,
    definitions: Arc<RwLock<HashMap<MetaTableId, MetaTableDef>>>,
}

impl SqliteMetaStore {
    pub async fn new_with_config(config: SqliteMetaStoreConfig) -> QueryResult<Self> {
        let options = SqliteConnectOptions::from_str(&config.url)
            .map_err(error("parse connect options"))?
            .busy_timeout(Duration::from_secs(30));

        let pool = SqlitePoolOptions::new()
            .max_connections(config.max_connections.max(1))
            .connect_with(options)
            .await
            .map_err(error("connect"))?;

        Ok(Self::new_with_pool(pool))
    }

    pub fn new_with_pool(pool: SqlitePool) -> Self {
        Self {
            pool,
            max_variable_number: Arc::new(OnceCell::new()),
            definitions: Arc::default(),
        }
    }

    async fn max_variable_number(&self) -> QueryResult<usize> {
        self.max_variable_number
            .get_or_try_init(|| async {
                let mut connection = self.pool.acquire().await.map_err(error("acquire"))?;

                let mut handle = connection
                    .lock_handle()
                    .await
                    .map_err(error("lock handle"))?;

                // SAFETY: SQLx guarantees the worker is stopped while the handle is locked.
                let limit = unsafe {
                    libsqlite3_sys::sqlite3_limit(
                        handle.as_raw_handle().as_ptr(),
                        libsqlite3_sys::SQLITE_LIMIT_VARIABLE_NUMBER,
                        -1,
                    )
                };

                usize::try_from(limit).map_err(|_| {
                    QueryError::Backend("sqlite reported an invalid variable limit".into())
                })
            })
            .await
            .copied()
    }

    fn decode_row(
        &self,
        table: MetaTableId,
        row: sqlx::sqlite::SqliteRow,
        key: MetaKey,
    ) -> QueryResult<MetaRow> {
        let definition = self
            .definitions
            .read()
            .expect("metadata definitions lock is not poisoned")
            .get(&table)
            .copied();
        let columns = row
            .columns()
            .iter()
            .filter(|column| {
                !Self::key_columns(&key).any(|key_column| key_column.column == column.name())
            })
            .map(|column| {
                let name = column.name();
                let is_bool = definition
                    .and_then(|definition| {
                        definition.columns.iter().find(|column| column.name == name)
                    })
                    .map_or_else(
                        || column.type_info().name() == "BOOLEAN",
                        |column| column.kind == CellType::Bool,
                    );
                let value = if is_bool {
                    row.try_get::<i64, _>(name)
                        .map(|value| CellValue::Bool(value != 0))
                } else {
                    row.try_get::<i64, _>(name)
                        .map(|value| CellValue::Number(value.to_string()))
                        .or_else(|_| {
                            row.try_get::<Vec<u8>, _>(name)
                                .map(|value| CellValue::Bytes(value.into_boxed_slice()))
                        })
                }
                .map_err(|_| Self::unsupported_column(name))?;

                Ok(Cell::new(name.to_owned(), value))
            })
            .collect::<QueryResult<Box<[_]>>>()?;

        Ok(MetaRow { key, columns })
    }

    fn definition_columns(definition: MetaTableDef) -> impl Iterator<Item = &'static ColumnDef> {
        definition
            .partition_key
            .iter()
            .chain(definition.clustering_key)
            .chain(definition.columns)
    }

    fn key_columns(key: &MetaKey) -> impl Iterator<Item = &Cell> + Clone {
        key.partition.iter().chain(key.clustering.iter())
    }

    fn row_columns(row: &MetaRow) -> impl Iterator<Item = &Cell> + Clone {
        Self::key_columns(&row.key).chain(row.columns.iter())
    }

    fn equality_predicate<'a>(columns: impl Iterator<Item = &'a Cell>) -> String {
        columns
            .map(|column| format!("{} = ?", quote(&column.column)))
            .collect::<Vec<_>>()
            .join(" AND ")
    }

    fn placeholders(count: usize) -> String {
        std::iter::repeat_n("?", count)
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn insert_sql<'a>(
        table: MetaTableId,
        columns: impl Iterator<Item = &'a Cell>,
        row_count: usize,
    ) -> String {
        let names = columns
            .map(|column| quote(&column.column))
            .collect::<Vec<_>>();
        let row_placeholders = format!("({})", Self::placeholders(names.len()));
        let names = names.join(", ");
        let values = std::iter::repeat_n(row_placeholders.as_str(), row_count)
            .collect::<Vec<_>>()
            .join(", ");

        format!(
            "INSERT OR REPLACE INTO {} ({names}) VALUES {values}",
            quote(table.as_str())
        )
    }

    fn bind_values<'a, 'q>(
        query: sqlx::query::Query<'q, Sqlite, SqliteArguments>,
        values: impl IntoIterator<Item = &'a CellValue>,
    ) -> QueryResult<sqlx::query::Query<'q, Sqlite, SqliteArguments>> {
        values.into_iter().try_fold(query, |query, value| {
            Ok(match value {
                CellValue::Bool(value) => query.bind(i64::from(*value)),
                CellValue::Number(value) => query.bind(value.parse::<i64>().map_err(|_| {
                    QueryError::InvalidRequest("SQLite numbers must be signed 64-bit integers")
                })?),
                CellValue::Bytes(value) => query.bind(value.to_vec()),
            })
        })
    }

    fn row_key(
        row: &sqlx::sqlite::SqliteRow,
        partition: RowCells,
        clustering: &Cell,
    ) -> QueryResult<MetaKey> {
        let name = clustering.column.as_ref();
        let value = match &clustering.value {
            CellValue::Bool(_) => row
                .try_get::<i64, _>(name)
                .map(|value| CellValue::Bool(value != 0)),
            CellValue::Number(_) => row
                .try_get::<i64, _>(name)
                .map(|value| CellValue::Number(value.to_string())),
            CellValue::Bytes(_) => row
                .try_get::<Vec<u8>, _>(name)
                .map(|value| CellValue::Bytes(value.into_boxed_slice())),
        }
        .map_err(|_| Self::unsupported_column(name))?;

        Ok(MetaKey {
            partition,
            clustering: Box::new([Cell {
                column: clustering.column.clone(),
                value,
            }]),
        })
    }

    fn sql_type(kind: CellType) -> &'static str {
        match kind {
            CellType::Bool => "BOOLEAN",
            CellType::Number => "INTEGER",
            CellType::Bytes => "BLOB",
        }
    }

    fn unsupported_column(name: &str) -> QueryError {
        QueryError::Backend(format!("unsupported sqlite column `{name}`"))
    }
}

impl MetaStore for SqliteMetaStore {
    async fn init(&self, table: MetaTableId, definition: MetaTableDef) -> QueryResult<()> {
        if definition.partition_key.is_empty() {
            return Err(QueryError::InvalidRequest(
                "metadata table needs a partition key",
            ));
        }

        let mut columns = Vec::new();

        for column in Self::definition_columns(definition) {
            if columns.contains(&column.name) {
                return Err(QueryError::InvalidRequest(
                    "metadata schema has duplicate columns",
                ));
            }

            columns.push(column.name);
        }

        let definitions = Self::definition_columns(definition)
            .map(|column| {
                format!(
                    "{} {} NOT NULL",
                    quote(column.name),
                    Self::sql_type(column.kind)
                )
            })
            .collect::<Vec<_>>();
        let key_count = definition.partition_key.len() + definition.clustering_key.len();

        let mut sql = format!("CREATE TABLE IF NOT EXISTS {} (", quote(table.as_str()));

        sql.push_str(&definitions.join(", "));
        sql.push_str(", PRIMARY KEY (");
        sql.push_str(
            &Self::definition_columns(definition)
                .take(key_count)
                .map(|column| quote(column.name))
                .collect::<Vec<_>>()
                .join(", "),
        );
        sql.push(')');
        sql.push(')');

        if key_count > 1 {
            sql.push_str(" WITHOUT ROWID");
        }

        sqlx::query(sqlx::AssertSqlSafe(sql))
            .execute(&self.pool)
            .await
            .map_err(error("create table"))?;

        let actual = sqlx::query(
            "SELECT name, type, \"notnull\" AS not_null, pk FROM pragma_table_info(?) ORDER BY cid",
        )
        .bind(table.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(error("read table schema"))?;

        if actual.len() != key_count + definition.columns.len()
            || actual
                .iter()
                .zip(Self::definition_columns(definition))
                .enumerate()
                .any(|(index, (row, column))| {
                    let name = row.try_get::<String, _>("name").ok();
                    let kind = row.try_get::<String, _>("type").ok();
                    let not_null = row.try_get::<i64, _>("not_null").ok();
                    let primary_key = row.try_get::<i64, _>("pk").ok();
                    let expected_primary_key = if index < key_count {
                        (index + 1) as i64
                    } else {
                        0
                    };

                    name.as_deref() != Some(column.name)
                        || !(kind.as_deref() == Some(Self::sql_type(column.kind))
                            || (column.kind == CellType::Bool
                                && kind.as_deref() == Some("INTEGER")))
                        || not_null != Some(1)
                        || primary_key != Some(expected_primary_key)
                })
        {
            return Err(QueryError::Backend(format!(
                "sqlite schema mismatch for table `{}`",
                table.as_str()
            )));
        }

        self.definitions
            .write()
            .expect("metadata definitions lock is not poisoned")
            .insert(table, definition);

        Ok(())
    }

    async fn get(&self, table: MetaTableId, key: MetaKey) -> QueryResult<Option<MetaRow>> {
        tracing::debug!(
            target = "query_store::meta",
            table = table.as_str(),
            "metadata get"
        );

        if key.partition.is_empty() {
            return Err(QueryError::InvalidRequest("missing partition key"));
        }

        let where_clause = Self::key_columns(&key);

        let sql = format!(
            "SELECT * FROM {} WHERE {} LIMIT 1",
            quote(table.as_str()),
            Self::equality_predicate(where_clause.clone()),
        );

        let query = Self::bind_values(
            sqlx::query(sqlx::AssertSqlSafe(sql)),
            where_clause.map(|column| &column.value),
        )?;

        let row = query
            .fetch_optional(&self.pool)
            .await
            .map_err(error("get"))?;

        row.map(|row| self.decode_row(table, row, key)).transpose()
    }

    async fn get_many(
        &self,
        table: MetaTableId,
        keys: Box<[MetaKey]>,
    ) -> QueryResult<Box<[MetaRow]>> {
        tracing::debug!(
            target = "query_store::meta",
            table = table.as_str(),
            keys = keys.len(),
            "metadata get_many"
        );

        MetaKey::validate_batch_layout(&keys)?;
        let Some(first) = keys.first() else {
            return Ok(Box::new([]));
        };

        let compatible = first.partition.len() == 1
            && first.clustering.len() == 1
            && keys.iter().all(|key| key.partition == first.partition);

        if !compatible || keys.len() >= self.max_variable_number().await? {
            let mut rows = Vec::with_capacity(keys.len());
            for key in keys {
                if let Some(row) = self.get(table, key).await? {
                    rows.push(row);
                }
            }
            return Ok(rows.into_boxed_slice());
        }

        let clustering = &first.clustering[0];

        let clustering_placeholders = Self::placeholders(keys.len());

        let sql = format!(
            "SELECT * FROM {} WHERE {} = ? AND {} IN ({})",
            quote(table.as_str()),
            quote(&first.partition[0].column),
            quote(&clustering.column),
            clustering_placeholders
        );

        let query = Self::bind_values(
            sqlx::query(sqlx::AssertSqlSafe(sql)),
            first
                .partition
                .iter()
                .map(|column| &column.value)
                .chain(keys.iter().map(|key| &key.clustering[0].value)),
        )?;

        query
            .fetch_all(&self.pool)
            .await
            .map_err(error("batch get"))?
            .into_iter()
            .map(|row| {
                let key = Self::row_key(&row, first.partition.clone(), clustering)?;
                self.decode_row(table, row, key)
            })
            .collect()
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
        if partition.is_empty() {
            return Err(QueryError::InvalidRequest("missing partition key"));
        }

        let clustering = start
            .first()
            .ok_or(QueryError::InvalidRequest("missing range start"))?;

        let end_clustering = end
            .first()
            .ok_or(QueryError::InvalidRequest("missing range end"))?;

        let predicates = format!(
            "{} AND {} BETWEEN ? AND ?",
            Self::equality_predicate(partition.iter()),
            quote(&clustering.column),
        );

        let order = match order {
            MetaScanOrder::Ascending => "ASC",
            MetaScanOrder::Descending => "DESC",
        };

        let mut sql = format!(
            "SELECT * FROM {} WHERE {} ORDER BY {} {order}",
            quote(table.as_str()),
            predicates,
            quote(&clustering.column)
        );

        if let Some(limit) = limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }

        let query = Self::bind_values(
            sqlx::query(sqlx::AssertSqlSafe(sql)),
            partition
                .iter()
                .map(|column| &column.value)
                .chain([&clustering.value, &end_clustering.value]),
        )?;

        query
            .fetch_all(&self.pool)
            .await
            .map_err(error("range query"))?
            .into_iter()
            .map(|row| {
                let key = Self::row_key(&row, partition.clone(), clustering)?;
                self.decode_row(table, row, key)
            })
            .collect()
    }

    async fn seek_le(
        &self,
        table: MetaTableId,
        partition: RowCells,
        clustering: RowCells,
    ) -> QueryResult<Option<MetaRow>> {
        if partition.is_empty() {
            return Err(QueryError::InvalidRequest("missing partition key"));
        }

        let [clustering] = clustering.as_ref() else {
            return Err(QueryError::InvalidRequest(
                "metadata seek requires exactly one clustering column",
            ));
        };

        let predicates = format!(
            "{} AND {} <= ?",
            Self::equality_predicate(partition.iter()),
            quote(&clustering.column),
        );

        let sql = format!(
            "SELECT * FROM {} WHERE {} ORDER BY {} DESC LIMIT 1",
            quote(table.as_str()),
            predicates,
            quote(&clustering.column),
        );

        let query = Self::bind_values(
            sqlx::query(sqlx::AssertSqlSafe(sql)),
            partition
                .iter()
                .map(|column| &column.value)
                .chain(std::iter::once(&clustering.value)),
        )?;

        query
            .fetch_optional(&self.pool)
            .await
            .map_err(error("seek"))?
            .map(|row| {
                let key = Self::row_key(&row, partition, clustering)?;
                self.decode_row(table, row, key)
            })
            .transpose()
    }

    async fn put(&self, table: MetaTableId, row: MetaRow) -> QueryResult<()> {
        let columns = Self::row_columns(&row);
        let sql = Self::insert_sql(table, columns.clone(), 1);
        let query = Self::bind_values(
            sqlx::query(sqlx::AssertSqlSafe(sql)),
            columns.map(|column| &column.value),
        )?;

        query.execute(&self.pool).await.map_err(error("put"))?;

        Ok(())
    }

    async fn put_batch(&self, table: MetaTableId, rows: Box<[MetaRow]>) -> QueryResult<()> {
        MetaRow::validate_batch_layout(&rows)?;
        let Some(first) = rows.first() else {
            return Ok(());
        };

        let columns = Self::row_columns(first);
        let column_count = columns.clone().count();
        let max_variable_number = self.max_variable_number().await?;
        if column_count == 0 || column_count > max_variable_number {
            return Err(QueryError::InvalidRequest(
                "metadata batch row must fit within SQLite's variable limit",
            ));
        }

        let mut transaction = self.pool.begin().await.map_err(error("begin"))?;
        for chunk in rows.chunks(max_variable_number / column_count) {
            let sql = Self::insert_sql(table, columns.clone(), chunk.len());
            let query = Self::bind_values(
                sqlx::query(sqlx::AssertSqlSafe(sql)),
                chunk
                    .iter()
                    .flat_map(|row| Self::row_columns(row).map(|column| &column.value)),
            )?;

            query
                .execute(&mut *transaction)
                .await
                .map_err(error("batch put"))?;
        }

        transaction.commit().await.map_err(error("commit"))?;

        Ok(())
    }
}

fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn error(operation: &'static str) -> impl Fn(sqlx::Error) -> QueryError {
    move |error| QueryError::Backend(format!("sqlite {operation}: {error}"))
}
