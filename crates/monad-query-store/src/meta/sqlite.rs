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

use std::{str::FromStr, sync::Arc, time::Duration};

use monad_query_types::{
    store::meta::{
        column::{Cell, CellType, CellValue, ColumnDef, RowCells},
        table::{MetaTableDef, MetaTableId},
        MetaKey, MetaRow, MetaScanOrder, MetaStore, MetaWrite,
    },
    QueryError, QueryResult,
};
use sqlx::{
    sqlite::{SqliteArguments, SqliteConnectOptions, SqlitePoolOptions},
    Column as SqlxColumn, Row, Sqlite, SqlitePool,
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

    fn table_name(table: MetaTableId) -> String {
        table.as_str().replace('"', "\"\"")
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
        for column in definition
            .partition_key
            .iter()
            .chain(definition.clustering_key)
            .chain(definition.columns)
        {
            if columns.contains(&column.name) {
                return Err(QueryError::InvalidRequest(
                    "metadata schema has duplicate columns",
                ));
            }
            columns.push(column.name);
        }

        let mut sql = format!("CREATE TABLE IF NOT EXISTS {} (", Self::table_name(table));
        let definitions = definition
            .partition_key
            .iter()
            .chain(definition.clustering_key)
            .chain(definition.columns)
            .map(|column| format!("{} {} NOT NULL", quote(column.name), sql_type(column.kind)))
            .collect::<Vec<_>>();
        sql.push_str(&definitions.join(", "));
        sql.push_str(", PRIMARY KEY (");
        sql.push_str(
            &definition
                .partition_key
                .iter()
                .chain(definition.clustering_key)
                .map(|column| quote(column.name))
                .collect::<Vec<_>>()
                .join(", "),
        );
        sql.push(')');
        sql.push(')');
        if definition.partition_key.len() + definition.clustering_key.len() > 1 {
            sql.push_str(" WITHOUT ROWID");
        }

        sqlx::query(sqlx::AssertSqlSafe(sql))
            .execute(&self.pool)
            .await
            .map_err(error("create table"))?;

        let actual = sqlx::query(
            "SELECT name, type, \"notnull\" AS not_null, pk FROM pragma_table_info(?) ORDER BY cid",
        )
        .bind(Self::table_name(table))
        .fetch_all(&self.pool)
        .await
        .map_err(error("read table schema"))?;
        let expected = definition
            .partition_key
            .iter()
            .chain(definition.clustering_key)
            .chain(definition.columns)
            .enumerate()
            .map(|(index, column)| {
                (
                    column.name,
                    sql_type(column.kind),
                    1_i64,
                    if index < definition.partition_key.len() + definition.clustering_key.len() {
                        (index + 1) as i64
                    } else {
                        0
                    },
                )
            })
            .collect::<Vec<_>>();

        if actual.len() != expected.len()
            || actual.iter().zip(expected).any(|(row, expected)| {
                let name = row.try_get::<String, _>("name").ok();
                let kind = row.try_get::<String, _>("type").ok();
                let not_null = row.try_get::<i64, _>("not_null").ok();
                let primary_key = row.try_get::<i64, _>("pk").ok();
                name.as_deref() != Some(expected.0)
                    || kind.as_deref() != Some(expected.1)
                    || not_null != Some(expected.2)
                    || primary_key != Some(expected.3)
            })
        {
            return Err(QueryError::Backend(format!(
                "sqlite schema mismatch for table `{}`",
                table.as_str()
            )));
        }

        Ok(())
    }

    async fn get(&self, table: MetaTableId, key: MetaKey) -> QueryResult<Option<MetaRow>> {
        tracing::debug!(
            target = "query_store::meta",
            table = table.as_str(),
            "metadata get"
        );
        let where_clause = key_columns(&key);
        let sql = format!(
            "SELECT * FROM {} WHERE {} LIMIT 1",
            Self::table_name(table),
            where_clause
                .iter()
                .map(|column| format!("{} = ?", quote(&column.column)))
                .collect::<Vec<_>>()
                .join(" AND "),
        );
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql));
        for column in where_clause {
            query = bind(query, column.value)?;
        }
        let row = query
            .fetch_optional(&self.pool)
            .await
            .map_err(error("get"))?;
        row.map(|row| decode_row(row, &key)).transpose()
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
        let Some(first) = keys.first() else {
            return Ok(Box::new([]));
        };
        let clustering = first
            .clustering
            .first()
            .ok_or(QueryError::InvalidRequest("missing clustering key"))?;
        if keys.iter().any(|key| {
            key.partition != first.partition
                || key.clustering.len() != 1
                || key.clustering[0].column != clustering.column
        }) {
            return Err(QueryError::InvalidRequest(
                "metadata batch requires one partition and clustering key",
            ));
        }
        let max_variable_number = self.max_variable_number().await?;
        let keys_per_query = max_variable_number
            .saturating_sub(first.partition.len())
            .max(1);
        let mut result = Vec::with_capacity(keys.len());
        for keys in keys.chunks(keys_per_query) {
            let placeholders = std::iter::repeat_n("?", keys.len())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT * FROM {} WHERE {} = ? AND {} IN ({})",
                Self::table_name(table),
                first.partition[0].column,
                clustering.column,
                placeholders
            );
            let mut query = sqlx::query(sqlx::AssertSqlSafe(sql));
            for column in &first.partition {
                query = bind(query, column.value.clone())?;
            }
            for key in keys {
                query = bind(query, key.clustering[0].value.clone())?;
            }
            for row in query
                .fetch_all(&self.pool)
                .await
                .map_err(error("batch get"))?
            {
                let value = column_value(&row, clustering)?;
                let key = MetaKey {
                    partition: first.partition.clone(),
                    clustering: Box::new([Cell {
                        column: clustering.column.clone(),
                        value,
                    }]),
                };
                result.push(decode_row(row, &key)?);
            }
        }
        Ok(result.into_boxed_slice())
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
        let clustering = start
            .first()
            .ok_or(QueryError::InvalidRequest("missing range start"))?;
        let end_clustering = end
            .first()
            .ok_or(QueryError::InvalidRequest("missing range end"))?;
        let mut predicates = partition
            .iter()
            .map(|column| format!("{} = ?", quote(&column.column)))
            .collect::<Vec<_>>();
        predicates.push(format!("{} BETWEEN ? AND ?", quote(&clustering.column)));
        let order = match order {
            MetaScanOrder::Ascending => "ASC",
            MetaScanOrder::Descending => "DESC",
        };
        let sql = format!(
            "SELECT * FROM {} WHERE {} ORDER BY {} {order}",
            Self::table_name(table),
            predicates.join(" AND "),
            quote(&clustering.column)
        );
        let sql = limit.map_or(sql.clone(), |limit| format!("{sql} LIMIT {limit}"));
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql));
        for column in &partition {
            query = bind(query, column.value.clone())?;
        }
        query = bind(query, clustering.value.clone())?;
        query = bind(query, end_clustering.value.clone())?;
        query
            .fetch_all(&self.pool)
            .await
            .map_err(error("range query"))?
            .into_iter()
            .map(|row| {
                let clustering_value = column_value(&row, clustering)?;
                let key = MetaKey {
                    partition: partition.clone(),
                    clustering: Box::new([Cell {
                        column: clustering.column.clone(),
                        value: clustering_value,
                    }]),
                };
                decode_row(row, &key)
            })
            .collect()
    }

    async fn seek_le(
        &self,
        table: MetaTableId,
        partition: RowCells,
        clustering: RowCells,
    ) -> QueryResult<Option<MetaRow>> {
        let clustering = clustering
            .first()
            .ok_or(QueryError::InvalidRequest("missing seek clustering"))?;
        let predicates = partition
            .iter()
            .map(|column| format!("{} = ?", quote(&column.column)))
            .chain(std::iter::once(format!(
                "{} <= ?",
                quote(&clustering.column)
            )))
            .collect::<Vec<_>>();
        let sql = format!(
            "SELECT * FROM {} WHERE {} ORDER BY {} DESC LIMIT 1",
            Self::table_name(table),
            predicates.join(" AND "),
            quote(&clustering.column),
        );
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql));
        for column in &partition {
            query = bind(query, column.value.clone())?;
        }
        query = bind(query, clustering.value.clone())?;
        query
            .fetch_optional(&self.pool)
            .await
            .map_err(error("seek"))?
            .map(|row| {
                let clustering_value = column_value(&row, clustering)?;
                let key = MetaKey {
                    partition,
                    clustering: Box::new([Cell {
                        column: clustering.column.clone(),
                        value: clustering_value,
                    }]),
                };
                decode_row(row, &key)
            })
            .transpose()
    }

    async fn put(&self, table: MetaTableId, row: MetaRow) -> QueryResult<()> {
        let columns = key_columns(&row.key)
            .into_iter()
            .chain(row.columns)
            .collect::<Vec<_>>();
        let names = columns
            .iter()
            .map(|column| quote(&column.column))
            .collect::<Vec<_>>()
            .join(", ");
        let placeholders = std::iter::repeat_n("?", columns.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "INSERT OR REPLACE INTO {} ({names}) VALUES ({placeholders})",
            Self::table_name(table),
        );
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql));
        for column in columns {
            query = bind(query, column.value)?;
        }
        query.execute(&self.pool).await.map_err(error("put"))?;
        Ok(())
    }

    async fn put_batch(&self, writes: Box<[MetaWrite]>) -> QueryResult<()> {
        let writes = writes.into_vec();
        let Some(first) = writes.first() else {
            return Ok(());
        };
        let column_count = first.row.key.partition.len()
            + first.row.key.clustering.len()
            + first.row.columns.len();
        let max_variable_number = self.max_variable_number().await?;
        let rows_per_query = (max_variable_number / column_count).max(1);
        let names = key_columns(&first.row.key)
            .into_iter()
            .chain(first.row.columns.iter().cloned())
            .map(|column| quote(&column.column))
            .collect::<Vec<_>>()
            .join(", ");
        let row_placeholders = format!(
            "({})",
            std::iter::repeat_n("?", column_count)
                .collect::<Vec<_>>()
                .join(", ")
        );
        let mut transaction = self.pool.begin().await.map_err(error("begin"))?;

        for chunk in writes.chunks(rows_per_query) {
            let sql = format!(
                "INSERT OR REPLACE INTO {} ({names}) VALUES {}",
                Self::table_name(first.table),
                std::iter::repeat_n(row_placeholders.as_str(), chunk.len())
                    .collect::<Vec<_>>()
                    .join(", "),
            );
            let mut query = sqlx::query(sqlx::AssertSqlSafe(sql));
            for write in chunk {
                for column in key_columns(&write.row.key)
                    .into_iter()
                    .chain(write.row.columns.iter().cloned())
                {
                    query = bind(query, column.value)?;
                }
            }
            query
                .execute(&mut *transaction)
                .await
                .map_err(error("batch put"))?;
        }
        transaction.commit().await.map_err(error("commit"))?;
        Ok(())
    }
}

fn key_columns(key: &MetaKey) -> Vec<Cell> {
    key.partition
        .iter()
        .chain(key.clustering.iter())
        .cloned()
        .collect()
}

fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn sql_type(kind: CellType) -> &'static str {
    match kind {
        CellType::Bool => "INTEGER",
        CellType::Number => "INTEGER",
        CellType::Bytes => "BLOB",
    }
}

fn bind<'q>(
    query: sqlx::query::Query<'q, Sqlite, SqliteArguments>,
    value: CellValue,
) -> QueryResult<sqlx::query::Query<'q, Sqlite, SqliteArguments>> {
    Ok(match value {
        CellValue::Bool(value) => query.bind(i64::from(value)),
        CellValue::Number(value) => query.bind(value),
        CellValue::Bytes(value) => query.bind(value.into_vec()),
    })
}

fn column_value(row: &sqlx::sqlite::SqliteRow, column: &Cell) -> QueryResult<CellValue> {
    match &column.value {
        CellValue::Bool(_) => row
            .try_get::<i64, _>(column.column.as_ref())
            .map(|value| CellValue::Bool(value != 0))
            .map_err(|_| {
                QueryError::Backend(format!("unsupported sqlite column `{}`", column.column))
            }),
        CellValue::Number(_) => row
            .try_get::<i64, _>(column.column.as_ref())
            .map(|value| CellValue::Number(value.to_string()))
            .map_err(|_| {
                QueryError::Backend(format!("unsupported sqlite column `{}`", column.column))
            }),
        CellValue::Bytes(_) => row
            .try_get::<Vec<u8>, _>(column.column.as_ref())
            .map(|value| CellValue::Bytes(value.into_boxed_slice()))
            .map_err(|_| {
                QueryError::Backend(format!("unsupported sqlite column `{}`", column.column))
            }),
    }
}

fn decode_row(row: sqlx::sqlite::SqliteRow, key: &MetaKey) -> QueryResult<MetaRow> {
    let mut columns = Vec::new();
    for name in row.columns().iter().map(|column| column.name()) {
        if key.partition.iter().any(|column| column.column == name)
            || key.clustering.iter().any(|column| column.column == name)
        {
            continue;
        }
        let value = if let Ok(value) = row.try_get::<i64, _>(name) {
            CellValue::Number(value.to_string())
        } else if let Ok(value) = row.try_get::<Vec<u8>, _>(name) {
            CellValue::Bytes(value.into_boxed_slice())
        } else {
            return Err(QueryError::Backend(format!(
                "unsupported sqlite column `{name}`"
            )));
        };

        columns.push(Cell::new(name.to_string(), value));
    }
    Ok(MetaRow {
        key: key.clone(),
        columns: columns.into_boxed_slice(),
    })
}

fn error(operation: &'static str) -> impl Fn(sqlx::Error) -> QueryError {
    move |error| QueryError::Backend(format!("sqlite {operation}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEFINITION: MetaTableDef = MetaTableDef {
        partition_key: &[ColumnDef::new_number("id")],
        clustering_key: &[],
        columns: &[ColumnDef::new_byte("value")],
    };

    #[tokio::test]
    async fn stores_native_sqlite_columns() {
        let store = SqliteMetaStore::new_with_config(SqliteMetaStoreConfig::new("sqlite::memory:"))
            .await
            .unwrap();
        let table = MetaTableId::new("test");
        let key = MetaKey {
            partition: Box::new([Cell {
                column: "id".into(),
                value: CellValue::Number("7".into()),
            }]),
            clustering: Box::new([]),
        };
        store.init(table, DEFINITION).await.unwrap();
        store
            .put(
                table,
                MetaRow {
                    key: key.clone(),
                    columns: Box::new([Cell {
                        column: "value".into(),
                        value: CellValue::Bytes(b"ok".to_vec().into_boxed_slice()),
                    }]),
                },
            )
            .await
            .unwrap();
        assert!(store.get(table, key).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn rejects_existing_schema_mismatch() {
        let store = SqliteMetaStore::new_with_config(SqliteMetaStoreConfig::new("sqlite::memory:"))
            .await
            .unwrap();
        sqlx::query("CREATE TABLE test (id INTEGER NOT NULL PRIMARY KEY, value TEXT NOT NULL)")
            .execute(&store.pool)
            .await
            .unwrap();

        let error = store.init(MetaTableId::new("test"), DEFINITION).await;
        assert!(
            matches!(error, Err(QueryError::Backend(message)) if message.contains("schema mismatch"))
        );
    }
}
