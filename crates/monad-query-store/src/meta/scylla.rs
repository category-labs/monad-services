use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use monad_query_types::{
    store::meta::{
        column::{Cell, CellType, CellValue, RowCells},
        table::{MetaTableDef, MetaTableId},
        MetaKey, MetaRow, MetaScanOrder, MetaStore,
    },
    QueryError, QueryResult,
};
use scylla::{
    client::{session::Session, session_builder::SessionBuilder},
    value::CqlValue,
};

#[derive(Debug, Clone)]
pub struct ScyllaMetaStoreConfig {
    pub nodes: Box<[String]>,
    pub keyspace: String,
}

impl ScyllaMetaStoreConfig {
    pub fn new(
        nodes: impl IntoIterator<Item = impl Into<String>>,
        keyspace: impl Into<String>,
    ) -> Self {
        Self {
            nodes: nodes.into_iter().map(Into::into).collect(),
            keyspace: keyspace.into(),
        }
    }
}

#[derive(Clone)]
pub struct ScyllaMetaStore {
    session: Arc<Session>,
    definitions: Arc<RwLock<HashMap<MetaTableId, MetaTableDef>>>,
}

impl ScyllaMetaStore {
    pub async fn new(config: ScyllaMetaStoreConfig) -> QueryResult<Self> {
        if config.nodes.is_empty() {
            return Err(QueryError::InvalidRequest(
                "Scylla metadata store needs a node",
            ));
        }
        let session = SessionBuilder::new()
            .known_nodes(config.nodes.iter())
            .use_keyspace(config.keyspace.clone(), false)
            .build()
            .await
            .map_err(error("connect"))?;
        Ok(Self::new_with_session(session, config.keyspace))
    }

    pub fn new_with_session(session: Session, _keyspace: impl Into<String>) -> Self {
        Self {
            session: Arc::new(session),
            definitions: Arc::default(),
        }
    }

    fn columns(
        definition: MetaTableDef,
    ) -> impl Iterator<Item = &'static monad_query_types::store::meta::column::ColumnDef> {
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

    fn cql_type(kind: CellType) -> &'static str {
        match kind {
            CellType::Bool => "boolean",
            CellType::Number => "bigint",
            CellType::Bytes => "blob",
        }
    }

    fn identifier(value: &str) -> String {
        format!("\"{}\"", value.replace('"', "\"\""))
    }

    fn cql_value(value: &CellValue) -> QueryResult<CqlValue> {
        Ok(match value {
            CellValue::Bool(value) => CqlValue::Boolean(*value),
            CellValue::Number(value) => CqlValue::BigInt(scylla_number(value)?),
            CellValue::Bytes(value) => CqlValue::Blob(value.to_vec()),
        })
    }

    fn values<'a>(values: impl IntoIterator<Item = &'a CellValue>) -> QueryResult<Vec<CqlValue>> {
        values.into_iter().map(Self::cql_value).collect()
    }

    fn decode_value(value: CqlValue, kind: CellType) -> QueryResult<CellValue> {
        match (kind, value) {
            (CellType::Bool, CqlValue::Boolean(value)) => Ok(CellValue::Bool(value)),
            (CellType::Number, CqlValue::BigInt(value)) => Ok(CellValue::Number(
                (value.cast_unsigned() ^ (1_u64 << 63)).to_string(),
            )),
            (CellType::Bytes, CqlValue::Blob(value)) => {
                Ok(CellValue::Bytes(value.into_boxed_slice()))
            }
            _ => Err(QueryError::Backend(
                "Scylla returned a value with the wrong type".into(),
            )),
        }
    }

    fn decode_row(&self, table: MetaTableId, values: Vec<CqlValue>) -> QueryResult<MetaRow> {
        let definition = *self
            .definitions
            .read()
            .expect("metadata definitions lock is not poisoned")
            .get(&table)
            .ok_or(QueryError::Backend(
                "metadata table is not initialized".into(),
            ))?;
        let definitions = Self::columns(definition).collect::<Vec<_>>();
        if definitions.len() != values.len() {
            return Err(QueryError::Backend(
                "Scylla returned an unexpected column count".into(),
            ));
        }
        let cells = definitions
            .into_iter()
            .zip(values)
            .map(|(definition, value)| {
                Ok(Cell {
                    column: definition.name.into(),
                    value: Self::decode_value(value, definition.kind)?,
                })
            })
            .collect::<QueryResult<Vec<_>>>()?;
        let (key, columns) =
            cells.split_at(definition.partition_key.len() + definition.clustering_key.len());
        Ok(MetaRow {
            key: MetaKey {
                partition: key[..definition.partition_key.len()]
                    .to_vec()
                    .into_boxed_slice(),
                clustering: key[definition.partition_key.len()..]
                    .to_vec()
                    .into_boxed_slice(),
            },
            columns: columns.to_vec().into_boxed_slice(),
        })
    }

    async fn select(
        &self,
        sql: String,
        values: Vec<CqlValue>,
        table: MetaTableId,
    ) -> QueryResult<Vec<MetaRow>> {
        let result = self
            .session
            .query_unpaged(sql, values)
            .await
            .map_err(error("query"))?;
        let rows = result.into_rows_result().map_err(error("decode rows"))?;
        let count = self
            .definitions
            .read()
            .expect("metadata definitions lock is not poisoned")
            .get(&table)
            .map(|definition| Self::columns(*definition).count())
            .ok_or(QueryError::Backend(
                "metadata table is not initialized".into(),
            ))?;
        macro_rules! decode_rows {
            ($type:ty, [$($column:ident),+]) => {
                rows.rows::<$type>().map_err(error("decode rows"))?.map(|row| {
                    let ($($column),+) = row.map_err(error("decode row"))?;
                    self.decode_row(table, vec![$($column),+])
                }).collect()
            };
        }
        match count {
            1 => rows
                .rows::<(CqlValue,)>()
                .map_err(error("decode rows"))?
                .map(|row| self.decode_row(table, vec![row.map_err(error("decode row"))?.0]))
                .collect(),
            2 => decode_rows!((CqlValue, CqlValue), [a, b]),
            3 => decode_rows!((CqlValue, CqlValue, CqlValue), [a, b, c]),
            4 => decode_rows!((CqlValue, CqlValue, CqlValue, CqlValue), [a, b, c, d]),
            5 => decode_rows!(
                (CqlValue, CqlValue, CqlValue, CqlValue, CqlValue),
                [a, b, c, d, e]
            ),
            6 => decode_rows!(
                (CqlValue, CqlValue, CqlValue, CqlValue, CqlValue, CqlValue),
                [a, b, c, d, e, f]
            ),
            7 => decode_rows!(
                (CqlValue, CqlValue, CqlValue, CqlValue, CqlValue, CqlValue, CqlValue),
                [a, b, c, d, e, f, g]
            ),
            8 => decode_rows!(
                (CqlValue, CqlValue, CqlValue, CqlValue, CqlValue, CqlValue, CqlValue, CqlValue),
                [a, b, c, d, e, f, g, h]
            ),
            _ => Err(QueryError::Backend(
                "metadata table has too many columns for Scylla row decoding".into(),
            )),
        }
    }
}

fn scylla_number(value: &str) -> QueryResult<i64> {
    let number = value.parse::<u64>().map_err(|_| {
        QueryError::InvalidRequest("metadata numbers must be unsigned 64-bit integers")
    })?;
    Ok((number ^ (1_u64 << 63)).cast_signed())
}

impl MetaStore for ScyllaMetaStore {
    async fn init(&self, table: MetaTableId, definition: MetaTableDef) -> QueryResult<()> {
        if definition.partition_key.is_empty() {
            return Err(QueryError::InvalidRequest(
                "metadata table needs a partition key",
            ));
        }
        let definitions = Self::columns(definition).collect::<Vec<_>>();
        if definitions.iter().enumerate().any(|(index, column)| {
            definitions[..index]
                .iter()
                .any(|previous| previous.name == column.name)
        }) {
            return Err(QueryError::InvalidRequest(
                "metadata schema has duplicate columns",
            ));
        }
        let mut sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (",
            Self::identifier(table.as_str())
        );
        sql.push_str(
            &definitions
                .iter()
                .map(|column| {
                    format!(
                        "{} {}",
                        Self::identifier(column.name),
                        Self::cql_type(column.kind)
                    )
                })
                .collect::<Vec<_>>()
                .join(", "),
        );
        sql.push_str(", PRIMARY KEY ((");
        sql.push_str(
            &definition
                .partition_key
                .iter()
                .map(|column| Self::identifier(column.name))
                .collect::<Vec<_>>()
                .join(", "),
        );
        if definition.clustering_key.is_empty() {
            sql.push_str("))");
        } else {
            sql.push_str("), ");
            sql.push_str(
                &definition
                    .clustering_key
                    .iter()
                    .map(|column| Self::identifier(column.name))
                    .collect::<Vec<_>>()
                    .join(", "),
            );
            sql.push(')');
        }
        self.session
            .query_unpaged(sql, ())
            .await
            .map_err(error("create table"))?;
        self.definitions
            .write()
            .expect("metadata definitions lock is not poisoned")
            .insert(table, definition);
        Ok(())
    }

    async fn get(&self, table: MetaTableId, key: MetaKey) -> QueryResult<Option<MetaRow>> {
        if key.partition.is_empty() {
            return Err(QueryError::InvalidRequest("missing partition key"));
        }
        let predicates = Self::key_columns(&key)
            .map(|cell| format!("{} = ?", Self::identifier(&cell.column)))
            .collect::<Vec<_>>()
            .join(" AND ");
        let sql = format!(
            "SELECT * FROM {} WHERE {} LIMIT 1",
            Self::identifier(table.as_str()),
            predicates
        );
        Ok(self
            .select(
                sql,
                Self::values(Self::key_columns(&key).map(|cell| &cell.value))?,
                table,
            )
            .await?
            .into_iter()
            .next())
    }

    async fn get_many(
        &self,
        table: MetaTableId,
        keys: Box<[MetaKey]>,
    ) -> QueryResult<Box<[MetaRow]>> {
        MetaKey::validate_batch_layout(&keys)?;
        let mut rows = Vec::new();
        for key in keys {
            if let Some(row) = self.get(table, key).await? {
                rows.push(row);
            }
        }
        Ok(rows.into_boxed_slice())
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
        let [start] = start.as_ref() else {
            return Err(QueryError::InvalidRequest("missing range start"));
        };
        let [end] = end.as_ref() else {
            return Err(QueryError::InvalidRequest("missing range end"));
        };
        let predicates = partition
            .iter()
            .map(|cell| format!("{} = ?", Self::identifier(&cell.column)))
            .chain([format!(
                "{} >= ? AND {} <= ?",
                Self::identifier(&start.column),
                Self::identifier(&end.column)
            )])
            .collect::<Vec<_>>()
            .join(" AND ");
        let mut sql = format!(
            "SELECT * FROM {} WHERE {} ORDER BY {} {}",
            Self::identifier(table.as_str()),
            predicates,
            Self::identifier(&start.column),
            match order {
                MetaScanOrder::Ascending => "ASC",
                MetaScanOrder::Descending => "DESC",
            }
        );
        if let Some(limit) = limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }
        let values = Self::values(
            partition
                .iter()
                .map(|cell| &cell.value)
                .chain([&start.value, &end.value]),
        )?;
        self.select(sql, values, table).await
    }

    async fn seek_le(
        &self,
        table: MetaTableId,
        partition: RowCells,
        clustering: RowCells,
    ) -> QueryResult<Option<MetaRow>> {
        let [clustering] = clustering.as_ref() else {
            return Err(QueryError::InvalidRequest(
                "metadata seek requires exactly one clustering column",
            ));
        };
        let predicates = partition
            .iter()
            .map(|cell| format!("{} = ?", Self::identifier(&cell.column)))
            .chain([format!("{} <= ?", Self::identifier(&clustering.column))])
            .collect::<Vec<_>>()
            .join(" AND ");
        let sql = format!(
            "SELECT * FROM {} WHERE {} ORDER BY {} DESC LIMIT 1",
            Self::identifier(table.as_str()),
            predicates,
            Self::identifier(&clustering.column)
        );
        Ok(self
            .select(
                sql,
                Self::values(
                    partition
                        .iter()
                        .map(|cell| &cell.value)
                        .chain([&clustering.value]),
                )?,
                table,
            )
            .await?
            .into_iter()
            .next())
    }

    async fn put(&self, table: MetaTableId, row: MetaRow) -> QueryResult<()> {
        let columns = Self::row_columns(&row).collect::<Vec<_>>();
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            Self::identifier(table.as_str()),
            columns
                .iter()
                .map(|cell| Self::identifier(&cell.column))
                .collect::<Vec<_>>()
                .join(", "),
            std::iter::repeat_n("?", columns.len())
                .collect::<Vec<_>>()
                .join(", ")
        );
        self.session
            .query_unpaged(sql, Self::values(columns.iter().map(|cell| &cell.value))?)
            .await
            .map_err(error("put"))?;
        Ok(())
    }

    async fn put_batch(&self, table: MetaTableId, rows: Box<[MetaRow]>) -> QueryResult<()> {
        MetaRow::validate_batch_layout(&rows)?;
        for row in rows {
            self.put(table, row).await?;
        }
        Ok(())
    }
}

fn error<E: std::fmt::Display>(operation: &'static str) -> impl Fn(E) -> QueryError {
    move |error| QueryError::Backend(format!("scylla {operation}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::scylla_number;

    #[test]
    fn number_mapping_preserves_u64_order() {
        let values = [0, 1, i64::MAX as u64, 1_u64 << 63, u64::MAX];
        let encoded = values
            .iter()
            .map(|value| scylla_number(&value.to_string()).unwrap())
            .collect::<Vec<_>>();

        assert!(encoded.windows(2).all(|pair| pair[0] < pair[1]));
        for (value, encoded) in values.into_iter().zip(encoded) {
            let decoded = encoded.cast_unsigned() ^ (1_u64 << 63);
            assert_eq!(decoded, value);
        }
    }
}
