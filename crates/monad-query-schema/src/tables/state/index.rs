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
        column::{Cell, CellValue, ColumnDef, RowCells},
        table::{MetaTableDef, MetaTableId, MetaTableSchema},
        MetaKey,
    },
    QueryError, QueryResult,
};

pub struct StateIndexKey;

#[derive(Default)]
pub struct StateIndex {
    pub indexed_start: Option<u64>,
    pub next_block_id: u64,
    pub next_log_id: u64,
    pub next_tx_id: u64,
    pub next_trace_id: u64,
}

pub struct StateIndexSchema;

impl MetaTableSchema for StateIndexSchema {
    const TABLE: MetaTableId = MetaTableId::new("state_index");

    type Key = StateIndexKey;
    type Value = StateIndex;

    const DEFINITION: MetaTableDef = MetaTableDef {
        partition_key: &[ColumnDef::new_number("id")],
        clustering_key: &[],
        columns: &[
            ColumnDef::new_number("indexed_start"),
            ColumnDef::new_number("next_block_id"),
            ColumnDef::new_number("next_log_id"),
            ColumnDef::new_number("next_tx_id"),
            ColumnDef::new_number("next_trace_id"),
        ],
    };

    fn key(_: &Self::Key) -> MetaKey {
        MetaKey {
            partition: Box::new([Cell {
                column: "id".into(),
                value: CellValue::Number("0".into()),
            }]),
            clustering: Box::new([]),
        }
    }
    fn decode_key(_: MetaKey) -> QueryResult<Self::Key> {
        Ok(StateIndexKey)
    }

    fn columns(value: &Self::Value) -> RowCells {
        Box::new([
            Cell::new_number("indexed_start", value.indexed_start.unwrap_or(0)),
            Cell::new_number("next_block_id", value.next_block_id),
            Cell::new_number("next_log_id", value.next_log_id),
            Cell::new_number("next_tx_id", value.next_tx_id),
            Cell::new_number("next_trace_id", value.next_trace_id),
        ])
    }

    fn decode(columns: RowCells) -> QueryResult<Self::Value> {
        let [Cell {
            column: column_indexed_start_name,
            value: column_indexed_start_value,
        }, Cell {
            column: column_next_block_id_name,
            value: column_next_block_id_value,
        }, Cell {
            column: column_next_log_id_name,
            value: column_next_log_id_value,
        }, Cell {
            column: column_next_tx_id_name,
            value: column_next_tx_id_value,
        }, Cell {
            column: column_next_trace_id_name,
            value: column_next_trace_id_value,
        }] = Vec::from(columns)
            .try_into()
            .map_err(|_| QueryError::Decode("invalid state index columns"))?;

        if [
            column_indexed_start_name.as_ref(),
            column_next_block_id_name.as_ref(),
            column_next_log_id_name.as_ref(),
            column_next_tx_id_name.as_ref(),
            column_next_trace_id_name.as_ref(),
        ] != [
            "indexed_start",
            "next_block_id",
            "next_log_id",
            "next_tx_id",
            "next_trace_id",
        ] {
            return Err(QueryError::Decode("invalid state index columns"));
        }

        let number = |column: &Cell| match &column.value {
            CellValue::Number(value) => value
                .parse()
                .map_err(|_| QueryError::Decode("invalid state index Cell::new_number")),
            _ => Err(QueryError::Decode("invalid state index Cell::new_number")),
        };

        Ok(Self::Value {
            indexed_start: Some(number(&Cell {
                column: column_indexed_start_name,
                value: column_indexed_start_value,
            })?),
            next_block_id: number(&Cell {
                column: column_next_block_id_name,
                value: column_next_block_id_value,
            })?,
            next_log_id: number(&Cell {
                column: column_next_log_id_name,
                value: column_next_log_id_value,
            })?,
            next_tx_id: number(&Cell {
                column: column_next_tx_id_name,
                value: column_next_tx_id_value,
            })?,
            next_trace_id: number(&Cell {
                column: column_next_trace_id_name,
                value: column_next_trace_id_value,
            })?,
        })
    }
}
