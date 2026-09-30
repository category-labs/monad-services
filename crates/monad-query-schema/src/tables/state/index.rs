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

pub struct StateIndex {
    pub indexed_start: Option<u64>,
    pub indexed_head: Option<u64>,
    pub published_head: Option<u64>,
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
            ColumnDef::new_number("indexed_head"),
            ColumnDef::new_number("published_head"),
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
            Cell::new_number("indexed_head", value.indexed_head.unwrap_or(0)),
            Cell::new_number("published_head", value.published_head.unwrap_or(0)),
            Cell::new_number("next_log_id", value.next_log_id),
            Cell::new_number("next_tx_id", value.next_tx_id),
            Cell::new_number("next_trace_id", value.next_trace_id),
        ])
    }

    fn decode(columns: RowCells) -> QueryResult<Self::Value> {
        if columns.len() != 6 {
            return Err(QueryError::Decode("invalid state index columns"));
        }
        let number = |index: usize| match &columns[index].value {
            CellValue::Number(value) => value
                .parse()
                .map_err(|_| QueryError::Decode("invalid state index Cell::new_number")),
            _ => Err(QueryError::Decode("invalid state index Cell::new_number")),
        };
        Ok(Self::Value {
            indexed_start: Some(number(0)?),
            indexed_head: Some(number(1)?),
            published_head: Some(number(2)?),
            next_log_id: number(3)?,
            next_tx_id: number(4)?,
            next_trace_id: number(5)?,
        })
    }
}
