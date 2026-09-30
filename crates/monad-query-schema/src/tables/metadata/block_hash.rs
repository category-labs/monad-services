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

use alloy_primitives::B256;
use monad_query_types::{
    store::meta::{
        column::{Cell, CellValue, ColumnDef, RowCells},
        table::{MetaTableDef, MetaTableId, MetaTableSchema},
        MetaKey,
    },
    QueryError, QueryResult,
};

pub struct MetadataBlockHashKey {
    pub block_height: u64,
}

pub struct MetadataBlockHashValue(pub B256);

pub struct MetadataBlockHashSchema;

impl MetaTableSchema for MetadataBlockHashSchema {
    const TABLE: MetaTableId = MetaTableId::new("metadata_block_hash");

    type Key = MetadataBlockHashKey;
    type Value = MetadataBlockHashValue;

    const DEFINITION: MetaTableDef = MetaTableDef {
        partition_key: &[ColumnDef::new_number("id")],
        clustering_key: &[ColumnDef::new_number("block_height")],
        columns: &[ColumnDef::new_byte("block_hash")],
    };

    fn key(key: &Self::Key) -> MetaKey {
        MetaKey {
            partition: Box::new([Cell::new_number("id", 0)]),
            clustering: Box::new([Cell::new_number("block_height", key.block_height)]),
        }
    }

    fn decode_key(key: MetaKey) -> QueryResult<Self::Key> {
        let block_height = match &key.clustering[0].value {
            CellValue::Number(value) => value
                .parse()
                .map_err(|_| QueryError::Decode("invalid block height"))?,
            _ => return Err(QueryError::Decode("invalid block height")),
        };
        Ok(MetadataBlockHashKey { block_height })
    }

    fn columns(value: &Self::Value) -> RowCells {
        Box::new([Cell {
            column: "block_hash".into(),
            value: CellValue::Bytes(value.0.as_slice().into()),
        }])
    }

    fn decode(columns: RowCells) -> QueryResult<Self::Value> {
        if columns.len() != 1 {
            return Err(QueryError::Decode("invalid block hash columns"));
        }

        let bytes = match &columns[0].value {
            CellValue::Bytes(value) if value.len() == 32 => value,
            _ => return Err(QueryError::Decode("invalid block hash")),
        };
        Ok(MetadataBlockHashValue(B256::from_slice(bytes)))
    }
}
