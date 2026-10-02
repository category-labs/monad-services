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

use crate::family::Family;

pub struct MetadataFamilyBlockIndexKey {
    pub family: Family,
    pub first_primary_id: u64,
}

pub struct MetadataFamilyBlockIndexValue {
    pub block_height: u64,
}

pub struct MetadataFamilyBlockIndexSchema;

impl MetaTableSchema for MetadataFamilyBlockIndexSchema {
    const TABLE: MetaTableId = MetaTableId::new("metadata_family_block_index");

    type Key = MetadataFamilyBlockIndexKey;
    type Value = MetadataFamilyBlockIndexValue;

    const DEFINITION: MetaTableDef = MetaTableDef {
        partition_key: &[ColumnDef::new_number("family")],
        clustering_key: &[ColumnDef::new_number("first_primary_id")],
        columns: &[ColumnDef::new_number("block_height")],
    };

    fn key(key: &Self::Key) -> MetaKey {
        MetaKey {
            partition: Box::new([Cell::new_number("family", u8::from(key.family) as u64)]),
            clustering: Box::new([Cell::new_number("first_primary_id", key.first_primary_id)]),
        }
    }

    fn decode_key(key: MetaKey) -> QueryResult<Self::Key> {
        let number = |column: &Cell| match &column.value {
            CellValue::Number(value) => value
                .parse::<u64>()
                .map_err(|_| QueryError::Decode("invalid family block index number")),
            _ => Err(QueryError::Decode("invalid family block index number")),
        };

        let family = u8::try_from(number(&key.partition[0])?)
            .map_err(|_| QueryError::Decode("invalid family block index family"))?
            .try_into()
            .map_err(|_| QueryError::Decode("invalid family block index family"))?;

        Ok(MetadataFamilyBlockIndexKey {
            family,
            first_primary_id: number(&key.clustering[0])?,
        })
    }

    fn columns(value: &Self::Value) -> RowCells {
        Box::new([Cell::new_number("block_height", value.block_height)])
    }

    fn decode(columns: RowCells) -> QueryResult<Self::Value> {
        let [Cell {
            column: column_block_height_name,
            value: column_block_height_value,
        }] = Vec::from(columns)
            .try_into()
            .map_err(|_| QueryError::Decode("invalid family block index columns"))?;

        if column_block_height_name != "block_height" {
            return Err(QueryError::Decode("invalid family block index columns"));
        }

        Ok(MetadataFamilyBlockIndexValue {
            block_height: match &column_block_height_value {
                CellValue::Number(value) => value
                    .parse()
                    .map_err(|_| QueryError::Decode("invalid block height"))?,
                _ => return Err(QueryError::Decode("invalid block height")),
            },
        })
    }
}
