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
    PrimaryId, QueryError, QueryResult,
};

use crate::family::{Family, FamilyMetadata};

pub struct MetadataFamilyKey {
    pub family: Family,
    pub block_height: u64,
}

pub struct MetadataFamilySchema;

impl MetaTableSchema for MetadataFamilySchema {
    const TABLE: MetaTableId = MetaTableId::new("metadata_family");

    type Key = MetadataFamilyKey;
    type Value = FamilyMetadata;

    const DEFINITION: MetaTableDef = MetaTableDef {
        partition_key: &[ColumnDef::new_number("family")],
        clustering_key: &[ColumnDef::new_number("block_height")],
        columns: &[
            ColumnDef::new_number("first_primary_id"),
            ColumnDef::new_number("row_count"),
        ],
    };

    fn key(key: &Self::Key) -> MetaKey {
        MetaKey {
            partition: Box::new([Cell::new_number("family", u8::from(key.family) as u64)]),
            clustering: Box::new([Cell::new_number("block_height", key.block_height)]),
        }
    }

    fn decode_key(key: MetaKey) -> QueryResult<Self::Key> {
        let number = |column: &Cell| match &column.value {
            CellValue::Number(value) => value
                .parse::<u64>()
                .map_err(|_| QueryError::Decode("invalid family metadata number")),
            _ => Err(QueryError::Decode("invalid family metadata number")),
        };
        let family = u8::try_from(number(&key.partition[0])?)
            .map_err(|_| QueryError::Decode("invalid family metadata family"))?
            .try_into()
            .map_err(|_| QueryError::Decode("invalid family metadata family"))?;
        Ok(MetadataFamilyKey {
            family,
            block_height: number(&key.clustering[0])?,
        })
    }

    fn columns(value: &Self::Value) -> RowCells {
        Box::new([
            Cell::new_number("first_primary_id", value.first_primary_id.as_u64()),
            Cell::new_number("row_count", value.count.into()),
        ])
    }

    fn decode(columns: RowCells) -> QueryResult<Self::Value> {
        if columns.len() != 2 {
            return Err(QueryError::Decode("invalid family metadata columns"));
        }

        let number = |column: &Cell| match &column.value {
            CellValue::Number(value) => value
                .parse::<u64>()
                .map_err(|_| QueryError::Decode("invalid family metadata number")),
            _ => Err(QueryError::Decode("invalid family metadata number")),
        };
        Ok(FamilyMetadata {
            first_primary_id: PrimaryId::new(number(&columns[0])?),
            count: number(&columns[1])?
                .try_into()
                .map_err(|_| QueryError::Decode("invalid row count"))?,
        })
    }
}
