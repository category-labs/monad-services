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

use bytes::Bytes;
use monad_query_types::{
    store::{
        blob::table::{TableKey, TableValue},
        meta::{
            column::{Cell, CellValue, ColumnDef, RowCells},
            table::{MetaTableDef, MetaTableId, MetaTableSchema},
            MetaKey,
        },
    },
    PrimaryId, QueryError, QueryResult,
};

use crate::family::Family;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PayloadLocationKey {
    pub family: Family,
    pub id: PrimaryId,
}

impl TableKey for PayloadLocationKey {
    fn encode(&self) -> Box<[u8]> {
        let mut bytes = [0; 9];

        bytes[0] = self.family.into();
        bytes[1..].copy_from_slice(&self.id.as_u64().to_be_bytes());

        bytes.into()
    }

    fn decode(bytes: &[u8]) -> QueryResult<Self> {
        if bytes.len() != 9 {
            return Err(QueryError::Decode("invalid payload location key"));
        }

        Ok(Self {
            family: Family::try_from(bytes[0])
                .map_err(|_| QueryError::Decode("invalid family tag"))?,
            id: PrimaryId::new(u64::from_be_bytes(bytes[1..].try_into().unwrap())),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PayloadLocation {
    offset_end: u64,
}

impl PayloadLocation {
    pub fn new(offset_end: u64) -> Self {
        Self { offset_end }
    }

    pub fn payload_end(&self) -> u64 {
        self.offset_end
    }
}

impl TableValue for PayloadLocation {
    fn encode(&self) -> Box<[u8]> {
        self.offset_end.to_be_bytes().into()
    }

    fn decode(bytes: Bytes) -> QueryResult<Self> {
        let offset_end = bytes
            .as_ref()
            .try_into()
            .map(u64::from_be_bytes)
            .map_err(|_| QueryError::Decode("invalid payload location"))?;
        Ok(Self::new(offset_end))
    }
}

pub struct PayloadLocationSchema;

impl MetaTableSchema for PayloadLocationSchema {
    const TABLE: MetaTableId = MetaTableId::new("payload_location");

    type Key = PayloadLocationKey;
    type Value = PayloadLocation;

    const DEFINITION: MetaTableDef = MetaTableDef {
        partition_key: &[ColumnDef::new_number("family")],
        clustering_key: &[ColumnDef::new_number("primary_id")],
        columns: &[ColumnDef::new_number("offset_end")],
    };

    fn key(key: &Self::Key) -> MetaKey {
        MetaKey {
            partition: Box::new([Cell::new_number("family", u8::from(key.family) as u64)]),
            clustering: Box::new([Cell::new_number("primary_id", key.id.as_u64())]),
        }
    }

    fn decode_key(key: MetaKey) -> QueryResult<Self::Key> {
        let family = match &key.partition[0].value {
            CellValue::Number(value) => value
                .parse::<u8>()
                .map_err(|_| QueryError::Decode("invalid payload family"))?,
            _ => return Err(QueryError::Decode("invalid payload family")),
        };

        let id = match &key.clustering[0].value {
            CellValue::Number(value) => value
                .parse()
                .map_err(|_| QueryError::Decode("invalid payload id"))?,
            _ => return Err(QueryError::Decode("invalid payload id")),
        };

        Ok(PayloadLocationKey {
            family: Family::try_from(family)
                .map_err(|_| QueryError::Decode("invalid payload family"))?,
            id: PrimaryId::new(id),
        })
    }

    fn columns(value: &Self::Value) -> RowCells {
        Box::new([Cell::new_number("offset_end", value.offset_end)])
    }

    fn decode(columns: RowCells) -> QueryResult<Self::Value> {
        let [Cell {
            column: column_offset_end_name,
            value: column_offset_end_value,
        }] = Vec::from(columns)
            .try_into()
            .map_err(|_| QueryError::Decode("invalid payload location columns"))?;

        if column_offset_end_name != "offset_end" {
            return Err(QueryError::Decode("invalid payload location columns"));
        }

        let offset_end = match &column_offset_end_value {
            CellValue::Number(value) => value
                .parse()
                .map_err(|_| QueryError::Decode("invalid payload location number")),
            _ => Err(QueryError::Decode("invalid payload location number")),
        }?;

        Ok(PayloadLocation::new(offset_end))
    }
}
