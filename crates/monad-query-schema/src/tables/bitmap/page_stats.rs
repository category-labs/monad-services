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

use crate::{bitmap::BitmapFilterValue, family::Family};

pub struct BitmapPageStatsKey {
    pub family: Family,
    pub filter: BitmapFilterValue,
    pub page: u64,
}

pub struct BitmapPageStatsSchema;

impl MetaTableSchema for BitmapPageStatsSchema {
    const TABLE: MetaTableId = MetaTableId::new("bitmap_page_stats");

    type Key = BitmapPageStatsKey;
    type Value = u32;

    const DEFINITION: MetaTableDef = MetaTableDef {
        partition_key: &[
            ColumnDef::new_number("family"),
            ColumnDef::new_byte("filter"),
        ],
        clustering_key: &[ColumnDef::new_number("page")],
        columns: &[ColumnDef::new_number("count")],
    };

    fn key(key: &Self::Key) -> MetaKey {
        MetaKey {
            partition: Box::new([
                Cell::new_number("family", Into::<u8>::into(key.family) as u64),
                Cell::new_bytes("filter", key.filter.encode()),
            ]),
            clustering: Box::new([Cell::new_number("page", key.page)]),
        }
    }

    fn decode_key(key: MetaKey) -> QueryResult<Self::Key> {
        let family = match &key.partition[0].value {
            CellValue::Number(v) => v
                .parse::<u8>()
                .map_err(|_| QueryError::Decode("invalid bitmap family"))?,
            _ => return Err(QueryError::Decode("invalid bitmap family")),
        };

        let filter = match &key.partition[1].value {
            CellValue::Bytes(v) => BitmapFilterValue::decode(v)?,
            _ => return Err(QueryError::Decode("invalid bitmap filter")),
        };

        let page = match &key.clustering[0].value {
            CellValue::Number(value) => value
                .parse::<u64>()
                .map_err(|_| QueryError::Decode("invalid bitmap page number"))?,
            _ => return Err(QueryError::Decode("invalid bitmap stats number")),
        };

        Ok(BitmapPageStatsKey {
            family: Family::try_from(family)
                .map_err(|_| QueryError::Decode("invalid bitmap family"))?,
            filter,
            page,
        })
    }

    fn columns(value: &Self::Value) -> RowCells {
        Box::new([Cell::new_number("count", *value as u64)])
    }

    fn decode(columns: RowCells) -> QueryResult<Self::Value> {
        if columns.len() != 1 {
            return Err(QueryError::Decode("invalid bitmap page stats"));
        }

        match &columns[0].value {
            CellValue::Number(value) => value
                .parse::<u64>()
                .map_err(|_| QueryError::Decode("invalid bitmap stats number"))?
                .try_into()
                .map_err(|_| QueryError::Decode("invalid bitmap page count")),
            _ => Err(QueryError::Decode("invalid bitmap stats number")),
        }
    }
}
