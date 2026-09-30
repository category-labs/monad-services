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
    store::blob::table::{TableKey, TableValue},
    PrimaryId, QueryError, QueryResult,
};
use roaring::RoaringBitmap;

use crate::{
    bitmap::{BitmapFilterKey, BitmapFilterValue},
    family::Family,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BitmapIndexKey {
    pub family: Family,
    pub filter: BitmapFilterKey,
    pub page: u64,
}

impl TableKey for BitmapIndexKey {
    fn encode(&self) -> Box<[u8]> {
        let filter = self.filter.value.encode();

        let mut bytes = Vec::with_capacity(1 + filter.len() + 8);

        bytes.push(self.family.into());
        bytes.extend_from_slice(&filter);
        bytes.extend_from_slice(&self.page.to_be_bytes());

        bytes.into_boxed_slice()
    }

    fn decode(bytes: &[u8]) -> QueryResult<Self> {
        if bytes.len() < 10 {
            return Err(QueryError::Decode("invalid bitmap key"));
        }

        Ok(Self {
            family: Family::try_from(bytes[0])
                .map_err(|_| QueryError::Decode("invalid family tag"))?,
            filter: BitmapFilterKey {
                value: BitmapFilterValue::decode(&bytes[1..bytes.len() - 8])?,
            },
            page: u64::from_be_bytes(bytes[bytes.len() - 8..].try_into().unwrap()),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitmapIndexValue {
    pub id_start: PrimaryId,
    pub id_end: PrimaryId,
    pub bitmap: RoaringBitmap,
}

impl BitmapIndexValue {
    pub const PAGE_SPAN: u64 = 64 * 1024;
}

impl TableValue for BitmapIndexValue {
    fn encode(&self) -> Box<[u8]> {
        let mut bytes = Vec::with_capacity(16);

        bytes.extend_from_slice(&self.id_start.as_u64().to_be_bytes());
        bytes.extend_from_slice(&self.id_end.as_u64().to_be_bytes());
        self.bitmap
            .serialize_into(&mut bytes)
            .expect("vec writes cannot fail");

        bytes.into_boxed_slice()
    }

    fn decode(bytes: Bytes) -> QueryResult<Self> {
        if bytes.len() < 16 {
            return Err(QueryError::Decode("invalid bitmap index value"));
        }

        use std::io::Cursor;

        let mut cursor = Cursor::new(&bytes[16..]);

        let bitmap = RoaringBitmap::deserialize_from(&mut cursor)
            .map_err(|_| QueryError::Decode("invalid bitmap payload"))?;

        if cursor.position() != (bytes.len() - 16) as u64 {
            return Err(QueryError::Decode("trailing bitmap payload"));
        }

        Ok(Self {
            id_start: PrimaryId::new(u64::from_be_bytes(bytes[..8].try_into().unwrap())),
            id_end: PrimaryId::new(u64::from_be_bytes(bytes[8..16].try_into().unwrap())),
            bitmap,
        })
    }
}
