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
    QueryError, QueryResult,
};

use crate::family::Family;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PayloadKey {
    pub family: Family,
    pub key: Box<[u8]>,
}

impl PayloadKey {
    const ITEMS_PER_BLOB: u64 = 1024;

    pub fn from_id(family: Family, id: u64) -> Self {
        Self {
            family,
            key: (id / Self::ITEMS_PER_BLOB).to_be_bytes().into(),
        }
    }

    pub fn partition_start(id: u64) -> u64 {
        id / Self::ITEMS_PER_BLOB * Self::ITEMS_PER_BLOB
    }

    pub fn partition_number(id: u64) -> u64 {
        id / Self::ITEMS_PER_BLOB
    }
}

impl TableKey for PayloadKey {
    fn encode(&self) -> Box<[u8]> {
        let mut bytes = Vec::with_capacity(5 + self.key.len());

        bytes.push(self.family.into());
        bytes.extend_from_slice(&(self.key.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&self.key);

        bytes.into_boxed_slice()
    }

    fn decode(bytes: &[u8]) -> QueryResult<Self> {
        if bytes.len() < 5 {
            return Err(QueryError::Decode("invalid payload key"));
        }

        let key_len = u32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize;

        if bytes.len() != 5 + key_len {
            return Err(QueryError::Decode("invalid payload key length"));
        }

        Ok(Self {
            family: Family::try_from(bytes[0])
                .map_err(|_| QueryError::Decode("invalid family tag"))?,
            key: bytes[5..].to_vec().into_boxed_slice(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PayloadItem {
    pub bytes: Bytes,
}

impl TableValue for PayloadItem {
    fn encode(&self) -> Box<[u8]> {
        zstd::bulk::compress(&self.bytes, 3)
            .expect("zstd compression")
            .into_boxed_slice()
    }

    fn decode(bytes: Bytes) -> QueryResult<Self> {
        const ZSTD_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];
        let bytes = if bytes.starts_with(&ZSTD_MAGIC) {
            zstd::bulk::decompress(&bytes, 16 * 1024 * 1024)
                .map(Bytes::from)
                .map_err(|_| QueryError::Decode("invalid compressed payload"))?
        } else {
            bytes
        };
        Ok(Self { bytes })
    }
}
