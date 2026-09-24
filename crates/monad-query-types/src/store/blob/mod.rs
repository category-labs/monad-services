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

use std::future::Future;

use bytes::Bytes;

use crate::{QueryError, QueryResult};

pub mod partition;
pub mod table;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlobTableId(&'static str);

impl BlobTableId {
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

#[derive(Debug, Clone)]
pub struct BlobStoreBatchPut {
    pub table: BlobTableId,
    pub key: Box<[u8]>,
    pub value: Bytes,
}

#[allow(async_fn_in_trait)]
pub trait BlobStore: Send + Sync + 'static {
    fn get(
        &self,
        table: BlobTableId,
        key: &[u8],
    ) -> impl Future<Output = QueryResult<Option<Bytes>>> + Send;

    fn get_range(
        &self,
        table: BlobTableId,
        key: &[u8],
        start: usize,
        end_exclusive: usize,
    ) -> impl Future<Output = QueryResult<Option<Bytes>>> + Send {
        async move {
            if start > end_exclusive {
                return Err(QueryError::Decode("invalid blob range"));
            }

            let Some(blob) = self.get(table, key).await? else {
                return Ok(None);
            };

            if end_exclusive > blob.len() {
                return Err(QueryError::Decode("invalid blob range"));
            }

            Ok(Some(blob.slice(start..end_exclusive)))
        }
    }

    fn put(
        &self,
        table: BlobTableId,
        key: &[u8],
        value: Bytes,
    ) -> impl Future<Output = QueryResult<()>> + Send;

    fn put_batch(
        &self,
        batch: Box<[BlobStoreBatchPut]>,
    ) -> impl Future<Output = QueryResult<()>> + Send;
}
