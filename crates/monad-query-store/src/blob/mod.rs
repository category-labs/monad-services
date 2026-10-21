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
    store::blob::{BlobStore, BlobStoreBatchPut, BlobTableId},
    QueryResult,
};

#[cfg(feature = "rate-limit")]
use self::rate_limit::RateLimitBlobStore;
#[cfg(feature = "rocksdb")]
use self::rocksdb::RocksDbBlobStore;
#[cfg(feature = "s3")]
use self::s3::S3BlobStore;
use self::{fs::FsBlobStore, in_memory::InMemoryBlobStore};

pub mod fs;
pub mod in_memory;
#[cfg(feature = "rate-limit")]
pub mod rate_limit;
#[cfg(feature = "rocksdb")]
pub mod rocksdb;
#[cfg(feature = "s3")]
pub mod s3;

#[derive(Clone)]
pub enum RawBlobStoreBackend {
    Fs(FsBlobStore),
    InMemory(InMemoryBlobStore),
    #[cfg(feature = "rocksdb")]
    RocksDb(RocksDbBlobStore),
    #[cfg(feature = "s3")]
    S3(S3BlobStore),
}

macro_rules! with_raw_blob_backend {
    ($backend:expr, $store:ident => $operation:expr) => {
        match $backend {
            RawBlobStoreBackend::Fs($store) => $operation,
            RawBlobStoreBackend::InMemory($store) => $operation,
            #[cfg(feature = "rocksdb")]
            RawBlobStoreBackend::RocksDb($store) => $operation,
            #[cfg(feature = "s3")]
            RawBlobStoreBackend::S3($store) => $operation,
        }
    };
}

impl BlobStore for RawBlobStoreBackend {
    async fn get(&self, table: BlobTableId, key: &[u8]) -> QueryResult<Option<Bytes>> {
        with_raw_blob_backend!(self, backend => backend.get(table, key).await)
    }

    async fn get_range(
        &self,
        table: BlobTableId,
        key: &[u8],
        start: usize,
        end_exclusive: usize,
    ) -> QueryResult<Option<Bytes>> {
        with_raw_blob_backend!(self, backend => {
            backend.get_range(table, key, start, end_exclusive).await
        })
    }

    async fn put(&self, table: BlobTableId, key: &[u8], value: Bytes) -> QueryResult<()> {
        with_raw_blob_backend!(self, backend => backend.put(table, key, value).await)
    }

    async fn put_batch(&self, writes: Box<[BlobStoreBatchPut]>) -> QueryResult<()> {
        with_raw_blob_backend!(self, backend => backend.put_batch(writes).await)
    }
}

#[derive(Clone)]
pub enum BlobStoreBackend {
    Raw(RawBlobStoreBackend),
    #[cfg(feature = "rate-limit")]
    RateLimit(RateLimitBlobStore<Box<Self>>),
}

macro_rules! with_blob_backend {
    ($backend:expr, $store:ident => $operation:expr) => {
        match $backend {
            BlobStoreBackend::Raw($store) => $operation,
            #[cfg(feature = "rate-limit")]
            BlobStoreBackend::RateLimit($store) => $operation,
        }
    };
}

impl BlobStore for BlobStoreBackend {
    async fn get(&self, table: BlobTableId, key: &[u8]) -> QueryResult<Option<Bytes>> {
        with_blob_backend!(self, backend => backend.get(table, key).await)
    }

    async fn get_range(
        &self,
        table: BlobTableId,
        key: &[u8],
        start: usize,
        end_exclusive: usize,
    ) -> QueryResult<Option<Bytes>> {
        with_blob_backend!(self, backend => {
            backend.get_range(table, key, start, end_exclusive).await
        })
    }

    async fn put(&self, table: BlobTableId, key: &[u8], value: Bytes) -> QueryResult<()> {
        with_blob_backend!(self, backend => backend.put(table, key, value).await)
    }

    async fn put_batch(&self, writes: Box<[BlobStoreBatchPut]>) -> QueryResult<()> {
        with_blob_backend!(self, backend => backend.put_batch(writes).await)
    }
}
