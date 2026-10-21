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

use std::{num::NonZeroU32, sync::Arc};

use bytes::Bytes;
use governor::{
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
    Quota, RateLimiter,
};
use monad_query_types::{
    store::blob::{BlobStore, BlobStoreBatchPut, BlobTableId},
    QueryResult,
};

type DirectRateLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

pub struct RateLimitedBlobStore<B> {
    inner: B,
    read_limiter: Option<Arc<DirectRateLimiter>>,
    write_limiter: Option<Arc<DirectRateLimiter>>,
}

impl<B> RateLimitedBlobStore<B> {
    pub fn new(
        inner: B,
        reads_per_second: Option<NonZeroU32>,
        writes_per_second: Option<NonZeroU32>,
    ) -> Self {
        Self {
            inner,
            read_limiter: reads_per_second
                .map(|rate| Arc::new(RateLimiter::direct(Quota::per_second(rate)))),
            write_limiter: writes_per_second
                .map(|rate| Arc::new(RateLimiter::direct(Quota::per_second(rate)))),
        }
    }

    async fn wait(limiter: Option<&Arc<DirectRateLimiter>>) {
        if let Some(limiter) = limiter {
            limiter.until_ready().await;
        }
    }
}

impl<B: BlobStore> BlobStore for RateLimitedBlobStore<B> {
    async fn get(&self, table: BlobTableId, key: &[u8]) -> QueryResult<Option<Bytes>> {
        Self::wait(self.read_limiter.as_ref()).await;
        self.inner.get(table, key).await
    }

    async fn get_range(
        &self,
        table: BlobTableId,
        key: &[u8],
        start: usize,
        end_exclusive: usize,
    ) -> QueryResult<Option<Bytes>> {
        Self::wait(self.read_limiter.as_ref()).await;
        self.inner.get_range(table, key, start, end_exclusive).await
    }

    async fn put(&self, table: BlobTableId, key: &[u8], value: Bytes) -> QueryResult<()> {
        Self::wait(self.write_limiter.as_ref()).await;
        self.inner.put(table, key, value).await
    }

    async fn put_batch(&self, writes: Box<[BlobStoreBatchPut]>) -> QueryResult<()> {
        Self::wait(self.write_limiter.as_ref()).await;
        self.inner.put_batch(writes).await
    }
}
