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

use std::{future::Future, num::NonZeroU32, sync::Arc};

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

#[derive(Clone)]
pub struct RateLimitBlobStore<B>
where
    B: BlobStore,
{
    inner: B,
    read_limiter: Option<Arc<DirectRateLimiter>>,
    write_limiter: Option<Arc<DirectRateLimiter>>,
}

impl<B> RateLimitBlobStore<B>
where
    B: BlobStore,
{
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

    async fn wait_until_ready(limiter: Option<&Arc<DirectRateLimiter>>, permits: u32) {
        if let Some(limiter) = limiter {
            if let Some(permits) = NonZeroU32::new(permits) {
                let _ = limiter.until_n_ready(permits).await;
            }
        }
    }
}

#[allow(refining_impl_trait)]
impl<B> BlobStore for RateLimitBlobStore<B>
where
    B: BlobStore,
{
    fn get(
        &self,
        table: BlobTableId,
        key: &[u8],
    ) -> impl Future<Output = QueryResult<Option<Bytes>>> + Send + '_ {
        let key = key.to_vec().into_boxed_slice();
        async move {
            Self::wait_until_ready(self.read_limiter.as_ref(), 1).await;
            Box::pin(self.inner.get(table, &key)).await
        }
    }

    fn get_range(
        &self,
        table: BlobTableId,
        key: &[u8],
        start: usize,
        end_exclusive: usize,
    ) -> impl Future<Output = QueryResult<Option<Bytes>>> + Send + '_ {
        let key = key.to_vec().into_boxed_slice();
        async move {
            Self::wait_until_ready(self.read_limiter.as_ref(), 1).await;
            Box::pin(self.inner.get_range(table, &key, start, end_exclusive)).await
        }
    }

    fn put(
        &self,
        table: BlobTableId,
        key: &[u8],
        value: Bytes,
    ) -> impl Future<Output = QueryResult<()>> + Send + '_ {
        let key = key.to_vec().into_boxed_slice();
        async move {
            Self::wait_until_ready(self.write_limiter.as_ref(), 1).await;
            Box::pin(self.inner.put(table, &key, value)).await
        }
    }

    #[allow(clippy::manual_async_fn)]
    fn put_batch(
        &self,
        writes: Box<[BlobStoreBatchPut]>,
    ) -> impl Future<Output = QueryResult<()>> + Send + '_ {
        async move {
            Self::wait_until_ready(
                self.write_limiter.as_ref(),
                writes.len().try_into().unwrap(),
            )
            .await;
            Box::pin(self.inner.put_batch(writes)).await
        }
    }
}
