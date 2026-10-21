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

use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use bytes::Bytes;
use monad_query_types::{
    store::blob::{BlobStore, BlobStoreBatchPut, BlobTableId},
    QueryError, QueryResult,
};
use rocksdb::{Options, WriteBatch, DB};

const CATCH_UP_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Clone)]
pub struct RocksDbBlobStore {
    db: Arc<DB>,
    last_catch_up: Option<Arc<Mutex<Instant>>>,
}

impl RocksDbBlobStore {
    pub fn new(path: impl AsRef<Path>) -> QueryResult<Self> {
        let mut options = Options::default();

        options.create_if_missing(true);

        DB::open(&options, path)
            .map(|db| Self {
                db: Arc::new(db),
                last_catch_up: None,
            })
            .map_err(|error| QueryError::Backend(format!("open RocksDB blob store: {error}")))
    }

    pub fn new_read_only(path: impl AsRef<Path>) -> QueryResult<Self> {
        let options = Options::default();

        DB::open_for_read_only(&options, path, false)
            .map(|db| Self {
                db: Arc::new(db),
                last_catch_up: None,
            })
            .map_err(|error| {
                QueryError::Backend(format!("open RocksDB blob store read-only: {error}"))
            })
    }

    pub fn new_secondary(path: impl AsRef<Path>) -> QueryResult<Self> {
        let path = path.as_ref();

        let secondary_path = path.with_extension(format!("secondary-{}", std::process::id()));

        let options = Options::default();

        DB::open_as_secondary(&options, path, &secondary_path)
            .map(|db| Self {
                db: Arc::new(db),
                last_catch_up: Some(Arc::new(Mutex::new(Instant::now() - CATCH_UP_INTERVAL))),
            })
            .map_err(|error| {
                QueryError::Backend(format!("open RocksDB blob store as secondary: {error}"))
            })
    }

    fn key(table: BlobTableId, key: &[u8]) -> Vec<u8> {
        let name = table.as_str().as_bytes();

        let mut result = Vec::with_capacity(name.len() + 1 + key.len());

        result.extend_from_slice(name);
        result.push(0);
        result.extend_from_slice(key);

        result
    }
}

impl BlobStore for RocksDbBlobStore {
    async fn get(&self, table: BlobTableId, key: &[u8]) -> QueryResult<Option<Bytes>> {
        let db = Arc::clone(&self.db);

        let last_catch_up = self.last_catch_up.clone();

        let key = Self::key(table, key);

        tokio::task::spawn_blocking(move || {
            if let Some(last_catch_up) = last_catch_up {
                let mut last = last_catch_up
                    .lock()
                    .map_err(|_| QueryError::Backend("RocksDB catch-up lock poisoned".into()))?;
                // ponytail: at most 100ms visibility lag; shorten only if RPC freshness requires it.
                if last.elapsed() >= CATCH_UP_INTERVAL {
                    db.try_catch_up_with_primary().map_err(|error| {
                        QueryError::Backend(format!("catch up RocksDB secondary: {error}"))
                    })?;
                    *last = Instant::now();
                }
            }
            db.get(key)
                .map(|value| value.map(Bytes::from))
                .map_err(|error| QueryError::Backend(format!("read RocksDB blob: {error}")))
        })
        .await
        .map_err(|error| QueryError::Backend(format!("join RocksDB read task: {error}")))?
    }

    async fn put(&self, table: BlobTableId, key: &[u8], value: Bytes) -> QueryResult<()> {
        self.put_batch(Box::new([BlobStoreBatchPut {
            table,
            key: key.to_vec().into_boxed_slice(),
            value,
        }]))
        .await
    }

    async fn put_batch(&self, writes: Box<[BlobStoreBatchPut]>) -> QueryResult<()> {
        let db = Arc::clone(&self.db);

        tokio::task::spawn_blocking(move || {
            let mut batch = WriteBatch::default();

            for write in writes {
                batch.put(Self::key(write.table, &write.key), write.value);
            }

            db.write(batch)
                .map_err(|error| QueryError::Backend(format!("write RocksDB blobs: {error}")))
        })
        .await
        .map_err(|error| QueryError::Backend(format!("join RocksDB write task: {error}")))?
    }
}
