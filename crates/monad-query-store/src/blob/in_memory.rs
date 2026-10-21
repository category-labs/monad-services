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
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
};

use bytes::Bytes;
use monad_query_types::{
    store::blob::{BlobStore, BlobStoreBatchPut, BlobTableId},
    QueryResult,
};

type BlobMap = BTreeMap<(BlobTableId, Vec<u8>), Bytes>;
type SharedBlobMap = Arc<RwLock<BlobMap>>;

#[derive(Debug, Clone, Default)]
pub struct InMemoryBlobStore {
    blob_store_map: SharedBlobMap,

    read_call_count: Arc<AtomicU64>,
}

impl InMemoryBlobStore {
    pub fn get_read_call_count(&self) -> u64 {
        self.read_call_count.load(Ordering::SeqCst)
    }

    pub fn blob_snapshot(&self) -> BTreeMap<(BlobTableId, Vec<u8>), Bytes> {
        self.blob_store_map
            .read()
            .expect("in-memory blob store lock is not poisoned")
            .clone()
    }
}

impl BlobStore for InMemoryBlobStore {
    async fn get(&self, table: BlobTableId, key: &[u8]) -> QueryResult<Option<Bytes>> {
        self.read_call_count.fetch_add(1, Ordering::SeqCst);

        let blob_store = self
            .blob_store_map
            .read()
            .expect("in-memory blob store lock is not poisoned");

        Ok(blob_store.get(&(table, key.to_vec())).cloned())
    }

    async fn put(&self, table: BlobTableId, key: &[u8], value: Bytes) -> QueryResult<()> {
        let mut blob_store = self
            .blob_store_map
            .write()
            .expect("in-memory blob store lock is not poisoned");

        blob_store.insert((table, key.to_vec()), value);

        Ok(())
    }

    async fn put_batch(&self, writes: Box<[BlobStoreBatchPut]>) -> QueryResult<()> {
        let mut blob_store = self
            .blob_store_map
            .write()
            .expect("in-memory blob store lock is not poisoned");

        for BlobStoreBatchPut { table, key, value } in writes {
            blob_store.insert((table, key.into_vec()), value);
        }

        Ok(())
    }
}
