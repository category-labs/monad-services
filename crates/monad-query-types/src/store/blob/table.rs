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

use std::{future::Future, marker::PhantomData};

use bytes::Bytes;

use super::{BlobStore, BlobStoreBatchPut, BlobTableId};
use crate::QueryResult;

pub trait TableKey: Sized + Send + Sync + 'static {
    fn encode(&self) -> Box<[u8]>;
    fn decode(bytes: &[u8]) -> QueryResult<Self>;
}

pub trait TableValue: Sized + Send + Sync + 'static {
    fn encode(&self) -> Box<[u8]>;
    fn decode(bytes: Bytes) -> QueryResult<Self>;
}

impl TableValue for Bytes {
    fn encode(&self) -> Box<[u8]> {
        self.to_vec().into_boxed_slice()
    }

    fn decode(bytes: Bytes) -> QueryResult<Self> {
        Ok(bytes)
    }
}

#[derive(Debug, Clone)]
pub struct BlobTablePutBatch<K, V> {
    pub key: K,
    pub value: V,
}

pub trait BlobTable: Send + Sync + 'static {
    type Config;

    fn new(table: BlobTableId, config: Self::Config) -> Self;

    fn get<K, V>(&self, key: &K) -> impl Future<Output = QueryResult<Option<V>>> + Send
    where
        K: TableKey,
        V: TableValue;

    fn get_range<K, V>(
        &self,
        key: &K,
        start: usize,
        end_exclusive: usize,
    ) -> impl Future<Output = QueryResult<Option<V>>> + Send
    where
        K: TableKey,
        V: TableValue;

    fn put<K, V>(&self, key: &K, value: V) -> impl Future<Output = QueryResult<()>> + Send
    where
        K: TableKey,
        V: TableValue;

    fn put_batch<K, V>(
        &self,
        batch: Box<[BlobTablePutBatch<K, V>]>,
    ) -> impl Future<Output = QueryResult<()>> + Send
    where
        K: TableKey,
        V: TableValue;
}

pub struct CodecBlobTable<B> {
    table: BlobTableId,
    blob_store: B,
}

impl<B> BlobTable for CodecBlobTable<B>
where
    B: BlobStore,
{
    type Config = B;

    fn new(table: BlobTableId, config: Self::Config) -> Self {
        Self {
            table,
            blob_store: config,
        }
    }

    async fn get<K, V>(&self, key: &K) -> QueryResult<Option<V>>
    where
        K: TableKey,
        V: TableValue,
    {
        self.blob_store
            .get(self.table, &key.encode())
            .await?
            .map(V::decode)
            .transpose()
    }

    async fn get_range<K, V>(
        &self,
        key: &K,
        start: usize,
        end_exclusive: usize,
    ) -> QueryResult<Option<V>>
    where
        K: TableKey,
        V: TableValue,
    {
        self.blob_store
            .get_range(self.table, &key.encode(), start, end_exclusive)
            .await?
            .map(V::decode)
            .transpose()
    }

    async fn put<K, V>(&self, key: &K, value: V) -> QueryResult<()>
    where
        K: TableKey,
        V: TableValue,
    {
        self.blob_store
            .put(self.table, &key.encode(), Bytes::from(value.encode()))
            .await
    }

    async fn put_batch<K, V>(&self, batch: Box<[BlobTablePutBatch<K, V>]>) -> QueryResult<()>
    where
        K: TableKey,
        V: TableValue,
    {
        let batch = batch
            .into_vec()
            .into_iter()
            .map(|entry| BlobStoreBatchPut {
                table: self.table,
                key: entry.key.encode(),
                value: entry.value.encode().into(),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();

        self.blob_store.put_batch(batch).await
    }
}

pub struct SchemaBlobTable<T, K, V>
where
    T: BlobTable,
    K: TableKey,
    V: TableValue,
{
    table: T,
    _types: PhantomData<(K, V)>,
}

impl<T, K, V> SchemaBlobTable<T, K, V>
where
    T: BlobTable,
    K: TableKey,
    V: TableValue,
{
    pub fn new(table: T) -> Self {
        Self {
            table,
            _types: PhantomData,
        }
    }

    pub async fn get(&self, key: &K) -> QueryResult<Option<V>> {
        self.table.get(key).await
    }

    pub async fn put(&self, key: &K, value: V) -> QueryResult<()> {
        self.table.put(key, value).await
    }

    pub async fn put_batch(&self, batch: Box<[BlobTablePutBatch<K, V>]>) -> QueryResult<()> {
        self.table.put_batch(batch).await
    }
}
