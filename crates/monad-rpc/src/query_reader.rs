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

use std::{num::NonZeroUsize, sync::Arc};

use monad_query_read::{
    BlockQueryRequest, BlockRangeLimit, QueryLimits, QueryLogsRequest, QueryPage, QueryReader,
    QueryReaderConfig, QueryTransactionsRequest, TraceQueryRequest, TransferQueryRequest,
};
use monad_query_schema::tables::QueryTables;
use monad_query_store::{
    blob::{
        fs::FsBlobStore,
        in_memory::InMemoryBlobStore,
        rocksdb::RocksDbBlobStore,
        s3::{S3BlobStore, S3BlobStoreConfig},
        BlobStoreBackend,
    },
    meta::{
        in_memory::InMemoryMetaStore,
        scylla::{ScyllaMetaStore, ScyllaMetaStoreConfig},
        sqlite::{SqliteMetaStore, SqliteMetaStoreConfig},
        MetaStoreBackend,
    },
};
use monad_query_types::{store::blob::table::CodecBlobTable, QueryResult};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct ReaderOpenConfig {
    pub storage: ReaderStorageConfig,
    #[serde(default = "default_max_target")]
    pub max_target: NonZeroUsize,
    #[serde(default = "default_max_block_range")]
    pub max_block_range: u64,
    #[serde(default = "default_candidate_batch_load_max_concurrency")]
    pub candidate_batch_load_max_concurrency: NonZeroUsize,
    #[serde(default = "default_candidate_batch_max_size")]
    pub candidate_batch_max_size: NonZeroUsize,
}

#[derive(Debug, Deserialize)]
pub struct ReaderStorageConfig {
    pub metadata: ReaderMetadataConfig,
    pub blob: ReaderBlobConfig,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "backend", rename_all = "snake_case")]
pub enum ReaderMetadataConfig {
    InMemory,
    Sqlite {
        url: String,
        #[serde(default = "default_max_connections")]
        max_connections: u32,
    },
    Scylla {
        #[serde(flatten)]
        config: ScyllaMetaStoreConfig,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "backend", rename_all = "snake_case")]
pub enum ReaderBlobConfig {
    InMemory,
    Fs {
        root: String,
    },
    RocksDb {
        root: String,
    },
    S3 {
        #[serde(flatten)]
        config: S3BlobStoreConfig,
    },
}

type BlobTable = CodecBlobTable<BlobStoreBackend>;
type Tables =
    QueryTables<BlobTable, MetaStoreBackend, MetaStoreBackend, BlobTable, MetaStoreBackend>;
type Reader =
    QueryReader<BlobTable, MetaStoreBackend, MetaStoreBackend, BlobTable, MetaStoreBackend>;

#[derive(Clone)]
pub struct ConfiguredQueryReader {
    reader: Arc<Reader>,
}

impl ConfiguredQueryReader {
    pub async fn open(config: ReaderOpenConfig) -> QueryResult<Self> {
        let metadata = match config.storage.metadata {
            ReaderMetadataConfig::InMemory => {
                MetaStoreBackend::InMemory(InMemoryMetaStore::default())
            }
            ReaderMetadataConfig::Sqlite {
                url,
                max_connections,
            } => MetaStoreBackend::Sqlite(
                SqliteMetaStore::new_with_config(SqliteMetaStoreConfig {
                    url,
                    max_connections,
                })
                .await?,
            ),
            ReaderMetadataConfig::Scylla { config } => {
                MetaStoreBackend::Scylla(ScyllaMetaStore::new(config).await?)
            }
        };
        let blob = match config.storage.blob {
            ReaderBlobConfig::InMemory => BlobStoreBackend::InMemory(InMemoryBlobStore::default()),
            ReaderBlobConfig::Fs { root } => BlobStoreBackend::Fs(FsBlobStore::new(root)),
            ReaderBlobConfig::RocksDb { root } => {
                BlobStoreBackend::RocksDb(RocksDbBlobStore::new_secondary(root)?)
            }
            ReaderBlobConfig::S3 { config } => {
                BlobStoreBackend::S3(S3BlobStore::new(config).await?)
            }
        };
        let tables: Tables = QueryTables::new(
            || blob.clone(),
            || metadata.clone(),
            || metadata.clone(),
            || blob.clone(),
            || metadata.clone(),
            || metadata.clone(),
        );
        tables.register().await?;
        let reader = QueryReader::new(
            QueryReaderConfig {
                limits: QueryLimits::new(
                    config.max_target,
                    BlockRangeLimit::from_u64(config.max_block_range),
                ),
                candidate_batch_load_max_concurrency: config.candidate_batch_load_max_concurrency,
                candidate_batch_max_size: config.candidate_batch_max_size,
            },
            tables,
        );
        Ok(Self {
            reader: Arc::new(reader),
        })
    }

    pub async fn query_blocks(&self, request: BlockQueryRequest) -> QueryResult<QueryPage> {
        self.reader.query_blocks(request).await
    }

    pub async fn query_logs(&self, request: QueryLogsRequest) -> QueryResult<QueryPage> {
        self.reader.query_logs(request).await
    }

    pub async fn query_transactions(
        &self,
        request: QueryTransactionsRequest,
    ) -> QueryResult<QueryPage> {
        self.reader.query_transactions(request).await
    }

    pub async fn query_traces(&self, request: TraceQueryRequest) -> QueryResult<QueryPage> {
        self.reader.query_traces(request).await
    }

    pub async fn query_transfers(&self, request: TransferQueryRequest) -> QueryResult<QueryPage> {
        self.reader.query_transfers(request).await
    }
}

fn default_candidate_batch_load_max_concurrency() -> NonZeroUsize {
    NonZeroUsize::new(4).expect("constant is non-zero")
}

fn default_candidate_batch_max_size() -> NonZeroUsize {
    NonZeroUsize::new(64).expect("constant is non-zero")
}

fn default_max_target() -> NonZeroUsize {
    NonZeroUsize::new(1_000).expect("constant is non-zero")
}

fn default_max_block_range() -> u64 {
    10_000
}

fn default_max_connections() -> u32 {
    16
}
