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

use eyre::{Result, WrapErr};
use monad_query_schema::tables::QueryTables;
use monad_query_store::{
    blob::{
        fs::FsBlobStore, in_memory::InMemoryBlobStore, rocksdb::RocksDbBlobStore, s3::S3BlobStore,
        BlobStoreBackend, RawBlobStoreBackend,
    },
    meta::{
        in_memory::InMemoryMetaStore,
        sqlite::{SqliteMetaStore, SqliteMetaStoreConfig},
        MetaStoreBackend,
    },
};
use monad_query_types::store::blob::table::CodecBlobTable;

use self::config::{
    MonadQueryServiceBlobStorageConfig, MonadQueryServiceMetadataStorageConfig,
    MonadQueryServiceStorageConfig,
};

pub(crate) mod config;

pub(crate) type Tables = QueryTables<
    CodecBlobTable<BlobStoreBackend>,
    MetaStoreBackend,
    MetaStoreBackend,
    CodecBlobTable<BlobStoreBackend>,
    MetaStoreBackend,
>;

pub(crate) async fn open_tables(
    storage: MonadQueryServiceStorageConfig,
    read_only: bool,
) -> Result<Tables> {
    let MonadQueryServiceStorageConfig { metadata, blob } = storage;

    let metadata = match metadata {
        MonadQueryServiceMetadataStorageConfig::InMemory => {
            MetaStoreBackend::InMemory(InMemoryMetaStore::default())
        }
        MonadQueryServiceMetadataStorageConfig::Sqlite {
            url,
            max_connections,
        } => MetaStoreBackend::Sqlite(
            SqliteMetaStore::new_with_config(SqliteMetaStoreConfig {
                url,
                max_connections,
            })
            .await
            .map_err(|error| eyre::eyre!("open metadata store: {error}"))?,
        ),
    };

    let blob = match blob {
        MonadQueryServiceBlobStorageConfig::InMemory => {
            BlobStoreBackend::Raw(RawBlobStoreBackend::InMemory(InMemoryBlobStore::default()))
        }
        MonadQueryServiceBlobStorageConfig::Fs { root } => {
            BlobStoreBackend::Raw(RawBlobStoreBackend::Fs(FsBlobStore::new(root)))
        }
        MonadQueryServiceBlobStorageConfig::RocksDb { root } => {
            BlobStoreBackend::Raw(RawBlobStoreBackend::RocksDb(
                if read_only {
                    RocksDbBlobStore::new_read_only(root)
                } else {
                    RocksDbBlobStore::new(root)
                }
                .map_err(|error| eyre::eyre!("open RocksDB blob store: {error}"))?,
            ))
        }
        MonadQueryServiceBlobStorageConfig::S3 { config } => {
            BlobStoreBackend::Raw(RawBlobStoreBackend::S3(
                S3BlobStore::new(config)
                    .await
                    .map_err(|error| eyre::eyre!("open S3 blob store: {error}"))?,
            ))
        }
    };

    Ok(QueryTables::new(
        || blob.clone(),
        || metadata.clone(),
        || metadata.clone(),
        || blob.clone(),
        || metadata.clone(),
        || metadata.clone(),
    ))
}

pub(crate) fn with_tracing<F, T>(run: F) -> Result<T>
where
    F: FnOnce() -> Result<T>,
{
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    run()
}

pub(crate) fn with_async_runtime<F, Fut, T>(run: F) -> Result<T>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .build()
        .wrap_err("build async runtime")?;

    runtime.block_on(run())
}
