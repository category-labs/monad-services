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

use eyre::{Result, WrapErr};
use monad_query_store::blob::s3::S3BlobStoreConfig;
use serde::Deserialize;
use validator::Validate;

#[derive(Debug, Deserialize, Validate)]
pub struct MonadQueryServiceConfig {
    pub storage: MonadQueryServiceStorageConfig,

    pub start_block: Option<u64>,

    #[validate(range(min = 1))]
    pub poll_ms: u64,

    #[validate(range(min = 1))]
    pub fetch_concurrency: usize,

    #[validate(range(min = 1))]
    pub fetch_batch_size: usize,

    #[validate(range(min = 1))]
    pub flush_every_blocks: usize,
}

impl MonadQueryServiceConfig {
    pub async fn load(path: &std::path::Path) -> Result<Self> {
        let config_str = tokio::fs::read_to_string(path)
            .await
            .wrap_err_with(|| format!("failed to read config {}", path.display()))?;

        let config: MonadQueryServiceConfig = toml::from_str(&config_str)
            .wrap_err_with(|| format!("failed to parse config {}", path.display()))?;

        config.validate()?;

        Ok(config)
    }
}

#[derive(Debug, Deserialize)]
pub struct MonadQueryServiceStorageConfig {
    pub metadata: MonadQueryServiceMetadataStorageConfig,

    pub blob: MonadQueryServiceBlobStorageConfig,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "backend", rename_all = "snake_case")]
pub enum MonadQueryServiceMetadataStorageConfig {
    InMemory,
    Sqlite { url: String, max_connections: u32 },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "backend", rename_all = "snake_case")]
pub enum MonadQueryServiceBlobStorageConfig {
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
