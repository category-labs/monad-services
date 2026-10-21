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
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    ops::Range,
    sync::Arc,
    time::Duration,
};

use aws_config::{timeout::TimeoutConfig, BehaviorVersion, Region};
use aws_sdk_s3::{
    config::Credentials,
    error::{ProvideErrorMetadata, SdkError},
    operation::get_object::GetObjectError,
    primitives::ByteStream,
    types::{BucketLocationConstraint, CreateBucketConfiguration},
    Client,
};
use bytes::Bytes;
use futures::stream::{StreamExt, TryStreamExt};
use monad_query_types::{
    store::blob::{BlobStore, BlobStoreBatchPut, BlobTableId},
    QueryError, QueryResult,
};
use serde::Deserialize;
use tracing::{debug, info};

const OPERATION_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);

const OPERATION_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, Deserialize)]
pub struct S3BlobStoreConfig {
    pub bucket: String,
    pub root_prefix: String,
    pub endpoint_urls: Vec<String>,
    pub region: Option<String>,
    pub profile: Option<String>,
    pub force_path_style: bool,
    pub max_concurrency: usize,
    pub create_bucket: bool,
    pub credentials: Option<S3BlobStoreCredentials>,
}

impl S3BlobStoreConfig {
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            root_prefix: String::new(),
            endpoint_urls: Vec::new(),
            region: None,
            profile: None,
            force_path_style: false,
            max_concurrency: 32,
            create_bucket: false,
            credentials: None,
        }
    }
}

#[derive(Deserialize)]
pub struct S3BlobStoreCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

impl std::fmt::Debug for S3BlobStoreCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Credentials")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

#[derive(Clone)]
pub struct S3BlobStore {
    inner: Arc<S3BlobStoreInner>,
}

impl std::fmt::Debug for S3BlobStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3BlobStore")
            .field("bucket", &self.inner.bucket)
            .field("key_encoder", &self.inner.key_encoder)
            .field("max_concurrency", &self.inner.max_concurrency)
            .field("endpoint_count", &self.inner.clients.len())
            .finish_non_exhaustive()
    }
}

impl S3BlobStore {
    pub async fn new(config: S3BlobStoreConfig) -> QueryResult<Self> {
        let S3BlobStoreConfig {
            bucket,
            root_prefix,
            endpoint_urls,
            region,
            profile,
            force_path_style,
            max_concurrency,
            create_bucket,
            credentials,
        } = config;

        let endpoint_is_aws = endpoint_urls.is_empty();

        let mut sdk_loader = aws_config::defaults(BehaviorVersion::latest()).timeout_config(
            TimeoutConfig::builder()
                .operation_attempt_timeout(OPERATION_ATTEMPT_TIMEOUT)
                .operation_timeout(OPERATION_TIMEOUT)
                .build(),
        );
        if let Some(region) = region {
            sdk_loader = sdk_loader.region(Region::new(region));
        }
        if let Some(profile) = profile {
            sdk_loader = sdk_loader.profile_name(profile);
        }
        if let Some(credentials) = credentials {
            sdk_loader = sdk_loader.credentials_provider(Credentials::new(
                credentials.access_key_id,
                credentials.secret_access_key,
                credentials.session_token,
                None,
                "S3BlobStoreConfig",
            ));
        }
        let sdk_config = sdk_loader.load().await;
        let region_for_create = sdk_config.region().map(|r| r.as_ref().to_string());

        let client_endpoints = if endpoint_urls.is_empty() {
            vec![None]
        } else {
            endpoint_urls
                .iter()
                .map(|endpoint| Some(endpoint.as_str()))
                .collect()
        };
        let clients = client_endpoints
            .into_iter()
            .map(|endpoint| {
                let mut s3_builder = aws_sdk_s3::config::Builder::from(&sdk_config);
                if let Some(endpoint) = endpoint {
                    s3_builder = s3_builder.endpoint_url(endpoint);
                }
                if force_path_style {
                    s3_builder = s3_builder.force_path_style(true);
                }
                Client::from_conf(s3_builder.build())
            })
            .collect::<Box<[_]>>();

        if create_bucket {
            let mut request = clients[0].create_bucket().bucket(&bucket);
            if endpoint_is_aws {
                let region = region_for_create.as_deref().unwrap_or("us-east-1");
                if region != "us-east-1" {
                    let configuration = CreateBucketConfiguration::builder()
                        .location_constraint(BucketLocationConstraint::from(region))
                        .build();
                    request = request.create_bucket_configuration(configuration);
                }
            }
            match request.send().await {
                Ok(_) => {}
                Err(error) if error.code() == Some("BucketAlreadyOwnedByYou") => {}
                Err(error) => return Err(backend_err("create_bucket", &bucket, error)),
            }
        }

        futures::stream::iter(clients.clone())
            .map(|client| {
                let bucket = bucket.clone();
                async move {
                    client
                        .head_bucket()
                        .bucket(&bucket)
                        .send()
                        .await
                        .map_err(|e| backend_err("head_bucket", &bucket, e))?;
                    Ok(())
                }
            })
            .buffer_unordered(clients.len())
            .try_collect::<()>()
            .await?;

        Ok(Self {
            inner: Arc::new(S3BlobStoreInner {
                clients,
                bucket,
                key_encoder: S3KeyEncoder::new(root_prefix),
                max_concurrency: max_concurrency.max(1),
                endpoint_urls: endpoint_urls.into_boxed_slice(),
            }),
        })
    }
}

impl BlobStore for S3BlobStore {
    async fn get(&self, table: BlobTableId, key: &[u8]) -> QueryResult<Option<Bytes>> {
        self.inner
            .get_object_at(&self.inner.key_encoder.encode(table, key), None)
            .await
    }

    async fn get_range(
        &self,
        table: BlobTableId,
        key: &[u8],
        start: usize,
        end_exclusive: usize,
    ) -> QueryResult<Option<Bytes>> {
        let range = Range {
            start,
            end: end_exclusive,
        };

        if range.is_empty() {
            return Err(QueryError::Decode("invalid blob range"));
        }

        self.inner
            .get_object_at(&self.inner.key_encoder.encode(table, key), Some(range))
            .await
    }

    async fn put(&self, table: BlobTableId, key: &[u8], blob_data: Bytes) -> QueryResult<()> {
        let object_key = self.inner.key_encoder.encode(table, key);
        let (client, endpoint_url) = self.inner.client_for_object_key(&object_key);
        let blob_data_length = blob_data.len();
        let started = std::time::Instant::now();
        client
            .put_object()
            .bucket(&self.inner.bucket)
            .key(&object_key)
            .body(ByteStream::from(blob_data))
            .send()
            .await
            .map_err(|e| backend_err("put_object", &object_key, e))?;

        debug!(
            table = %table.as_str(),
            object_key = %object_key,
            endpoint_url,
            blob_data_length,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "s3 put_object completed"
        );
        Ok(())
    }

    async fn put_batch(&self, writes: Box<[BlobStoreBatchPut]>) -> QueryResult<()> {
        if writes.is_empty() {
            return Ok(());
        }
        let started = std::time::Instant::now();
        let write_count = writes.len();
        let total_bytes = writes.iter().map(|op| op.value.len()).sum::<usize>();
        let concurrency = self.inner.max_concurrency;
        info!(
            write_count,
            total_bytes, concurrency, "s3 put_batch starting PUT batch"
        );
        futures::stream::iter(writes.into_vec().into_iter().map(|op| {
            let blob_store = self.clone();
            async move { blob_store.put(op.table, &op.key, op.value).await }
        }))
        .buffer_unordered(concurrency)
        .try_collect::<()>()
        .await?;
        info!(
            write_count,
            total_bytes,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "s3 put_batch completed PUT batch"
        );
        Ok(())
    }
}

struct S3BlobStoreInner {
    clients: Box<[Client]>,
    bucket: String,
    key_encoder: S3KeyEncoder,
    max_concurrency: usize,
    endpoint_urls: Box<[String]>,
}

impl S3BlobStoreInner {
    fn client_for_object_key(&self, object_key: &str) -> (&Client, Option<&str>) {
        let client_index = if self.clients.len() == 1 {
            0
        } else {
            let mut hasher = DefaultHasher::new();
            object_key.hash(&mut hasher);
            (hasher.finish() as usize) % self.clients.len()
        };

        (
            &self.clients[client_index],
            self.endpoint_urls.get(client_index).map(String::as_str),
        )
    }

    async fn get_object_at(
        &self,
        object_key: &str,
        range: Option<Range<usize>>,
    ) -> QueryResult<Option<Bytes>> {
        let (client, _) = self.client_for_object_key(object_key);

        let expected_range_length = range.as_ref().map(std::ops::Range::len);
        let mut req = client.get_object().bucket(&self.bucket).key(object_key);

        if let Some(range) = range {
            req = req.range(format!("bytes={}-{}", range.start, range.end - 1));
        }

        let resp = match req.send().await {
            Ok(resp) => resp,
            Err(e) => {
                return match e.as_service_error() {
                    Some(GetObjectError::NoSuchKey(_)) => Ok(None),
                    _ if e.code() == Some("InvalidRange") => {
                        Err(QueryError::Decode("invalid blob range"))
                    }
                    _ => Err(backend_err("get_object", object_key, e)),
                };
            }
        };

        let collected =
            resp.body.collect().await.map_err(|e| {
                QueryError::Backend(format!("s3 get_object body {object_key}: {e}"))
            })?;
        let bytes = collected.into_bytes();

        if expected_range_length.is_some_and(|expected| expected != bytes.len()) {
            return Err(QueryError::Decode("invalid blob range"));
        }

        Ok(Some(bytes))
    }
}

#[derive(Debug)]
struct S3KeyEncoder {
    root_prefix: String,
}

impl S3KeyEncoder {
    fn new(root_prefix: impl Into<String>) -> Self {
        Self {
            root_prefix: root_prefix.into().trim_matches('/').to_owned(),
        }
    }

    fn encode(&self, table: BlobTableId, key: &[u8]) -> String {
        let table = table.as_str();
        let mut object_key =
            String::with_capacity(self.root_prefix.len() + 1 + table.len() + 1 + key.len() * 2 + 1);

        if !self.root_prefix.is_empty() {
            object_key.push_str(&self.root_prefix);
            object_key.push('/');
        }
        object_key.push_str(table);
        object_key.push('/');

        const HEX: &[u8; 16] = b"0123456789abcdef";
        for &byte in key {
            object_key.push(HEX[(byte >> 4) as usize] as char);
            object_key.push(HEX[(byte & 0x0f) as usize] as char);
        }
        object_key
    }
}

fn backend_err<E, R>(op: &str, object_key: &str, e: SdkError<E, R>) -> QueryError
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
    R: std::fmt::Debug,
{
    let detail = match e.code() {
        Some(code) => format!("{code}: {}", e.message().unwrap_or("")),
        None => e.to_string(),
    };
    QueryError::Backend(format!("s3 {op} {object_key}: {detail}"))
}
