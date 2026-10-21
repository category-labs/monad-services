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
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use bytes::Bytes;
use monad_query_types::{
    store::blob::{BlobStore, BlobStoreBatchPut, BlobTableId},
    QueryError, QueryResult,
};

#[derive(Debug, Clone)]
pub struct FsBlobStore {
    root: PathBuf,
}

impl FsBlobStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn path(&self, table: BlobTableId, key: &[u8]) -> PathBuf {
        self.root.join(table.as_str()).join(hex(key))
    }
}

impl BlobStore for FsBlobStore {
    async fn get(&self, table: BlobTableId, key: &[u8]) -> QueryResult<Option<Bytes>> {
        let started = Instant::now();

        let path = self.path(table, key);

        let result = match tokio::fs::read(&path).await {
            Ok(value) => Ok(Some(Bytes::from(value))),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(QueryError::Backend(format!("read blob: {error}"))),
        };

        tracing::debug!(
            target = "query_store::blob",
            table = table.as_str(),
            key_len = key.len(),
            path = %path.display(),
            found = result.as_ref().is_ok_and(Option::is_some),
            bytes = result.as_ref().ok().and_then(Option::as_ref).map_or(0, Bytes::len),
            elapsed_us = started.elapsed().as_micros() as u64,
            "blob get",
        );

        result
    }

    async fn put(&self, table: BlobTableId, key: &[u8], value: Bytes) -> QueryResult<()> {
        let path = self.path(table, key);

        tokio::fs::create_dir_all(path.parent().expect("blob path has a parent"))
            .await
            .map_err(|error| QueryError::Backend(format!("create blob directory: {error}")))?;

        let temp_file = create_temporary_file(&path).await?;

        let result = async {
            tokio::fs::write(&temp_file, value)
                .await
                .map_err(|error| QueryError::Backend(format!("write blob: {error}")))?;

            tokio::fs::rename(&temp_file, &path)
                .await
                .map_err(|error| QueryError::Backend(format!("publish blob: {error}")))
        }
        .await;

        if result.is_err() {
            // Best effort: preserve the original error if cleanup also fails.
            let _ = tokio::fs::remove_file(&temp_file).await;
        }

        result
    }

    async fn put_batch(&self, batch: Box<[BlobStoreBatchPut]>) -> QueryResult<()> {
        for BlobStoreBatchPut { table, key, value } in batch {
            self.put(table, &key, value).await?;
        }

        Ok(())
    }
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";

    let mut result = String::with_capacity(bytes.len() * 2);

    for byte in bytes {
        result.push(DIGITS[(byte >> 4) as usize] as char);
        result.push(DIGITS[(byte & 0xf) as usize] as char);
    }

    result
}

async fn create_temporary_file(path: &std::path::Path) -> QueryResult<PathBuf> {
    static NEXT_TEMPORARY: AtomicU64 = AtomicU64::new(0);

    loop {
        let id = NEXT_TEMPORARY.fetch_add(1, Ordering::Relaxed);

        let temporary = path.with_file_name(format!(
            ".{}.tmp-{}-{id}",
            path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("blob"),
            std::process::id(),
        ));

        match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .await
        {
            Ok(_) => return Ok(temporary),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(QueryError::Backend(format!(
                    "create temporary blob: {error}"
                )))
            }
        }
    }
}
