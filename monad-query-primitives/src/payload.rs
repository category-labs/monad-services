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

use alloy_rlp::{RlpDecodable, RlpEncodable};
use bytes::Bytes;

use crate::{QueryError, QueryResult};

/// Native per-row zstd frames in a chain-data-owned blob object.
pub const ENCODING_NATIVE: u8 = 0;

/// Archive containers v1 inside an existing monad-archive object.
pub const ENCODING_EXTERNAL_V1: u8 = 1;

/// Byte offsets and container metadata for one family's block payload.
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct BlockPayloadManifest {
    pub offsets: Vec<u32>,
    pub dict_version: u32,
    pub base_offset: u32,
    pub physical_key: Vec<u8>,
    pub physical_base_offset: u64,
    pub encoding: u8,
    pub container_rows: Vec<u32>,
    pub container_status: Bytes,
}

impl BlockPayloadManifest {
    pub fn is_external(&self) -> bool {
        self.encoding != ENCODING_NATIVE
    }

    pub fn row_count(&self) -> usize {
        self.container_rows
            .last()
            .copied()
            .map_or(self.offsets.len().saturating_sub(1), |v| v as usize)
    }
    pub fn container_count(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    /// Maps a row index to its container and ordinal within that container.
    pub fn container_of_row(&self, idx: usize) -> (usize, usize) {
        if self.container_rows.is_empty() {
            return (idx, 0);
        }
        let container = self
            .container_rows
            .partition_point(|&cum| cum as usize <= idx);
        (container, idx - self.container_row_base(container))
    }

    pub fn container_row_base(&self, container: usize) -> usize {
        if self.container_rows.is_empty() {
            return container;
        }
        container
            .checked_sub(1)
            .and_then(|i| self.container_rows.get(i))
            .copied()
            .unwrap_or(0) as usize
    }

    pub fn container_row_len(&self, container: usize) -> usize {
        if self.container_rows.is_empty() {
            return 1;
        }
        (self.container_rows.get(container).copied().unwrap_or(0) as usize)
            .saturating_sub(self.container_row_base(container))
    }

    fn manifest_is_consistent(&self) -> bool {
        if !self.is_external() {
            return self.container_rows.is_empty() && self.container_status.is_empty();
        }
        let containers = self.container_count();
        (!(!self.container_rows.is_empty()
            && (self.container_rows.len() != containers
                || self.container_rows.windows(2).any(|w| w[0] > w[1]))))
            && self.row_count() <= *self.offsets.last().unwrap_or(&0) as usize
            && (self.container_status.is_empty()
                || self.container_status.len() == containers.div_ceil(8))
    }

    pub fn container_status_bit(&self, container: usize) -> bool {
        self.container_status
            .get(container / 8)
            .is_some_and(|byte| byte >> (container % 8) & 1 == 1)
    }

    fn base(&self) -> usize {
        self.physical_base_offset as usize + self.base_offset as usize
    }

    /// Absolute byte range for one offset-delimited payload unit.
    pub fn abs_range(&self, idx: usize) -> (usize, usize) {
        let base = self.base();
        (
            base + self.offsets[idx] as usize,
            base + self.offsets[idx + 1] as usize,
        )
    }
    pub fn region_range(&self) -> (usize, usize) {
        let base = self.base();
        (base, base + *self.offsets.last().unwrap_or(&0) as usize)
    }
    pub fn physical_key_or<'a>(&'a self, default: &'a [u8]) -> &'a [u8] {
        if self.physical_key.is_empty() {
            default
        } else {
            &self.physical_key
        }
    }
    pub fn encode(&self) -> Vec<u8> {
        alloy_rlp::encode(self)
    }

    pub fn decode(bytes: &[u8]) -> QueryResult<Self> {
        let manifest: Self = alloy_rlp::decode_exact(bytes)
            .map_err(|_| QueryError::Decode("invalid block payload manifest rlp"))?;
        if !manifest.manifest_is_consistent() {
            return Err(QueryError::Decode("block payload manifest is inconsistent"));
        }
        Ok(manifest)
    }
}
