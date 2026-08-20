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

use alloy_primitives::B256;
use alloy_rlp::{RlpDecodable, RlpEncodable};

use crate::{ids::PrimaryId, refs::BlockRef, QueryError, QueryResult};

/// Persisted primary-ID range for one record family within a block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct BlockFamilyRangeMetadata {
    pub first_primary_id: PrimaryId,
    pub count: u32,
}

impl BlockFamilyRangeMetadata {
    pub fn next_primary_id_exclusive(self) -> QueryResult<PrimaryId> {
        self.first_primary_id.checked_add(u64::from(self.count))
    }
}

/// Persisted block identity, family ranges, and row-chain digest.
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct BlockMetadata {
    pub block_number: u64,
    pub block_hash: B256,
    pub parent_hash: B256,
    pub logs: BlockFamilyRangeMetadata,
    pub txs: BlockFamilyRangeMetadata,
    pub traces: BlockFamilyRangeMetadata,
    pub row_chain: B256,
}

impl BlockMetadata {
    pub fn encode(&self) -> Vec<u8> {
        alloy_rlp::encode(self)
    }

    pub fn decode(bytes: &[u8]) -> QueryResult<Self> {
        alloy_rlp::decode_exact(bytes).map_err(|_| QueryError::Decode("invalid block metadata rlp"))
    }
    pub fn block_ref(&self) -> BlockRef {
        BlockRef {
            number: self.block_number,
            hash: self.block_hash,
            parent_hash: self.parent_hash,
        }
    }
}
