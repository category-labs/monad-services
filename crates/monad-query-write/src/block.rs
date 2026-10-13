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
use monad_query_schema::family::{
    StoredBlockQueryItem, StoredLogQueryItem, StoredTraceQueryItem, StoredTxQueryItem,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryWriterBlock {
    pub header: StoredBlockQueryItem,
    pub txs: Box<[QueryWriterTx]>,
}

impl QueryWriterBlock {
    pub fn block_number(&self) -> u64 {
        self.header.header.number
    }

    pub fn block_hash(&self) -> B256 {
        self.header.header.hash_slow()
    }

    pub fn parent_hash(&self) -> B256 {
        self.header.header.parent_hash
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryWriterTx {
    pub envelope: StoredTxQueryItem,
    pub logs: Box<[StoredLogQueryItem]>,
    pub traces: Box<[StoredTraceQueryItem]>,
}
