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

use alloy_consensus::Header;
use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_query_types::QueryResult;

use super::BitmapIndexable;
use crate::bitmap::BitmapFilterValue;

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct StoredBlockQueryItem {
    pub header: Header,
    pub size: u64,
    pub total_difficulty: alloy_primitives::U256,
}

impl BitmapIndexable for StoredBlockQueryItem {
    fn bitmap_filters(&self) -> QueryResult<Box<[BitmapFilterValue]>> {
        Ok(Box::new([BitmapFilterValue::Miner(
            self.header.beneficiary,
        )]))
    }
}
