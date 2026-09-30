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

use alloy_primitives::{Address, Bytes, B256};
use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_query_types::QueryResult;

use crate::{bitmap::BitmapFilterValue, family::BitmapIndexable};

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct StoredLogQueryItem {
    pub tx_index: u32,
    pub log_index: u32,
    pub address: Address,
    pub topics: Vec<B256>,
    pub data: Bytes,
}

impl BitmapIndexable for StoredLogQueryItem {
    fn bitmap_filters(&self) -> QueryResult<Box<[BitmapFilterValue]>> {
        let mut filters = vec![BitmapFilterValue::Address(self.address)];

        for (index, topic) in self.topics.iter().take(4).enumerate() {
            filters.push(match index {
                0 => BitmapFilterValue::Topic0(*topic),
                1 => BitmapFilterValue::Topic1(*topic),
                2 => BitmapFilterValue::Topic2(*topic),
                _ => BitmapFilterValue::Topic3(*topic),
            });
        }

        Ok(filters.into_boxed_slice())
    }
}
