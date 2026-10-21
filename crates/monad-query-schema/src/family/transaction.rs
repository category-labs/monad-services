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

use alloy_eip2930::AccessList;
use alloy_eip7702::SignedAuthorization;
use alloy_primitives::{Address, Bloom, Bytes, B256, U256};
use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_query_types::QueryResult;

use crate::{bitmap::BitmapFilterValue, family::BitmapIndexable};

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
#[rlp(trailing)]
pub struct StoredTxQueryItem {
    pub block_timestamp: u64,

    // Tx Envelope Fields
    pub transaction_hash: B256,
    pub sender: Address,
    pub nonce: u64,
    pub tx_type: u8,
    pub input: Bytes,
    pub value: U256,
    pub gas: u64,
    pub r: U256,
    pub s: U256,
    pub y_parity: bool,
    pub logs_bloom: Bloom,

    // Receipt Fields
    pub status: bool,
    pub gas_used: u64,
    pub cumulative_gas_used: u64,
    pub effective_gas_price: U256,

    // Optional Tx Envelope Fields
    pub chain_id: Option<u64>,
    pub to: Option<Address>,
    pub gas_price: Option<U256>,
    pub max_fee_per_gas: Option<U256>,
    pub max_priority_fee_per_gas: Option<U256>,
    pub max_fee_per_blob_gas: Option<U256>,
    pub access_list: Option<AccessList>,
    pub authorization_list: Option<Vec<SignedAuthorization>>,
    pub blob_versioned_hashes: Option<Vec<B256>>,
    pub contract_address: Option<Address>,
}

impl BitmapIndexable for StoredTxQueryItem {
    fn bitmap_filters(&self) -> QueryResult<Box<[BitmapFilterValue]>> {
        let mut filters = vec![BitmapFilterValue::From(self.sender)];

        if let Some(to) = self.to {
            filters.push(BitmapFilterValue::To(to));
        }

        if self.input.len() >= 4 {
            filters.push(BitmapFilterValue::Selector(
                self.input[..4].try_into().expect("four-byte selector"),
            ));
        }

        Ok(filters.into_boxed_slice())
    }
}
