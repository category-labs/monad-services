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

use alloy_consensus::{Transaction, TxEnvelope};
use alloy_eips::Decodable2718;
use alloy_primitives::{Address, Bytes, B256};
use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_query_types::{QueryError, QueryResult};

use crate::{bitmap::BitmapFilterValue, family::BitmapIndexable};

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct StoredTxQueryItem {
    pub tx_hash: B256,
    pub sender: Address,
    pub signed_tx_bytes: Bytes,
}

impl BitmapIndexable for StoredTxQueryItem {
    fn bitmap_filters(&self) -> QueryResult<Box<[BitmapFilterValue]>> {
        let envelope = TxEnvelope::decode_2718(&mut &self.signed_tx_bytes[..])
            .map_err(|_| QueryError::Decode("invalid signed tx envelope"))?;

        let mut filters = vec![BitmapFilterValue::From(self.sender)];

        if let Some(to) = envelope.to() {
            filters.push(BitmapFilterValue::To(to));
        }

        if let Some(selector) = envelope.function_selector().map(|selector| selector.0) {
            filters.push(BitmapFilterValue::Selector(selector));
        }

        Ok(filters.into_boxed_slice())
    }
}
