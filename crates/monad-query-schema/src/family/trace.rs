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

use alloy_primitives::{Address, Bytes, B256, U256};
use alloy_rlp::{Decodable, Encodable, RlpDecodable, RlpEncodable};
use monad_query_types::{CallKind, QueryResult};

use crate::{bitmap::BitmapFilterValue, family::BitmapIndexable};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredTraceQueryItem {
    pub transaction_hash: B256,
    pub transaction_index: u32,
    pub trace_address: Box<[u32]>,
    pub r#type: CallKind,
    pub from: Address,
    pub to: Option<Address>,
    pub value: U256,
    pub gas: u64,
    pub gas_used: u64,
    pub input: Bytes,
    pub output: Bytes,
    pub status: u8,
    pub reverted: bool,
}

#[derive(Debug, RlpEncodable, RlpDecodable)]
struct StoredTraceQueryItemRlp {
    transaction_hash: B256,
    transaction_index: u32,
    trace_address: Vec<u32>,
    type_bytes: u8,
    from: Address,
    to_bytes: Bytes,
    value: U256,
    gas: u64,
    gas_used: u64,
    input: Bytes,
    output: Bytes,
    status: u8,
    reverted: bool,
}

impl Encodable for StoredTraceQueryItem {
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        let to_bytes = self
            .to
            .as_ref()
            .map(|addr| Bytes::copy_from_slice(addr.as_slice()))
            .unwrap_or_default();

        StoredTraceQueryItemRlp {
            transaction_hash: self.transaction_hash,
            transaction_index: self.transaction_index,
            trace_address: self.trace_address.to_vec(),
            type_bytes: self.r#type.into(),
            from: self.from,
            to_bytes,
            value: self.value,
            gas: self.gas,
            gas_used: self.gas_used,
            input: self.input.clone(),
            output: self.output.clone(),
            status: self.status,
            reverted: self.reverted,
        }
        .encode(out);
    }
}

impl Decodable for StoredTraceQueryItem {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let StoredTraceQueryItemRlp {
            transaction_hash,
            transaction_index,
            trace_address,
            type_bytes,
            from,
            to_bytes,
            value,
            gas,
            gas_used,
            input,
            output,
            status,
            reverted,
        } = StoredTraceQueryItemRlp::decode(buf)?;

        let r#type = CallKind::try_from(type_bytes)
            .map_err(|_| alloy_rlp::Error::Custom("invalid trace call kind byte"))?;

        let to = (!to_bytes.is_empty())
            .then(|| Address::try_from(to_bytes.as_ref()))
            .transpose()
            .map_err(|_| alloy_rlp::Error::Custom("invalid trace `to` byte length"))?;

        Ok(Self {
            transaction_hash,
            transaction_index,
            trace_address: trace_address.into_boxed_slice(),
            r#type,
            from,
            to,
            value,
            gas,
            gas_used,
            input,
            output,
            status,
            reverted,
        })
    }
}

impl BitmapIndexable for StoredTraceQueryItem {
    fn bitmap_filters(&self) -> QueryResult<Box<[BitmapFilterValue]>> {
        let Self {
            transaction_hash: _,
            transaction_index: _,
            trace_address,
            r#type,
            from,
            to,
            value: _,
            gas: _,
            gas_used: _,
            input,
            output: _,
            status: _,
            reverted,
        } = self;

        let mut filters = vec![BitmapFilterValue::From(*from)];

        if let Some(to) = to {
            filters.push(BitmapFilterValue::To(*to));
        }

        if let Ok(selector) = <[u8; 4]>::try_from(&input[..4.min(input.len())]) {
            filters.push(BitmapFilterValue::Selector(selector));
        }

        if trace_address.is_empty() {
            filters.push(BitmapFilterValue::TopLevel);
        }

        if !reverted
            && matches!(
                r#type,
                CallKind::Call | CallKind::Create | CallKind::Create2 | CallKind::SelfDestruct
            )
        {
            filters.push(BitmapFilterValue::HasTransfer);
        }

        Ok(filters.into_boxed_slice())
    }
}
