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

use std::collections::HashMap;

use alloy_primitives::{Address, Bytes, U256};
use alloy_rlp::{Decodable, Encodable, RlpDecodable, RlpEncodable};
use monad_query_types::{CallKind, QueryResult};

use crate::{bitmap::BitmapFilterValue, family::BitmapIndexable};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredTraceQueryItem {
    pub r#type: CallKind,
    pub from: Address,
    pub to: Option<Address>,
    pub value: U256,
    pub gas: u64,
    pub gas_used: u64,
    pub input: Bytes,
    pub output: Bytes,
    pub status: u8,
    pub depth: u32,
    pub tx_index: u32,
    pub trace_address: Box<[u32]>,
    pub tx_status: bool,
}

#[derive(Debug, RlpEncodable, RlpDecodable)]
struct StoredTraceQueryItemRlp {
    type_bytes: u8,
    from: Address,
    to_bytes: Bytes,
    value: U256,
    gas: u64,
    gas_used: u64,
    input: Bytes,
    output: Bytes,
    status: u8,
    depth: u32,
    tx_index: u32,
    trace_address: Vec<u32>,
    tx_status: bool,
}

impl Encodable for StoredTraceQueryItem {
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        let to_bytes = self
            .to
            .as_ref()
            .map(|addr| Bytes::copy_from_slice(addr.as_slice()))
            .unwrap_or_default();

        StoredTraceQueryItemRlp {
            type_bytes: self.r#type.into(),
            from: self.from,
            to_bytes,
            value: self.value,
            gas: self.gas,
            gas_used: self.gas_used,
            input: self.input.clone(),
            output: self.output.clone(),
            status: self.status,
            depth: self.depth,
            tx_index: self.tx_index,
            trace_address: self.trace_address.to_vec(),
            tx_status: self.tx_status,
        }
        .encode(out);
    }
}

impl Decodable for StoredTraceQueryItem {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let StoredTraceQueryItemRlp {
            type_bytes,
            from,
            to_bytes,
            value,
            gas,
            gas_used,
            input,
            output,
            status,
            depth,
            tx_index,
            trace_address,
            tx_status,
        } = StoredTraceQueryItemRlp::decode(buf)?;

        let r#type = CallKind::try_from(type_bytes)
            .map_err(|_| alloy_rlp::Error::Custom("invalid trace call kind byte"))?;

        let to = (!to_bytes.is_empty())
            .then(|| Address::try_from(to_bytes.as_ref()))
            .transpose()
            .map_err(|_| alloy_rlp::Error::Custom("invalid trace `to` byte length"))?;

        Ok(Self {
            r#type,
            from,
            to,
            value,
            gas,
            gas_used,
            input,
            output,
            status,
            depth,
            tx_index,
            trace_address: trace_address.into_boxed_slice(),
            tx_status,
        })
    }
}

pub struct StoredTraceQueryItemWithAncestorStatus<'a> {
    pub trace: &'a StoredTraceQueryItem,
    pub ancestor_reverted: bool,
}

impl<'a> StoredTraceQueryItemWithAncestorStatus<'a> {
    pub fn from_traces(traces: &'a [StoredTraceQueryItem]) -> Box<[Self]> {
        // Per-tx depth stack: stack[d] = frame at depth d, or an ancestor of it, failed.
        let mut stacks: HashMap<u32, Vec<bool>> = HashMap::new();

        traces
            .iter()
            .map(|trace| {
                let stack = stacks.entry(trace.tx_index).or_default();

                let depth = trace.trace_address.len();

                let ancestor_reverted = depth
                    .checked_sub(1)
                    .and_then(|d| stack.get(d).copied())
                    .unwrap_or(false);

                stack.truncate(depth);

                if stack.len() < depth {
                    stack.resize(depth, false);
                }

                stack.push(ancestor_reverted || trace.status != 0);

                Self {
                    trace,
                    ancestor_reverted,
                }
            })
            .collect()
    }
}

impl BitmapIndexable for StoredTraceQueryItemWithAncestorStatus<'_> {
    fn bitmap_filters(&self) -> QueryResult<Box<[BitmapFilterValue]>> {
        let trace = self.trace;

        let mut filters = vec![BitmapFilterValue::From(trace.from)];

        if let Some(to) = trace.to {
            filters.push(BitmapFilterValue::To(to));
        }

        if let Ok(selector) = <[u8; 4]>::try_from(&trace.input[..4.min(trace.input.len())]) {
            filters.push(BitmapFilterValue::Selector(selector));
        }

        if trace.trace_address.is_empty() {
            filters.push(BitmapFilterValue::TopLevel);
        }

        if !self.ancestor_reverted
            && trace.tx_status
            && trace.status == 0
            && matches!(
                trace.r#type,
                CallKind::Call | CallKind::Create | CallKind::Create2 | CallKind::SelfDestruct
            )
        {
            filters.push(BitmapFilterValue::HasTransfer);
        }

        Ok(filters.into_boxed_slice())
    }
}
