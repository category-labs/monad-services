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

use alloy_primitives::{Address, Bytes, FixedBytes, B256, U256, U64};
use monad_query_read::{
    StoredTraceQueryItem, TraceQueryItem as ReadTraceQueryItem, TraceQueryRequestFilter,
};
use serde::{Deserialize, Serialize};

use super::{call_kind, FilterValues, QueryFieldSelection, QueryFilter};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TraceQueryField {
    BlockHash,
    BlockNumber,
    Error,
    From,
    Gas,
    GasUsed,
    Input,
    Output,
    Reverted,
    To,
    TraceAddress,
    TransactionHash,
    TransactionIndex,
    Type,
    Value,
}

#[serde_with::skip_serializing_none]
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceQueryItem {
    pub block_hash: Option<B256>,
    pub block_number: Option<U64>,
    pub error: Option<Option<String>>,
    pub from: Option<Address>,
    pub gas: Option<U64>,
    pub gas_used: Option<U64>,
    pub input: Option<Bytes>,
    pub output: Option<Bytes>,
    pub reverted: Option<bool>,
    pub to: Option<Address>,
    pub trace_address: Option<Vec<u32>>,
    pub transaction_hash: Option<B256>,
    pub transaction_index: Option<U64>,
    #[serde(rename = "type")]
    pub typ: Option<&'static str>,
    pub value: Option<U256>,
}

impl TraceQueryItem {
    pub fn new(item: ReadTraceQueryItem, fields: &QueryFieldSelection) -> Self {
        let ReadTraceQueryItem {
            block_number,
            block_hash,
            trace:
                StoredTraceQueryItem {
                    transaction_hash,
                    transaction_index,
                    trace_address,
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
                },
        } = item;

        TraceQueryItem {
            block_hash: fields
                .traces_selected(TraceQueryField::BlockHash)
                .then_some(block_hash),
            block_number: fields
                .traces_selected(TraceQueryField::BlockNumber)
                .then(|| U64::from(block_number)),
            error: fields.traces_selected(TraceQueryField::Error).then(|| {
                (status != 0).then(|| {
                    alloy_sol_types::decode_revert_reason(&output)
                        .unwrap_or_else(|| "execution reverted".to_owned())
                })
            }),
            from: fields
                .traces_selected(TraceQueryField::From)
                .then_some(from),
            gas: fields
                .traces_selected(TraceQueryField::Gas)
                .then(|| U64::from(gas)),
            gas_used: fields
                .traces_selected(TraceQueryField::GasUsed)
                .then(|| U64::from(gas_used)),
            input: fields
                .traces_selected(TraceQueryField::Input)
                .then_some(input),
            output: fields
                .traces_selected(TraceQueryField::Output)
                .then_some(output.clone()),
            reverted: fields
                .traces_selected(TraceQueryField::Reverted)
                .then_some(reverted),
            to: fields
                .traces_selected(TraceQueryField::To)
                .then_some(to)
                .flatten(),
            trace_address: fields
                .traces_selected(TraceQueryField::TraceAddress)
                .then(|| trace_address.into_vec()),
            transaction_hash: fields
                .traces_selected(TraceQueryField::TransactionHash)
                .then_some(transaction_hash),
            transaction_index: fields
                .traces_selected(TraceQueryField::TransactionIndex)
                .then(|| U64::from(transaction_index)),
            typ: fields
                .traces_selected(TraceQueryField::Type)
                .then(|| call_kind(r#type)),
            value: fields
                .traces_selected(TraceQueryField::Value)
                .then_some(value),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, default)]
pub struct TraceFilter {
    pub from: QueryFilter<FilterValues<Address>>,
    pub to: QueryFilter<FilterValues<Address>>,
    pub selector: QueryFilter<FilterValues<FixedBytes<4>>>,
    pub include_reverted: bool,
}

impl TraceFilter {
    pub fn into_query_filter(self) -> TraceQueryRequestFilter {
        TraceQueryRequestFilter {
            from: self.from.0.map(|value| value.0),
            to: self.to.0.map(|value| value.0),
            selector: self
                .selector
                .0
                .map(|value| value.0.into_iter().map(|selector| selector.0).collect()),
            include_reverted: self.include_reverted,
        }
    }
}
