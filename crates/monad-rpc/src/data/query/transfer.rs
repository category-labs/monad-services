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

use alloy_primitives::{Address, Bytes, B256, U256, U64};
use monad_query_read::{TransferQueryFilter, TransferQueryItem as ReadTransferQueryItem};
use serde::{Deserialize, Serialize};

use super::{call_kind, FilterValues, QueryFieldSelection};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TransferQueryField {
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
pub struct TransferQueryItem {
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

impl TransferQueryItem {
    pub fn new(item: ReadTransferQueryItem, fields: &QueryFieldSelection) -> Self {
        let ReadTransferQueryItem {
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            trace_address,
            typ,
            from,
            to,
            value,
            gas,
            gas_used,
            input,
            output,
            status,
            reverted,
        } = item;

        TransferQueryItem {
            block_hash: fields
                .transfers_selected(TransferQueryField::BlockHash)
                .then_some(block_hash),
            block_number: fields
                .transfers_selected(TransferQueryField::BlockNumber)
                .then(|| U64::from(block_number)),
            error: fields
                .transfers_selected(TransferQueryField::Error)
                .then(|| {
                    (status != 0).then(|| {
                        alloy_sol_types::decode_revert_reason(&output)
                            .unwrap_or_else(|| "execution reverted".to_owned())
                    })
                }),
            from: fields
                .transfers_selected(TransferQueryField::From)
                .then_some(from),
            gas: fields
                .transfers_selected(TransferQueryField::Gas)
                .then(|| U64::from(gas)),
            gas_used: fields
                .transfers_selected(TransferQueryField::GasUsed)
                .then(|| U64::from(gas_used)),
            input: fields
                .transfers_selected(TransferQueryField::Input)
                .then_some(input.clone()),
            output: fields
                .transfers_selected(TransferQueryField::Output)
                .then_some(output.clone()),
            reverted: fields
                .transfers_selected(TransferQueryField::Reverted)
                .then_some(reverted),
            to: fields
                .transfers_selected(TransferQueryField::To)
                .then_some(to),
            trace_address: fields
                .transfers_selected(TransferQueryField::TraceAddress)
                .then_some(trace_address.into_vec()),
            transaction_hash: fields
                .transfers_selected(TransferQueryField::TransactionHash)
                .then_some(transaction_hash),
            transaction_index: fields
                .transfers_selected(TransferQueryField::TransactionIndex)
                .then(|| U64::from(transaction_index)),
            typ: fields
                .transfers_selected(TransferQueryField::Type)
                .then(|| call_kind(typ)),
            value: fields
                .transfers_selected(TransferQueryField::Value)
                .then_some(value),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, default)]
pub struct TransferFilter {
    pub from: Option<FilterValues<Address>>,
    pub to: Option<FilterValues<Address>>,
    pub include_reverted: bool,
}

impl TransferFilter {
    pub fn into_query_filter(self) -> TransferQueryFilter {
        TransferQueryFilter {
            from: self.from.map(|value| value.0),
            to: self.to.map(|value| value.0),
            include_reverted: self.include_reverted,
        }
    }
}
