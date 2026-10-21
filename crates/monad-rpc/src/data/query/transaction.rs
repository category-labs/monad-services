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
use alloy_primitives::{normalize_v, Address, Bytes, FixedBytes, B256, U256, U64};
use monad_query_read::{TxQueryFilter, TxQueryItem as ReadTxQueryItem};
use serde::{Deserialize, Serialize};

use super::{FilterValues, QueryFieldSelection};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TxQueryField {
    AccessList,
    AuthorizationList,
    BlobVersionedHashes,
    BlockHash,
    BlockNumber,
    BlockTimestamp,
    ChainId,
    ContractAddress,
    CumulativeGasUsed,
    EffectiveGasPrice,
    From,
    Gas,
    GasPrice,
    GasUsed,
    Hash,
    Input,
    LogsBloom,
    MaxFeePerBlobGas,
    MaxFeePerGas,
    MaxPriorityFeePerGas,
    Nonce,
    R,
    S,
    Status,
    To,
    TransactionIndex,
    Type,
    V,
    Value,
    YParity,
}

#[serde_with::skip_serializing_none]
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TxQueryItem {
    pub access_list: Option<AccessList>,
    pub authorization_list: Option<Vec<SignedAuthorization>>,
    pub blob_versioned_hashes: Option<Vec<B256>>,
    pub block_hash: Option<B256>,
    pub block_number: Option<U64>,
    pub block_timestamp: Option<U64>,
    pub chain_id: Option<U64>,
    pub contract_address: Option<Option<Address>>,
    pub cumulative_gas_used: Option<U64>,
    pub effective_gas_price: Option<U256>,
    pub from: Option<Address>,
    pub gas: Option<U64>,
    pub gas_price: Option<Option<U256>>,
    pub gas_used: Option<U64>,
    pub hash: Option<B256>,
    pub input: Option<Bytes>,
    pub logs_bloom: Option<alloy_primitives::Bloom>,
    pub max_fee_per_blob_gas: Option<U256>,
    pub max_fee_per_gas: Option<U256>,
    pub max_priority_fee_per_gas: Option<U256>,
    pub nonce: Option<U64>,
    pub r: Option<U256>,
    pub s: Option<U256>,
    pub status: Option<U64>,
    pub to: Option<Address>,
    pub transaction_index: Option<U64>,
    #[serde(rename = "type")]
    pub typ: Option<U64>,
    pub v: Option<U64>,
    pub value: Option<U256>,
    pub y_parity: Option<U64>,
}

impl TxQueryItem {
    pub fn new(item: ReadTxQueryItem, fields: &QueryFieldSelection) -> Self {
        TxQueryItem {
            access_list: fields
                .transactions_selected(TxQueryField::AccessList)
                .then(|| item.access_list)
                .flatten(),
            authorization_list: fields
                .transactions_selected(TxQueryField::AuthorizationList)
                .then(|| item.authorization_list)
                .flatten(),
            blob_versioned_hashes: fields
                .transactions_selected(TxQueryField::BlobVersionedHashes)
                .then(|| item.blob_versioned_hashes)
                .flatten(),
            block_hash: fields
                .transactions_selected(TxQueryField::BlockHash)
                .then_some(item.block_hash),
            block_number: fields
                .transactions_selected(TxQueryField::BlockNumber)
                .then(|| U64::from(item.block_number)),
            block_timestamp: fields
                .transactions_selected(TxQueryField::BlockTimestamp)
                .then(|| U64::from(item.block_timestamp)),
            chain_id: fields
                .transactions_selected(TxQueryField::ChainId)
                .then(|| item.chain_id.map(U64::from))
                .flatten(),
            contract_address: fields
                .transactions_selected(TxQueryField::ContractAddress)
                .then_some(item.contract_address),
            cumulative_gas_used: fields
                .transactions_selected(TxQueryField::CumulativeGasUsed)
                .then(|| U64::from(item.cumulative_gas_used)),
            effective_gas_price: fields
                .transactions_selected(TxQueryField::EffectiveGasPrice)
                .then_some(item.effective_gas_price),
            from: fields
                .transactions_selected(TxQueryField::From)
                .then_some(item.sender),
            gas: fields
                .transactions_selected(TxQueryField::Gas)
                .then(|| U64::from(item.gas)),
            gas_price: fields
                .transactions_selected(TxQueryField::GasPrice)
                .then_some(item.gas_price),
            gas_used: fields
                .transactions_selected(TxQueryField::GasUsed)
                .then(|| U64::from(item.gas_used)),
            hash: fields
                .transactions_selected(TxQueryField::Hash)
                .then_some(item.transaction_hash),
            input: fields
                .transactions_selected(TxQueryField::Input)
                .then_some(item.input),
            logs_bloom: fields
                .transactions_selected(TxQueryField::LogsBloom)
                .then_some(item.logs_bloom),
            max_fee_per_blob_gas: fields
                .transactions_selected(TxQueryField::MaxFeePerBlobGas)
                .then_some(item.max_fee_per_blob_gas)
                .flatten(),
            max_fee_per_gas: fields
                .transactions_selected(TxQueryField::MaxFeePerGas)
                .then_some(item.max_fee_per_gas)
                .flatten(),
            max_priority_fee_per_gas: fields
                .transactions_selected(TxQueryField::MaxPriorityFeePerGas)
                .then_some(item.max_priority_fee_per_gas)
                .flatten(),
            nonce: fields
                .transactions_selected(TxQueryField::Nonce)
                .then(|| U64::from(item.nonce)),
            r: fields
                .transactions_selected(TxQueryField::R)
                .then_some(item.r),
            s: fields
                .transactions_selected(TxQueryField::S)
                .then_some(item.s),
            status: fields
                .transactions_selected(TxQueryField::Status)
                .then(|| U64::from(item.status as u64)),
            to: fields
                .transactions_selected(TxQueryField::To)
                .then(|| item.to)
                .flatten(),
            transaction_index: fields
                .transactions_selected(TxQueryField::TransactionIndex)
                .then(|| U64::from(item.transaction_index)),
            typ: fields
                .transactions_selected(TxQueryField::Type)
                .then(|| U64::from(item.tx_type)),
            v: fields
                .transactions_selected(TxQueryField::V)
                .then(|| U64::from(item.v.to::<u64>())),
            value: fields
                .transactions_selected(TxQueryField::Value)
                .then_some(item.value),
            y_parity: fields
                .transactions_selected(TxQueryField::YParity)
                .then(|| {
                    U64::from(
                        normalize_v(item.v.to::<u64>())
                            .expect("stored transaction has invalid signature v")
                            as u64,
                    )
                }),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, default)]
pub struct TxFilter {
    pub from: Option<FilterValues<Address>>,
    pub to: Option<FilterValues<Address>>,
    pub selector: Option<FilterValues<FixedBytes<4>>>,
}

impl TxFilter {
    pub fn into_query_filter(self) -> TxQueryFilter {
        TxQueryFilter {
            from: self.from.map(|value| value.0),
            to: self.to.map(|value| value.0),
            selector: self
                .selector
                .map(|value| value.0.into_iter().map(|selector| selector.0).collect()),
        }
    }
}
