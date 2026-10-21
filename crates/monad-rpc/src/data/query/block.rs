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
use alloy_primitives::{Address, Bytes, B256, U256, U64};
use monad_query_read::{
    BlockQueryFilter as ReadBlockQueryFilter, BlockQueryItem as ReadBlockQueryItem,
};
use serde::{Deserialize, Serialize};

use super::{FilterValues, QueryFieldSelection};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum BlockQueryField {
    BaseFeePerGas,
    BlobGasUsed,
    Difficulty,
    ExcessBlobGas,
    ExtraData,
    GasLimit,
    GasUsed,
    Hash,
    LogsBloom,
    Miner,
    MixHash,
    Nonce,
    Number,
    ParentBeaconBlockRoot,
    ParentHash,
    ReceiptsRoot,
    RequestsHash,
    Sha3Uncles,
    Size,
    StateRoot,
    Timestamp,
    TotalDifficulty,
    TransactionsRoot,
    WithdrawalsRoot,
}

#[serde_with::skip_serializing_none]
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockQueryItem {
    pub base_fee_per_gas: Option<U64>,
    pub blob_gas_used: Option<U64>,
    pub difficulty: Option<U256>,
    pub excess_blob_gas: Option<U64>,
    pub extra_data: Option<Bytes>,
    pub gas_limit: Option<U64>,
    pub gas_used: Option<U64>,
    pub hash: Option<B256>,
    pub logs_bloom: Option<alloy_primitives::Bloom>,
    pub miner: Option<Address>,
    pub mix_hash: Option<B256>,
    pub nonce: Option<alloy_primitives::B64>,
    pub number: Option<U64>,
    pub parent_beacon_block_root: Option<B256>,
    pub parent_hash: Option<B256>,
    pub receipts_root: Option<B256>,
    pub requests_hash: Option<B256>,
    pub sha3_uncles: Option<B256>,
    pub size: Option<U64>,
    pub state_root: Option<B256>,
    pub timestamp: Option<U64>,
    pub total_difficulty: Option<U256>,
    pub transactions_root: Option<B256>,
    pub withdrawals_root: Option<B256>,
}

impl BlockQueryItem {
    pub fn new(item: ReadBlockQueryItem, fields: &QueryFieldSelection) -> Self {
        let ReadBlockQueryItem {
            hash,
            header:
                Header {
                    parent_hash,
                    ommers_hash,
                    beneficiary,
                    state_root,
                    transactions_root,
                    receipts_root,
                    logs_bloom,
                    difficulty,
                    number,
                    gas_limit,
                    gas_used,
                    timestamp,
                    extra_data,
                    mix_hash,
                    nonce,
                    base_fee_per_gas,
                    withdrawals_root,
                    blob_gas_used,
                    excess_blob_gas,
                    parent_beacon_block_root,
                    requests_hash,
                    block_access_list_hash: _block_access_list_hash,
                    slot_number: _slot_number,
                },
            size,
            total_difficulty,
        } = item;

        BlockQueryItem {
            base_fee_per_gas: fields
                .blocks_selected(BlockQueryField::BaseFeePerGas)
                .then(|| base_fee_per_gas.map(U64::from))
                .flatten(),
            blob_gas_used: fields
                .blocks_selected(BlockQueryField::BlobGasUsed)
                .then(|| blob_gas_used.map(U64::from))
                .flatten(),
            difficulty: fields
                .blocks_selected(BlockQueryField::Difficulty)
                .then_some(difficulty),
            excess_blob_gas: fields
                .blocks_selected(BlockQueryField::ExcessBlobGas)
                .then(|| excess_blob_gas.map(U64::from))
                .flatten(),
            extra_data: fields
                .blocks_selected(BlockQueryField::ExtraData)
                .then_some(extra_data),
            gas_limit: fields
                .blocks_selected(BlockQueryField::GasLimit)
                .then(|| U64::from(gas_limit)),
            gas_used: fields
                .blocks_selected(BlockQueryField::GasUsed)
                .then(|| U64::from(gas_used)),
            hash: fields
                .blocks_selected(BlockQueryField::Hash)
                .then_some(hash),
            logs_bloom: fields
                .blocks_selected(BlockQueryField::LogsBloom)
                .then_some(logs_bloom),
            miner: fields
                .blocks_selected(BlockQueryField::Miner)
                .then_some(beneficiary),
            mix_hash: fields
                .blocks_selected(BlockQueryField::MixHash)
                .then_some(mix_hash),
            nonce: fields
                .blocks_selected(BlockQueryField::Nonce)
                .then_some(nonce),
            number: fields
                .blocks_selected(BlockQueryField::Number)
                .then(|| U64::from(number)),
            parent_beacon_block_root: fields
                .blocks_selected(BlockQueryField::ParentBeaconBlockRoot)
                .then_some(parent_beacon_block_root)
                .flatten(),
            parent_hash: fields
                .blocks_selected(BlockQueryField::ParentHash)
                .then_some(parent_hash),
            receipts_root: fields
                .blocks_selected(BlockQueryField::ReceiptsRoot)
                .then_some(receipts_root),
            requests_hash: fields
                .blocks_selected(BlockQueryField::RequestsHash)
                .then_some(requests_hash)
                .flatten(),
            sha3_uncles: fields
                .blocks_selected(BlockQueryField::Sha3Uncles)
                .then_some(ommers_hash),
            size: fields
                .blocks_selected(BlockQueryField::Size)
                .then(|| U64::from(size)),
            state_root: fields
                .blocks_selected(BlockQueryField::StateRoot)
                .then_some(state_root),
            timestamp: fields
                .blocks_selected(BlockQueryField::Timestamp)
                .then(|| U64::from(timestamp)),
            total_difficulty: fields
                .blocks_selected(BlockQueryField::TotalDifficulty)
                .then_some(total_difficulty),
            transactions_root: fields
                .blocks_selected(BlockQueryField::TransactionsRoot)
                .then_some(transactions_root),
            withdrawals_root: fields
                .blocks_selected(BlockQueryField::WithdrawalsRoot)
                .then_some(withdrawals_root)
                .flatten(),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, default)]
pub struct BlockFilter {
    pub miner: Option<FilterValues<Address>>,
}

impl BlockFilter {
    pub fn into_query_filter(self) -> ReadBlockQueryFilter {
        ReadBlockQueryFilter {
            miner: self.miner.map(|values| values.0.into_iter().collect()),
        }
    }
}
