// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::collections::HashSet;

use alloy_eip2930::AccessList;
use alloy_eip7702::SignedAuthorization;
use alloy_primitives::{Address, Bytes, B256, U256};
use alloy_rlp::Decodable;
use monad_query_schema::{
    bitmap::BitmapFilterValue,
    family::{Family, FamilyMetadata, StoredTxQueryItem},
};
use monad_query_types::{QueryError, QueryResult};

use super::{BlockRelations, QueryFamily, RelationKey};
use crate::{bitmap::BitmapFilter, request::QueryEnvelope, FullQueryPageSchema, QueryPageData};

#[derive(Debug, Clone, Default)]
pub struct TxQueryFilter {
    pub from: Option<HashSet<Address>>,
    pub to: Option<HashSet<Address>>,
    pub selector: Option<HashSet<[u8; 4]>>,
}

#[derive(Debug, Clone)]
pub struct QueryTransactionsRequest {
    pub envelope: QueryEnvelope,
    pub filter: TxQueryFilter,
    pub relations: BlockRelations,
}

pub struct TxQuery {
    filter: TxQueryFilter,
    relations: BlockRelations,
}

impl TxQuery {
    pub fn new(filter: TxQueryFilter, relations: BlockRelations) -> Self {
        Self { filter, relations }
    }
}

impl QueryFamily for TxQuery {
    fn family() -> Family {
        Family::Transaction
    }

    fn bitmap_filter(&self) -> BitmapFilter {
        let mut clauses = Vec::new();

        if let Some(values) = &self.filter.from {
            clauses.push(BitmapFilter::Any(
                values
                    .iter()
                    .copied()
                    .map(BitmapFilterValue::From)
                    .map(BitmapFilter::Value)
                    .collect::<Box<[_]>>(),
            ));
        }

        if let Some(values) = &self.filter.to {
            clauses.push(BitmapFilter::Any(
                values
                    .iter()
                    .copied()
                    .map(BitmapFilterValue::To)
                    .map(BitmapFilter::Value)
                    .collect::<Box<[_]>>(),
            ));
        }

        if let Some(values) = &self.filter.selector {
            clauses.push(BitmapFilter::Any(
                values
                    .iter()
                    .copied()
                    .map(BitmapFilterValue::Selector)
                    .map(BitmapFilter::Value)
                    .collect::<Box<[_]>>(),
            ));
        }

        BitmapFilter::All(clauses.into_boxed_slice())
    }

    type PayloadItem = StoredTxQueryItem;
    type Item = TxQueryItem;

    fn decode(&self, bytes: &mut &[u8]) -> QueryResult<Self::PayloadItem> {
        StoredTxQueryItem::decode(bytes)
            .map_err(|_| QueryError::Decode("invalid stored transaction"))
    }

    fn item(
        &self,
        payload: Self::PayloadItem,
        block_hash: B256,
        block_number: u64,
        metadata: &FamilyMetadata,
        primary_id: u64,
    ) -> QueryResult<Self::Item> {
        let Self::PayloadItem {
            block_timestamp,
            transaction_hash,
            sender,
            nonce,
            tx_type,
            input,
            value,
            gas,
            r,
            s,
            y_parity,
            logs_bloom,
            status,
            gas_used,
            cumulative_gas_used,
            effective_gas_price,
            chain_id,
            to,
            gas_price,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            max_fee_per_blob_gas,
            access_list,
            authorization_list,
            blob_versioned_hashes,
            contract_address,
        } = payload;

        let transaction_index = u32::try_from(primary_id - metadata.first_primary_id.as_u64())
            .map_err(|_| QueryError::Decode("transaction index overflow"))?;

        Ok(TxQueryItem {
            block_hash,
            block_number,
            block_timestamp,
            transaction_hash,
            transaction_index,
            sender,
            nonce,
            tx_type,
            input,
            value,
            gas,
            r,
            s,
            y_parity,
            logs_bloom,
            status,
            gas_used,
            cumulative_gas_used,
            effective_gas_price,
            chain_id,
            to,
            gas_price,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            max_fee_per_blob_gas,
            access_list,
            authorization_list,
            blob_versioned_hashes,
            contract_address,
        })
    }

    fn block_number(tx: &Self::Item) -> u64 {
        tx.block_number
    }

    fn relation_keys(&self, item: &Self::Item) -> Box<[RelationKey]> {
        if self.relations.blocks {
            vec![RelationKey::Block(item.block_number)].into_boxed_slice()
        } else {
            Box::new([])
        }
    }

    fn matches(&self, other: &Self::Item) -> bool {
        self.filter
            .from
            .as_ref()
            .is_none_or(|values| values.contains(&other.sender))
            && self
                .filter
                .to
                .as_ref()
                .is_none_or(|values| other.to.is_some_and(|to| values.contains(&to)))
            && self.filter.selector.as_ref().is_none_or(|values| {
                other
                    .selector()
                    .is_some_and(|selector| values.contains(&selector))
            })
    }

    fn update_page_data(
        &self,
        data: &mut QueryPageData<FullQueryPageSchema>,
        items: Box<[Self::Item]>,
    ) -> QueryResult<()> {
        if data.transactions.is_some() {
            return Err(QueryError::Internal("query page data field already set"));
        }

        data.transactions = Some(items);
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxQueryItem {
    pub block_hash: B256,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub transaction_hash: B256,
    pub transaction_index: u32,
    pub sender: Address,
    pub nonce: u64,
    pub tx_type: u8,
    pub input: Bytes,
    pub value: U256,
    pub gas: u64,
    pub r: U256,
    pub s: U256,
    pub y_parity: bool,
    pub logs_bloom: alloy_primitives::Bloom,
    pub status: bool,
    pub gas_used: u64,
    pub cumulative_gas_used: u64,
    pub effective_gas_price: alloy_primitives::U256,
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

impl TxQueryItem {
    /// Extracts 4-byte function selector from tx calldata.
    pub fn selector(&self) -> Option<[u8; 4]> {
        (self.input.len() >= 4).then(|| self.input[..4].try_into().expect("four-byte selector"))
    }
}
