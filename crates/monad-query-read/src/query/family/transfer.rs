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

use alloy_primitives::{Address, Bytes, B256, U256};
use alloy_rlp::Decodable;
use monad_query_schema::{
    bitmap::BitmapFilterValue,
    family::{Family, FamilyMetadata, StoredTraceQueryItem},
};
use monad_query_types::{CallKind, QueryError, QueryResult};

use super::{QueryFamily, RelationKey, TransactionRelations};
use crate::{bitmap::BitmapFilter, request::QueryEnvelope, FullQueryPageSchema, QueryPageData};

#[derive(Debug, Clone, Default)]
pub struct TransferQueryFilter {
    pub from: Option<HashSet<Address>>,
    pub to: Option<HashSet<Address>>,
    pub include_reverted: bool,
}

#[derive(Debug, Clone)]
pub struct TransferQueryRequest {
    pub envelope: QueryEnvelope,
    pub filter: TransferQueryFilter,
    pub relations: TransactionRelations,
}

pub struct TransferQuery {
    filter: TransferQueryFilter,
    relations: TransactionRelations,
}

impl TransferQuery {
    pub fn new(filter: TransferQueryFilter, relations: TransactionRelations) -> Self {
        Self { filter, relations }
    }
}

impl QueryFamily for TransferQuery {
    fn family() -> Family {
        Family::Trace
    }

    fn bitmap_filter(&self) -> BitmapFilter {
        let mut clauses = Vec::new();

        if !self.filter.include_reverted {
            clauses.push(BitmapFilter::Value(BitmapFilterValue::HasTransfer));
        }

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

        BitmapFilter::All(clauses.into_boxed_slice())
    }

    type PayloadItem = StoredTraceQueryItem;
    type Item = TransferQueryItem;

    fn decode(&self, bytes: &mut &[u8]) -> QueryResult<Self::PayloadItem> {
        StoredTraceQueryItem::decode(bytes).map_err(|_| QueryError::Decode("invalid stored trace"))
    }

    fn item(
        &self,
        payload: Self::PayloadItem,
        block_hash: B256,
        block_number: u64,
        _metadata: &FamilyMetadata,
        _primary_id: u64,
    ) -> QueryResult<Self::Item> {
        let Self::PayloadItem {
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
        } = payload;

        let to = to.ok_or(QueryError::Decode("transfer frame missing `to` address"))?;

        Ok(TransferQueryItem {
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            trace_address,
            typ: r#type,
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

    fn block_number(item: &Self::Item) -> u64 {
        item.block_number
    }

    fn relation_keys(&self, item: &Self::Item) -> Box<[RelationKey]> {
        let mut keys = Vec::new();

        if self.relations.blocks {
            keys.push(RelationKey::Block(item.block_number));
        }

        if self.relations.transactions {
            keys.push(RelationKey::Transaction {
                block: item.block_number,
                index: item.transaction_index,
            });
        }

        keys.into_boxed_slice()
    }

    fn matches(&self, item: &Self::Item) -> bool {
        item.value != U256::ZERO
            && self
                .filter
                .from
                .as_ref()
                .is_none_or(|values| values.contains(&item.from))
            && self
                .filter
                .to
                .as_ref()
                .is_none_or(|values| values.contains(&item.to))
            && (self.filter.include_reverted || !item.reverted)
    }

    fn update_page_data(
        &self,
        data: &mut QueryPageData<FullQueryPageSchema>,
        items: Box<[Self::Item]>,
    ) -> QueryResult<()> {
        if data.transfers.is_some() {
            return Err(QueryError::Internal("query page data field already set"));
        }

        data.transfers = Some(items);
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferQueryItem {
    pub block_hash: B256,
    pub block_number: u64,
    pub transaction_hash: B256,
    pub transaction_index: u32,
    pub trace_address: Box<[u32]>,
    pub typ: CallKind,
    pub from: Address,
    pub to: Address,
    pub value: U256,
    pub gas: u64,
    pub gas_used: u64,
    pub input: Bytes,
    pub output: Bytes,
    pub status: u8,
    pub reverted: bool,
}
