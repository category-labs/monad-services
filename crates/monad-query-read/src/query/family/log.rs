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

use alloy_primitives::{Address, Bytes, B256};
use alloy_rlp::Decodable;
use monad_query_schema::{
    bitmap::BitmapFilterValue,
    family::{Family, FamilyMetadata, StoredLogQueryItem},
};
use monad_query_types::{QueryError, QueryResult};

use super::{QueryFamily, RelationKey, TransactionRelations};
use crate::{bitmap::BitmapFilter, request::QueryEnvelope, FullQueryPageSchema, QueryPageData};

#[derive(Debug, Clone, Default)]
pub struct LogQueryFilter {
    pub address: Option<HashSet<Address>>,
    pub topics: [Option<HashSet<B256>>; 4],
}

#[derive(Debug, Clone)]
pub struct QueryLogsRequest {
    pub envelope: QueryEnvelope,
    pub filter: LogQueryFilter,
    pub relations: TransactionRelations,
}

pub struct LogQuery {
    filter: LogQueryFilter,
    relations: TransactionRelations,
}

impl LogQuery {
    pub fn new(filter: LogQueryFilter, relations: TransactionRelations) -> Self {
        Self { filter, relations }
    }
}

impl QueryFamily for LogQuery {
    fn family() -> Family {
        Family::Log
    }

    fn bitmap_filter(&self) -> BitmapFilter {
        let mut clauses = Vec::new();

        if let Some(values) = &self.filter.address {
            clauses.push(BitmapFilter::Any(
                values
                    .iter()
                    .cloned()
                    .map(BitmapFilterValue::Address)
                    .map(BitmapFilter::Value)
                    .collect::<Box<[_]>>(),
            ));
        }

        for (position, values) in self.filter.topics.iter().enumerate() {
            if let Some(values) = values {
                clauses.push(BitmapFilter::Any(
                    values
                        .iter()
                        .map(|topic| match position {
                            0 => BitmapFilterValue::Topic0(*topic),
                            1 => BitmapFilterValue::Topic1(*topic),
                            2 => BitmapFilterValue::Topic2(*topic),
                            _ => BitmapFilterValue::Topic3(*topic),
                        })
                        .map(BitmapFilter::Value)
                        .collect::<Box<[_]>>(),
                ));
            }
        }

        BitmapFilter::All(clauses.into_boxed_slice())
    }

    type PayloadItem = StoredLogQueryItem;
    type Item = LogQueryItem;

    fn decode(&self, bytes: &mut &[u8]) -> QueryResult<Self::PayloadItem> {
        StoredLogQueryItem::decode(bytes).map_err(|_| QueryError::Decode("invalid stored log"))
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
            block_timestamp,
            transaction_hash,
            transaction_index,
            log_index,
            address,
            topics,
            data,
        } = payload;

        Ok(LogQueryItem {
            block_hash,
            block_number,
            block_timestamp,
            transaction_hash,
            transaction_index,
            log_index: log_index,
            address: address,
            topics: topics,
            data: data,
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
        self.filter
            .address
            .as_ref()
            .is_none_or(|set| set.contains(&item.address))
            && self.filter.topics.iter().enumerate().all(|(i, set)| {
                set.as_ref()
                    .is_none_or(|set| item.topics.get(i).is_some_and(|topic| set.contains(topic)))
            })
    }

    fn update_page_data(
        &self,
        data: &mut QueryPageData<FullQueryPageSchema>,
        items: Box<[Self::Item]>,
    ) -> QueryResult<()> {
        if data.logs.is_some() {
            return Err(QueryError::Internal("query page data field already set"));
        }

        data.logs = Some(items);
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogQueryItem {
    pub block_hash: B256,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub transaction_hash: B256,
    pub transaction_index: u32,
    pub log_index: u32,
    pub address: Address,
    pub topics: Vec<B256>,
    pub data: Bytes,
}
