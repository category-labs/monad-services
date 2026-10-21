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

use alloy_primitives::{Address, B256};
use alloy_rlp::Decodable;
use monad_query_schema::{
    bitmap::BitmapFilterValue,
    family::{Family, FamilyMetadata, StoredTraceQueryItem},
};
use monad_query_types::{QueryError, QueryResult};

use super::{QueryFamily, RelationKey, TransactionRelations};
use crate::{bitmap::BitmapFilter, request::QueryEnvelope, FullQueryPageSchema, QueryPageData};

#[derive(Debug, Clone, Default)]
pub struct TraceQueryRequestFilter {
    pub from: Option<HashSet<Address>>,
    pub to: Option<HashSet<Address>>,
    pub selector: Option<HashSet<[u8; 4]>>,
    pub include_reverted: bool,
}

#[derive(Debug, Clone)]
pub struct TraceQueryRequest {
    pub envelope: QueryEnvelope,
    pub filter: TraceQueryRequestFilter,
    pub relations: TransactionRelations,
}

pub struct TraceQuery {
    filter: TraceQueryRequestFilter,
    relations: TransactionRelations,
}

impl TraceQuery {
    pub fn new(filter: TraceQueryRequestFilter, relations: TransactionRelations) -> Self {
        Self { filter, relations }
    }
}

impl QueryFamily for TraceQuery {
    fn family() -> Family {
        Family::Trace
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

    type PayloadItem = StoredTraceQueryItem;
    type Item = TraceQueryItem;

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
        Ok(TraceQueryItem {
            block_number,
            block_hash,
            trace: payload,
        })
    }

    fn block_number(trace: &Self::Item) -> u64 {
        trace.block_number
    }

    fn relation_keys(&self, item: &Self::Item) -> Box<[RelationKey]> {
        let mut keys = Vec::new();

        if self.relations.blocks {
            keys.push(RelationKey::Block(item.block_number));
        }

        if self.relations.transactions {
            keys.push(RelationKey::Transaction {
                block: item.block_number,
                index: item.trace.transaction_index,
            });
        }

        keys.into_boxed_slice()
    }

    fn matches(&self, trace: &Self::Item) -> bool {
        self.filter
            .from
            .as_ref()
            .is_none_or(|values| values.contains(&trace.trace.from))
            && self
                .filter
                .to
                .as_ref()
                .is_none_or(|values| trace.trace.to.is_some_and(|to| values.contains(&to)))
            && self.filter.selector.as_ref().is_none_or(|values| {
                trace
                    .selector()
                    .is_some_and(|selector| values.contains(&selector))
            })
            && (self.filter.include_reverted || !trace.trace.reverted)
    }

    fn update_page_data(
        &self,
        data: &mut QueryPageData<FullQueryPageSchema>,
        items: Box<[Self::Item]>,
    ) -> QueryResult<()> {
        if data.traces.is_some() {
            return Err(QueryError::Internal("query page data field already set"));
        }

        data.traces = Some(items);
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceQueryItem {
    pub block_hash: B256,
    pub block_number: u64,
    pub trace: StoredTraceQueryItem,
}

impl TraceQueryItem {
    pub fn selector(&self) -> Option<[u8; 4]> {
        self.trace
            .input
            .get(..4)
            .and_then(|selector| selector.try_into().ok())
    }
}
