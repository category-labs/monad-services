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

use alloy_consensus::Header;
use alloy_primitives::{Address, B256, U256};
use alloy_rlp::Decodable;
use monad_query_schema::{
    bitmap::BitmapFilterValue,
    family::{Family, FamilyMetadata, StoredBlockQueryItem},
};
use monad_query_types::{QueryError, QueryResult};

use super::{QueryFamily, RelationKey};
use crate::{bitmap::BitmapFilter, request::QueryEnvelope, FullQueryPageSchema, QueryPageData};

#[derive(Debug, Clone, Default)]
pub struct BlockQueryFilter {
    pub miner: Option<HashSet<Address>>,
}

#[derive(Debug, Clone)]
pub struct BlockQueryRequest {
    pub envelope: QueryEnvelope,
    pub filter: BlockQueryFilter,
}

pub struct BlockQuery {
    filter: BlockQueryFilter,
}

impl BlockQuery {
    pub fn new(filter: BlockQueryFilter) -> Self {
        Self { filter }
    }
}

impl QueryFamily for BlockQuery {
    fn family() -> Family {
        Family::BlockHeader
    }

    fn bitmap_filter(&self) -> BitmapFilter {
        self.filter.miner.as_ref().map_or_else(
            || BitmapFilter::All(Box::new([])),
            |values| {
                BitmapFilter::Any(
                    values
                        .iter()
                        .copied()
                        .map(BitmapFilterValue::Miner)
                        .map(BitmapFilter::Value)
                        .collect(),
                )
            },
        )
    }

    type PayloadItem = StoredBlockQueryItem;
    type Item = BlockQueryItem;

    fn decode(&self, bytes: &mut &[u8]) -> QueryResult<Self::PayloadItem> {
        StoredBlockQueryItem::decode(bytes).map_err(|_| QueryError::Decode("invalid block header"))
    }

    fn item(
        &self,
        header: Self::PayloadItem,
        block_hash: B256,
        _block_number: u64,
        _metadata: &FamilyMetadata,
        _primary_id: u64,
    ) -> QueryResult<Self::Item> {
        Ok(BlockQueryItem {
            hash: block_hash,
            header: header.header,
            size: header.size,
            total_difficulty: header.total_difficulty,
        })
    }

    fn block_number(item: &Self::Item) -> u64 {
        item.header.number
    }

    fn relation_keys(&self, _item: &Self::Item) -> Box<[RelationKey]> {
        Box::new([])
    }

    fn matches(&self, _item: &Self::Item) -> bool {
        true
    }

    fn update_page_data(
        &self,
        data: &mut QueryPageData<FullQueryPageSchema>,
        items: Box<[Self::Item]>,
    ) -> QueryResult<()> {
        if data.blocks.is_some() {
            return Err(QueryError::Internal("query page data field already set"));
        }

        data.blocks = Some(items);
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockQueryItem {
    pub hash: B256,
    pub header: Header,
    pub size: u64,
    pub total_difficulty: U256,
}
