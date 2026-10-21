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

use alloy_primitives::B256;
use monad_query_schema::family::{Family, FamilyMetadata};
use monad_query_types::QueryResult;

use crate::{bitmap::BitmapFilter, FullQueryPageSchema, QueryPageData};

pub mod block;
pub mod log;
pub mod trace;
pub mod transaction;
pub mod transfer;

pub(crate) trait QueryFamily {
    fn family() -> Family;

    fn bitmap_filter(&self) -> BitmapFilter;

    type PayloadItem: Clone;
    type Item;

    fn decode(&self, bytes: &mut &[u8]) -> QueryResult<Self::PayloadItem>;

    fn item(
        &self,
        payload: Self::PayloadItem,
        block_hash: B256,
        block_number: u64,
        metadata: &FamilyMetadata,
        primary_id: u64,
    ) -> QueryResult<Self::Item>;

    fn block_number(item: &Self::Item) -> u64;

    fn relation_keys(&self, item: &Self::Item) -> Box<[RelationKey]>;

    fn matches(&self, item: &Self::Item) -> bool;

    fn update_page_data(
        &self,
        data: &mut QueryPageData<FullQueryPageSchema>,
        items: Box<[Self::Item]>,
    ) -> QueryResult<()>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RelationKey {
    Block(u64),
    Transaction { block: u64, index: u32 },
}

#[derive(Debug, Clone, Copy, Default)]
pub struct BlockRelations {
    pub blocks: bool,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct TransactionRelations {
    pub transactions: bool,
    pub blocks: bool,
}
