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

use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_query_types::{PrimaryId, QueryResult};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use strum::EnumIter;

pub use self::{
    block::StoredBlockQueryItem,
    log::StoredLogQueryItem,
    trace::{StoredTraceQueryItem, StoredTraceQueryItemWithAncestorStatus},
    transaction::StoredTxQueryItem,
};
use crate::bitmap::BitmapFilterValue;

mod block;
mod log;
mod trace;
mod transaction;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    EnumIter,
    IntoPrimitive,
    TryFromPrimitive,
)]
#[repr(u8)]
pub enum Family {
    BlockHeader,
    Transaction,
    Log,
    Trace,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct FamilyMetadata {
    pub first_primary_id: PrimaryId,
    pub count: u32,
}

pub trait BitmapIndexable {
    fn bitmap_filters(&self) -> QueryResult<Box<[BitmapFilterValue]>>;
}
