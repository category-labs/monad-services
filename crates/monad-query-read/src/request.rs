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

use std::num::NonZeroUsize;

use itertools::Either;
use serde::Deserialize;

/// Request envelope for query operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryEnvelope {
    pub from_block: Option<QueryBound>,
    pub to_block: Option<QueryBound>,
    pub order: QueryOrder,
    pub target: Option<NonZeroUsize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
pub enum QueryOrder {
    #[default]
    #[serde(rename = "asc")]
    Ascending,
    #[serde(rename = "desc")]
    Descending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryBound {
    Earliest,
    Latest,
    Number(u64),
}

impl QueryOrder {
    pub fn iterate<I>(self, items: I) -> impl Iterator<Item = I::Item>
    where
        I: IntoIterator,
        I::IntoIter: DoubleEndedIterator,
    {
        match self {
            Self::Ascending => Either::Left(items.into_iter()),
            Self::Descending => Either::Right(items.into_iter().rev()),
        }
    }
}
