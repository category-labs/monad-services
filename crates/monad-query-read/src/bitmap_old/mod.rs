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

use monad_query_schema::bitmap::BitmapFilterValue;

mod executor;
mod interval;
mod page_reader;
mod plan;
mod resolver;
mod stats;

#[derive(Debug, Clone)]
pub(crate) enum BitmapFilter {
    Unfiltered,
    NoMatches,
    Value(BitmapFilterValue),
    Any(Box<[BitmapFilter]>),
    All(Box<[BitmapFilter]>),
}

#[derive(Debug, Clone)]
pub(crate) enum BitmapRangePlan {
    Unfiltered,
    NoMatches,
    Indexed(BitmapFilterValue),
}

impl BitmapFilter {
    pub(crate) fn unfiltered() -> Self {
        Self::Unfiltered
    }

    pub(crate) fn value(value: BitmapFilterValue) -> Self {
        Self::Value(value)
    }

    pub(crate) fn any(filters: Box<[Self]>) -> Self {
        if filters.is_empty() {
            Self::NoMatches
        } else {
            Self::Any(filters)
        }
    }

    pub(crate) fn all(filters: Box<[Self]>) -> Self {
        if filters.is_empty() {
            Self::Unfiltered
        } else {
            Self::All(filters)
        }
    }

    pub(crate) fn range_plan(&self) -> BitmapRangePlan {
        match self {
            Self::Unfiltered => BitmapRangePlan::Unfiltered,
            Self::NoMatches => BitmapRangePlan::NoMatches,
            Self::Value(filter) => BitmapRangePlan::Indexed(filter.clone()),
            Self::Any(filters) => {
                let mut indexed = None;
                for filter in filters {
                    match filter.range_plan() {
                        BitmapRangePlan::Unfiltered => return BitmapRangePlan::Unfiltered,
                        BitmapRangePlan::NoMatches => {}
                        BitmapRangePlan::Indexed(value) => {
                            if indexed.is_some() {
                                return BitmapRangePlan::Unfiltered;
                            }
                            indexed = Some(value);
                        }
                    }
                }
                indexed.map_or(BitmapRangePlan::NoMatches, BitmapRangePlan::Indexed)
            }
            Self::All(filters) => {
                for filter in filters {
                    match filter.range_plan() {
                        BitmapRangePlan::Unfiltered => {}
                        BitmapRangePlan::NoMatches => return BitmapRangePlan::NoMatches,
                        BitmapRangePlan::Indexed(value) => return BitmapRangePlan::Indexed(value),
                    }
                }
                BitmapRangePlan::Unfiltered
            }
        }
    }
}

pub(crate) use self::{interval::BitmapCandidateIdIntervals, resolver::BitmapResolver};
