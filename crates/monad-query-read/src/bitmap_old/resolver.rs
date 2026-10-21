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

use monad_query_schema::family::Family;

use std::{collections::VecDeque, ops::Range};

use futures::future::LocalBoxFuture;
use monad_query_schema::tables::{bitmap::BitmapTables};
use monad_query_types::{
    store::{
        blob::table::BlobTable,
        meta::{MetaScanOrder, MetaStore},
    },
    QueryResult,
};

use super::{
    executor::BitmapExecutor,
    plan::BitmapPlanner,
    stats::{BitmapPageStats, BitmapStatsReader},
    BitmapCandidateIdIntervals, BitmapFilter, BitmapRangePlan,
};
use crate::{request::QueryOrder, window::WindowRangeSource};

const PAGE_STATS_BATCH_SIZE: usize = 64;

pub struct BitmapResolver<'a, M: BlobTable, S: MetaStore> {
    stats: BitmapStatsReader<'a, M, S>,
    executor: BitmapExecutor<'a, M, S>,
}

impl<'a, M: BlobTable, S: MetaStore> BitmapResolver<'a, M, S> {
    pub fn new(tables: &'a BitmapTables<M, S>) -> Self {
        Self {
            stats: BitmapStatsReader::new(tables),
            executor: BitmapExecutor::new(tables),
        }
    }

    pub(crate) fn work<'b>(
        &'b self,
        family: Family,
        filter: BitmapFilter,
        start: u64,
        end: u64,
        order: QueryOrder,
    ) -> BitmapWork<'b, 'a, M, S> {
        BitmapWork::new(&self.stats, family, filter, start, end, order)
    }

    pub async fn select(
        &self,
        family: Family,
        filter: &BitmapFilter,
        start: u64,
        end: u64,
        limit: usize,
        order: QueryOrder,
    ) -> QueryResult<BitmapCandidateIdIntervals> {
        let plan = BitmapPlanner::plan(&self.stats, family, filter, start, end, order).await?;
        let candidates = self.executor.execute(&plan, limit, order).await?;
        Ok(candidates)
    }
}

pub(crate) struct BitmapWork<'r, 't, M: BlobTable, S: MetaStore> {
    stats: &'r BitmapStatsReader<'t, M, S>,
    family: Family,
    plan: BitmapRangePlan,
    range: Option<Range<u64>>,
    first_page: u64,
    last_page: u64,
    next_page: Option<u64>,
    pending: VecDeque<Range<u64>>,
    order: MetaScanOrder,
}

impl<'r, 't, M: BlobTable, S: MetaStore> BitmapWork<'r, 't, M, S> {
    fn new(
        stats: &'r BitmapStatsReader<'t, M, S>,
        family: Family,
        filter: BitmapFilter,
        start: u64,
        end: u64,
        order: QueryOrder,
    ) -> Self {
        let plan = filter.range_plan();
        let (first_page, last_page) = if start < end {
            (
                start / BitmapIndexValue::PAGE_SPAN * BitmapIndexValue::PAGE_SPAN,
                (end - 1) / BitmapIndexValue::PAGE_SPAN * BitmapIndexValue::PAGE_SPAN,
            )
        } else {
            (0, 0)
        };
        let next_page = match plan {
            BitmapRangePlan::Indexed(_) => Some(match order {
                QueryOrder::Ascending => first_page,
                QueryOrder::Descending => last_page,
            }),
            BitmapRangePlan::Unfiltered | BitmapRangePlan::NoMatches => None,
        };
        Self {
            stats,
            family,
            plan,
            range: (start < end).then_some(start..end),
            first_page,
            last_page,
            next_page,
            pending: VecDeque::new(),
            order: match order {
                QueryOrder::Ascending => MetaScanOrder::Ascending,
                QueryOrder::Descending => MetaScanOrder::Descending,
            },
        }
    }

    async fn next_work_range(&mut self) -> QueryResult<Option<Range<u64>>> {
        match self.plan.clone() {
            BitmapRangePlan::Unfiltered => Ok(self.range.take()),
            BitmapRangePlan::NoMatches => Ok(None),
            BitmapRangePlan::Indexed(filter) => {
                if let Some(range) = self.pending.pop_front() {
                    return Ok(Some(range));
                }

                loop {
                    let Some(page) = self.next_page else {
                        return Ok(None);
                    };
                    let rows = self
                        .stats
                        .read_batch(
                            self.family,
                            &filter,
                            match self.order {
                                MetaScanOrder::Ascending => page,
                                MetaScanOrder::Descending => self.first_page,
                            },
                            match self.order {
                                MetaScanOrder::Ascending => self.last_page,
                                MetaScanOrder::Descending => page,
                            },
                            self.order,
                            PAGE_STATS_BATCH_SIZE,
                        )
                        .await?;
                    self.advance(&rows);
                    self.pending = Self::page_ranges(
                        rows,
                        self.range.as_ref().map_or(0, |range| range.start),
                        self.range.as_ref().map_or(0, |range| range.end),
                        self.order,
                    );
                    if let Some(range) = self.pending.pop_front() {
                        return Ok(Some(range));
                    }
                }
            }
        }
    }

    fn advance(&mut self, rows: &[BitmapPageStats]) {
        if rows.len() < PAGE_STATS_BATCH_SIZE {
            self.next_page = None;
            return;
        }

        let Some(page) = rows.last().map(|stats| stats.page) else {
            self.next_page = None;
            return;
        };
        self.next_page = match self.order {
            MetaScanOrder::Ascending => page
                .checked_add(BitmapIndexValue::PAGE_SPAN)
                .filter(|page| *page <= self.last_page),
            MetaScanOrder::Descending => page
                .checked_sub(BitmapIndexValue::PAGE_SPAN)
                .filter(|page| *page >= self.first_page),
        };
    }

    fn page_ranges(
        pages: Box<[BitmapPageStats]>,
        start: u64,
        end: u64,
        order: MetaScanOrder,
    ) -> VecDeque<Range<u64>> {
        let mut ranges: VecDeque<Range<u64>> = VecDeque::new();
        for page in pages {
            if page.count == 0 {
                continue;
            }
            let range = start.max(page.page)..end.min(page.page.saturating_add(BitmapIndexValue::PAGE_SPAN));
            if range.start >= range.end {
                continue;
            }
            match order {
                MetaScanOrder::Ascending => {
                    if let Some(previous) = ranges.back_mut() {
                        if previous.end == range.start {
                            previous.end = range.end;
                            continue;
                        }
                    }
                }
                MetaScanOrder::Descending => {
                    if let Some(previous) = ranges.back_mut() {
                        if previous.start == range.end {
                            previous.start = range.start;
                            continue;
                        }
                    }
                }
            }
            ranges.push_back(range);
        }
        ranges
    }
}

impl<'r, 't, M: BlobTable, S: MetaStore> WindowRangeSource for BitmapWork<'r, 't, M, S> {
    fn next_range<'b>(&'b mut self) -> LocalBoxFuture<'b, QueryResult<Option<Range<u64>>>> {
        Box::pin(async move { self.next_work_range().await })
    }
}
