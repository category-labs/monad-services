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

use std::ops::Range;

use futures::future::LocalBoxFuture;
use monad_query_schema::tables::bitmap::index::BitmapIndexValue;
use monad_query_types::QueryResult;

use crate::request::QueryOrder;

#[derive(Debug, Clone, Copy)]
pub enum WindowSchedulerGrowthStrategy {
    Linear,
    Exponential(f64),
}

#[derive(Debug, Clone, Copy)]
pub struct WindowSchedulerConfig {
    pub size_initial: usize,
    pub size_max: usize,
    pub size_growth: WindowSchedulerGrowthStrategy,
    pub concurrency_initial: usize,
    pub concurrency_max: usize,
    pub concurrency_growth: WindowSchedulerGrowthStrategy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryWindow {
    pub from: u64,
    pub to: u64,
}

pub(crate) trait WindowRangeSource {
    fn next_range<'a>(&'a mut self) -> LocalBoxFuture<'a, QueryResult<Option<Range<u64>>>>;
}

impl WindowRangeSource for Option<Range<u64>> {
    fn next_range<'a>(&'a mut self) -> LocalBoxFuture<'a, QueryResult<Option<Range<u64>>>> {
        Box::pin(async move { Ok(self.take()) })
    }
}

pub struct WindowScheduler<'a> {
    source: Box<dyn WindowRangeSource + 'a>,
    current: Option<Range<u64>>,
    order: QueryOrder,
    size: u64,
    cursor: u64,
    concurrency: usize,
    outstanding: usize,
    config: WindowSchedulerConfig,
}

impl<'a> WindowScheduler<'a> {
    pub(crate) fn from_source<S: WindowRangeSource + 'a>(
        source: S,
        order: QueryOrder,
        config: WindowSchedulerConfig,
    ) -> Self {
        assert!(config.size_initial > 0, "window size must be non-zero");
        Self {
            source: Box::new(source),
            current: None,
            order,
            size: config.size_initial as u64,
            cursor: 0,
            concurrency: config.concurrency_initial.max(1),
            outstanding: 0,
            config,
        }
    }

    pub async fn next(&mut self) -> QueryResult<Option<QueryWindow>> {
        if self.outstanding >= self.concurrency {
            return Ok(None);
        }

        loop {
            if self.current.is_none() {
                self.current = self.source.next_range().await?;
                let Some(range) = self.current.as_ref() else {
                    return Ok(None);
                };
                if range.start >= range.end {
                    self.current = None;
                    continue;
                }
                self.cursor = match self.order {
                    QueryOrder::Ascending => range.start,
                    QueryOrder::Descending => range.end - 1,
                };
            }

            let range = self
                .current
                .as_ref()
                .expect("scheduler range was initialized")
                .clone();
            let window = match self.order {
                QueryOrder::Ascending if self.cursor < range.end => {
                    let from = self.cursor;
                    let end = from
                        .saturating_add(self.size)
                        .min(range.end)
                        .max(
                            from / BitmapIndexValue::PAGE_SPAN * BitmapIndexValue::PAGE_SPAN
                                + BitmapIndexValue::PAGE_SPAN,
                        )
                        .min(range.end);
                    self.cursor = end;
                    QueryWindow { from, to: end - 1 }
                }
                QueryOrder::Descending if self.cursor >= range.start => {
                    let cursor = self.cursor;
                    let to = cursor + 1;
                    let raw_start = to.saturating_sub(self.size).max(range.start);
                    let page_start =
                        cursor / BitmapIndexValue::PAGE_SPAN * BitmapIndexValue::PAGE_SPAN;
                    let start = if cursor - page_start < self.size {
                        page_start
                    } else {
                        raw_start / BitmapIndexValue::PAGE_SPAN * BitmapIndexValue::PAGE_SPAN
                    }
                    .max(range.start);
                    self.cursor = start.saturating_sub(1);
                    QueryWindow {
                        from: start,
                        to: to - 1,
                    }
                }
                _ => {
                    self.current = None;
                    continue;
                }
            };
            self.outstanding += 1;
            return Ok(Some(window));
        }
    }

    pub fn observe(&mut self, accepted: usize, target: usize) {
        self.outstanding = self.outstanding.saturating_sub(1);
        if accepted >= target {
            return;
        }
        self.size = Self::grow(
            self.size,
            self.config.size_max as u64,
            self.config.size_growth,
        );
        self.concurrency = Self::grow(
            self.concurrency as u64,
            self.config.concurrency_max as u64,
            self.config.concurrency_growth,
        ) as usize;
    }

    fn grow(value: u64, max: u64, strategy: WindowSchedulerGrowthStrategy) -> u64 {
        match strategy {
            WindowSchedulerGrowthStrategy::Linear => value.saturating_add(1).min(max),
            WindowSchedulerGrowthStrategy::Exponential(factor) => ((value as f64 * factor).ceil()
                as u64)
                .max(value + 1)
                .min(max),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn aligns_ascending_windows_to_bitmap_pages() {
        let mut scheduler = WindowScheduler::from_source(
            Some(5..(BitmapIndexValue::PAGE_SPAN + 5)),
            QueryOrder::Ascending,
            config(),
        );
        assert_eq!(
            scheduler.next().await.unwrap(),
            Some(QueryWindow {
                from: 5,
                to: BitmapIndexValue::PAGE_SPAN - 1
            })
        );
        scheduler.observe(0, 1);
        assert_eq!(
            scheduler.next().await.unwrap(),
            Some(QueryWindow {
                from: BitmapIndexValue::PAGE_SPAN,
                to: BitmapIndexValue::PAGE_SPAN + 4
            })
        );
    }

    #[tokio::test]
    async fn aligns_descending_windows_to_bitmap_pages() {
        let mut scheduler = WindowScheduler::from_source(
            Some(5..(BitmapIndexValue::PAGE_SPAN + 5)),
            QueryOrder::Descending,
            config(),
        );
        assert_eq!(
            scheduler.next().await.unwrap(),
            Some(QueryWindow {
                from: BitmapIndexValue::PAGE_SPAN,
                to: BitmapIndexValue::PAGE_SPAN + 4
            })
        );
        scheduler.observe(0, 1);
        assert_eq!(
            scheduler.next().await.unwrap(),
            Some(QueryWindow {
                from: 5,
                to: BitmapIndexValue::PAGE_SPAN - 1
            })
        );
    }

    fn config() -> WindowSchedulerConfig {
        WindowSchedulerConfig {
            size_initial: 1024,
            size_max: 1 << 20,
            size_growth: WindowSchedulerGrowthStrategy::Exponential(2.0),
            concurrency_initial: 1,
            concurrency_max: 4,
            concurrency_growth: WindowSchedulerGrowthStrategy::Exponential(1.5),
        }
    }
}
