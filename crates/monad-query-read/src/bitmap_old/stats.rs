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

use dashmap::DashMap;
use monad_query_schema::{
    bitmap::BitmapFilterValue,
    tables::{bitmap::page_stats::BitmapPageStatsKey, bitmap::BitmapTables},
};
use monad_query_types::{
    store::{
        blob::table::BlobTable,
        meta::{MetaScanOrder, MetaStore},
    },
    QueryResult,
};

#[derive(Debug, Clone, Copy)]
pub struct BitmapPageStats {
    pub page: u64,
    pub count: u32,
}

pub struct BitmapStatsReader<'a, M: BlobTable, S: MetaStore> {
    tables: &'a BitmapTables<M, S>,
    cache: DashMap<(Family, BitmapFilterValue, u64), u32>,
}

impl<'a, M: BlobTable, S: MetaStore> BitmapStatsReader<'a, M, S> {
    pub fn new(tables: &'a BitmapTables<M, S>) -> Self {
        Self {
            tables,
            cache: DashMap::new(),
        }
    }

    pub async fn read(
        &self,
        family: Family,
        filter: &BitmapFilterValue,
        start: u64,
        end: u64,
        order: MetaScanOrder,
    ) -> QueryResult<Box<[BitmapPageStats]>> {
        if start >= end {
            return Ok(Box::new([]));
        }

        let first = start / BitmapIndexValue::PAGE_SPAN * BitmapIndexValue::PAGE_SPAN;
        let last = (end - 1) / BitmapIndexValue::PAGE_SPAN * BitmapIndexValue::PAGE_SPAN;
        let mut missing = Vec::new();
        for page in (first..=last).step_by(BitmapIndexValue::PAGE_SPAN as usize) {
            if !self.cache.contains_key(&(family, filter.clone(), page)) {
                missing.push(page);
            }
        }

        if let (Some(&from), Some(&to)) = (missing.first(), missing.last()) {
            let rows = self
                .tables
                .page_stats
                .scan(
                    &BitmapPageStatsKey {
                        family,
                        filter: filter.clone(),
                        page: from,
                    },
                    &BitmapPageStatsKey {
                        family,
                        filter: filter.clone(),
                        page: to,
                    },
                    order,
                    None,
                )
                .await?;
            for (key, count) in rows {
                self.cache.insert((key.family, key.filter, key.page), count);
            }
            for page in missing {
                self.cache
                    .entry((family, filter.clone(), page))
                    .or_insert(0);
            }
        }

        Ok((first..=last)
            .step_by(BitmapIndexValue::PAGE_SPAN as usize)
            .map(|page| BitmapPageStats {
                page,
                count: self
                    .cache
                    .get(&(family, filter.clone(), page))
                    .map_or(0, |value| *value),
            })
            .collect())
    }

    pub async fn read_batch(
        &self,
        family: Family,
        filter: &BitmapFilterValue,
        start: u64,
        end: u64,
        order: MetaScanOrder,
        limit: usize,
    ) -> QueryResult<Box<[BitmapPageStats]>> {
        if start > end || limit == 0 {
            return Ok(Box::new([]));
        }

        let rows = self
            .tables
            .page_stats
            .scan(
                &BitmapPageStatsKey {
                    family,
                    filter: filter.clone(),
                    page: start,
                },
                &BitmapPageStatsKey {
                    family,
                    filter: filter.clone(),
                    page: end,
                },
                order,
                Some(limit),
            )
            .await?;
        Ok(rows
            .into_vec()
            .into_iter()
            .map(|(key, count)| {
                self.cache
                    .insert((key.family, key.filter.clone(), key.page), count);
                BitmapPageStats {
                    page: key.page,
                    count,
                }
            })
            .collect())
    }
}
