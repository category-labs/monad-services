use std::{future::Future, pin::Pin};

use monad_query_schema::tables::bitmap::BitmapTables;
use monad_query_types::{
    store::{blob::table::BlobTable, meta::MetaStore},
    QueryError, QueryResult,
};

use super::{
    interval::BitmapCandidateIdIntervals,
    page_reader::{BitmapPageReader, BitmapPageSet},
    plan::BitmapPlan,
};
use crate::request::QueryOrder;

pub struct BitmapExecutor<'a, M: BlobTable, S: MetaStore> {
    pages: BitmapPageReader<'a, M, S>,
}

impl<'a, M: BlobTable, S: MetaStore> BitmapExecutor<'a, M, S> {
    pub fn new(tables: &'a BitmapTables<M, S>) -> Self {
        Self {
            pages: BitmapPageReader::new(tables),
        }
    }

    pub fn execute<'b>(
        &'b self,
        plan: &'b BitmapPlan,
        limit: usize,
        order: QueryOrder,
    ) -> Pin<Box<dyn Future<Output = QueryResult<BitmapCandidateIdIntervals>> + 'b>> {
        Box::pin(async move {
            if limit == 0 {
                return Ok(BitmapCandidateIdIntervals::empty());
            }

            match plan {
                BitmapPlan::Empty => Ok(BitmapCandidateIdIntervals::empty()),
                BitmapPlan::Scan { start, end } => {
                    Ok(BitmapCandidateIdIntervals::new(*start, *end))
                }
                BitmapPlan::ReadBitmap(super::plan::BitmapReadPlan {
                    family,
                    filter,
                    pages,
                }) => {
                    let mut result = BitmapCandidateIdIntervals::empty();
                    for page in pages {
                        let mut bitmap = self.pages.read(*family, filter, page.page).await?;
                        bitmap.remove_range(..page.start.saturating_sub(page.page) as u32);
                        bitmap.remove_range(page.end_exclusive.saturating_sub(page.page) as u32..);
                        result.union(&BitmapPageSet {
                            pages: vec![(page.page, bitmap)],
                        });
                        if result
                            .ranges()
                            .map(|range| range.end - range.start)
                            .sum::<u64>()
                            >= limit as u64
                        {
                            break;
                        }
                    }
                    Ok(result)
                }
                BitmapPlan::Union(plans) => {
                    let mut result = BitmapCandidateIdIntervals::empty();
                    for plan in plans {
                        let child = self.execute(plan, limit, order).await?;
                        result.union_intervals(&child);
                        if result
                            .ranges()
                            .map(|range| range.end - range.start)
                            .sum::<u64>()
                            >= limit as u64
                        {
                            break;
                        }
                    }
                    Ok(result)
                }
                BitmapPlan::Intersect(plans) => {
                    let Some((first, rest)) = plans.split_first() else {
                        return Err(QueryError::InvalidRequest("empty bitmap conjunction"));
                    };
                    let mut result = self.execute(first, limit, order).await?;
                    for plan in rest {
                        let child = self.execute(plan, limit, order).await?;
                        result.intersect_intervals(&child);
                        if result.ranges().next().is_none() {
                            break;
                        }
                    }
                    Ok(result)
                }
            }
        })
    }
}
