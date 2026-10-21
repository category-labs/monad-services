use monad_query_schema::family::Family;
use std::{future::Future, pin::Pin};

use monad_query_schema::{bitmap::BitmapFilterValue, tables::{}};
use monad_query_types::{
    store::{
        blob::table::BlobTable,
        meta::{MetaScanOrder, MetaStore},
    },
    QueryResult,
};

use super::{stats::BitmapStatsReader, BitmapFilter};
use crate::request::QueryOrder;

#[derive(Debug, Clone)]
pub struct BitmapPagePlan {
    pub page: u64,
    pub start: u64,
    pub end_exclusive: u64,
}

#[derive(Debug, Clone)]
pub struct BitmapReadPlan {
    pub family: Family,
    pub filter: BitmapFilterValue,
    pub pages: Box<[BitmapPagePlan]>,
}

/// A physical bitmap execution plan. This is deliberately separate from `BitmapFilter`.
#[derive(Debug, Clone)]
pub enum BitmapPlan {
    Empty,
    Scan { start: u64, end: u64 },
    ReadBitmap(BitmapReadPlan),
    Union(Box<[BitmapPlan]>),
    Intersect(Box<[BitmapPlan]>),
}

#[derive(Debug, Clone)]
struct Planned {
    plan: BitmapPlan,
    cost: u64,
    estimate: u64,
}

pub struct BitmapPlanner;

impl BitmapPlanner {
    pub fn plan<'a, M: BlobTable, S: MetaStore>(
        stats: &'a BitmapStatsReader<'a, M, S>,
        family: Family,
        expression: &'a BitmapFilter,
        start: u64,
        end: u64,
        order: QueryOrder,
    ) -> Pin<Box<dyn Future<Output = QueryResult<BitmapPlan>> + 'a>> {
        Box::pin(async move {
            Ok(Self::build(stats, family, expression, start, end, order)
                .await?
                .plan)
        })
    }

    fn build<'a, M: BlobTable, S: MetaStore>(
        stats: &'a BitmapStatsReader<'a, M, S>,
        family: Family,
        expression: &'a BitmapFilter,
        start: u64,
        end: u64,
        order: QueryOrder,
    ) -> Pin<Box<dyn Future<Output = QueryResult<Planned>> + 'a>> {
        Box::pin(async move {
            if start >= end {
                return Ok(Planned {
                    plan: BitmapPlan::Empty,
                    cost: 0,
                    estimate: 0,
                });
            }
            let scan_cost = end - start;
            match expression {
                BitmapFilter::Unfiltered => Ok(Planned {
                    plan: BitmapPlan::Scan { start, end },
                    cost: scan_cost,
                    estimate: scan_cost,
                }),
                BitmapFilter::NoMatches => Ok(Planned {
                    plan: BitmapPlan::Empty,
                    cost: 0,
                    estimate: 0,
                }),
                BitmapFilter::Value(filter) => {
                    let pages = stats
                        .read(
                            family,
                            filter,
                            start,
                            end,
                            match order {
                                QueryOrder::Ascending => MetaScanOrder::Ascending,
                                QueryOrder::Descending => MetaScanOrder::Descending,
                            },
                        )
                        .await?;
                    let estimate: u64 = pages.iter().map(|page| u64::from(page.count)).sum();
                    let bitmap = BitmapPlan::ReadBitmap(BitmapReadPlan {
                        family,
                        filter: filter.clone(),
                        pages: pages
                            .into_vec()
                            .into_iter()
                            .map(|page| BitmapPagePlan {
                                page: page.page,
                                start: start.max(page.page),
                                end_exclusive: end.min(page.page + BitmapIndexValue::PAGE_SPAN),
                            })
                            .collect(),
                    });
                    let bitmap_cost = estimate.saturating_add(scan_cost / 16);
                    if bitmap_cost >= scan_cost {
                        Ok(Planned {
                            plan: BitmapPlan::Scan { start, end },
                            cost: scan_cost,
                            estimate: scan_cost,
                        })
                    } else {
                        Ok(Planned {
                            plan: bitmap,
                            cost: bitmap_cost,
                            estimate,
                        })
                    }
                }
                BitmapFilter::Any(expressions) => {
                    let mut plans = Vec::with_capacity(expressions.len());
                    for expression in expressions {
                        plans
                            .push(Self::build(stats, family, expression, start, end, order).await?);
                    }
                    if plans.iter().any(|planned| match &planned.plan {
                        BitmapPlan::Empty
                        | BitmapPlan::ReadBitmap(_)
                        | BitmapPlan::Union(_)
                        | BitmapPlan::Intersect(_) => false,
                        BitmapPlan::Scan { .. } => true,
                    }) {
                        return Ok(Planned {
                            plan: BitmapPlan::Scan { start, end },
                            cost: scan_cost,
                            estimate: scan_cost,
                        });
                    }
                    let estimate = plans
                        .iter()
                        .map(|planned| planned.estimate)
                        .sum::<u64>()
                        .min(scan_cost);
                    let cost = plans.iter().map(|planned| planned.cost).sum();
                    if cost >= scan_cost {
                        Ok(Planned {
                            plan: BitmapPlan::Scan { start, end },
                            cost: scan_cost,
                            estimate: scan_cost,
                        })
                    } else {
                        Ok(Planned {
                            plan: BitmapPlan::Union(
                                plans.into_iter().map(|planned| planned.plan).collect(),
                            ),
                            cost,
                            estimate,
                        })
                    }
                }
                BitmapFilter::All(expressions) => {
                    let mut plans = Vec::with_capacity(expressions.len());
                    for expression in expressions {
                        plans
                            .push(Self::build(stats, family, expression, start, end, order).await?);
                    }
                    plans.sort_unstable_by_key(|planned| planned.estimate);
                    let estimate = plans[0].estimate;
                    let cost = plans.iter().map(|planned| planned.cost).sum();
                    Ok(Planned {
                        plan: BitmapPlan::Intersect(
                            plans.into_iter().map(|planned| planned.plan).collect(),
                        ),
                        cost,
                        estimate,
                    })
                }
            }
        })
    }
}
