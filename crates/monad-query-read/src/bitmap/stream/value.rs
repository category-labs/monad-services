// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use std::{
    marker::PhantomData,
    ops::Range,
    pin::Pin,
    task::{Context as TaskContext, Poll},
};

use futures::{future::LocalBoxFuture, Future};
use monad_query_schema::{
    bitmap::{BitmapFilterKey, BitmapFilterValue},
    tables::bitmap::index::{BitmapIndexKey, BitmapIndexValue},
};
use monad_query_types::store::{blob::table::BlobTable, meta::MetaStore};
use pin_project_lite::pin_project;

use super::{BitmapCandidateStreamContext, BitmapCandidateSubStream};
use crate::{request::QueryOrder, QueryResult};

pin_project! {
    #[project = BitmapCandidateValueStreamProj]
    pub(crate) struct BitmapCandidateValueStream<'a, BM, SM>
    where
        BM: BlobTable,
        SM: MetaStore,
    {
        filter: BitmapFilterValue,
        next_page: Option<u64>,
        #[pin]
        pending: Option<LocalBoxFuture<'a, QueryResult<Option<BitmapIndexValue>>>>,
        done: bool,
        _backends: PhantomData<(&'a BM, SM)>,
    }
}

impl<'a, BM, SM> BitmapCandidateValueStream<'a, BM, SM>
where
    BM: BlobTable,
    SM: MetaStore,
{
    pub(crate) fn new(filter: BitmapFilterValue) -> Self {
        Self {
            filter,
            next_page: None,
            pending: None,
            done: false,
            _backends: PhantomData,
        }
    }

    fn page_ranges(
        value: BitmapIndexValue,
        page: u64,
        from: u64,
        to: u64,
        order: QueryOrder,
    ) -> Box<[Range<u64>]> {
        let mut ranges = Vec::new();
        let mut start = None;
        let mut previous = 0;

        for local in value.bitmap.iter() {
            let id = page + u64::from(local);
            if id < from || id >= to {
                continue;
            }
            match start {
                None => start = Some(id),
                Some(begin) if id > previous + 1 => {
                    ranges.push(begin..previous + 1);
                    start = Some(id);
                }
                Some(_) => {}
            }
            previous = id;
        }

        if let Some(begin) = start {
            ranges.push(begin..previous + 1);
        }
        if order == QueryOrder::Descending {
            ranges.reverse();
        }
        ranges.into_boxed_slice()
    }
}

impl<'a, BM, SM> BitmapCandidateSubStream<'a, BM, SM> for BitmapCandidateValueStream<'a, BM, SM>
where
    BM: BlobTable,
    SM: MetaStore,
{
    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        context: &BitmapCandidateStreamContext<'a, BM, SM>,
    ) -> Poll<Option<QueryResult<Box<[Range<u64>]>>>> {
        let BitmapCandidateValueStreamProj {
            filter,
            next_page,
            mut pending,
            done,
            _backends: _,
        } = self.project();

        let BitmapCandidateStreamContext {
            tables,
            family,
            from,
            to,
            order,
        } = *context;

        loop {
            if *done {
                return Poll::Ready(None);
            }

            if from >= to {
                *done = true;
                return Poll::Ready(None);
            }

            let first_page = from / BitmapIndexValue::PAGE_SPAN * BitmapIndexValue::PAGE_SPAN;
            let final_page = (to - 1) / BitmapIndexValue::PAGE_SPAN * BitmapIndexValue::PAGE_SPAN;

            if next_page.is_none() {
                *next_page = Some(match order {
                    QueryOrder::Ascending => first_page,
                    QueryOrder::Descending => final_page,
                });
            }

            if pending.is_none() {
                let page = next_page.as_ref().copied().unwrap();

                let filter = filter.clone();

                pending.set(Some(Box::pin(async move {
                    tables
                        .index
                        .get(&BitmapIndexKey {
                            family,
                            filter: BitmapFilterKey { value: filter },
                            page,
                        })
                        .await
                })));
            }

            let pending_fut = pending
                .as_mut()
                .as_pin_mut()
                .expect("bitmap page future initialized");

            let value = match pending_fut.poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(value) => value,
            };

            pending.set(None);
            let value = value?;

            let page = next_page.as_ref().copied().unwrap();

            let ranges = value.map_or_else(
                || Box::<[Range<u64>]>::default(),
                |value| Self::page_ranges(value, page, from, to, order),
            );

            *next_page = match order {
                QueryOrder::Ascending => page
                    .checked_add(BitmapIndexValue::PAGE_SPAN)
                    .filter(|next| *next <= final_page),
                QueryOrder::Descending => {
                    page.checked_sub(BitmapIndexValue::PAGE_SPAN)
                        .filter(|next| {
                            *next
                                >= from / BitmapIndexValue::PAGE_SPAN * BitmapIndexValue::PAGE_SPAN
                        })
                }
            };

            if next_page.is_none() {
                *done = true;
            }

            if !ranges.is_empty() {
                return Poll::Ready(Some(Ok(ranges)));
            }
        }
    }
}
