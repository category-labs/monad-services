// Copyright (C) 2025 Category Labs, Inc.

use std::{
    collections::VecDeque,
    ops::Range,
    pin::Pin,
    task::{Context as TaskContext, Poll},
};

use monad_query_types::store::{blob::table::BlobTable, meta::MetaStore};

use super::{BitmapCandidateStreamContext, BitmapCandidateStreamKind, BitmapCandidateSubStream};
use crate::{bitmap::BitmapFilter, request::QueryOrder, QueryResult};

pub(crate) struct BitmapCandidateAllStream<'a, BM, SM>
where
    BM: BlobTable,
    SM: MetaStore,
{
    children: Vec<Pin<Box<BitmapCandidateStreamKind<'a, BM, SM>>>>,
    heads: Vec<VecDeque<Range<u64>>>,
    range_emitted: bool,
}

impl<'a, BM, SM> BitmapCandidateAllStream<'a, BM, SM>
where
    BM: BlobTable,
    SM: MetaStore,
{
    pub(crate) fn new(filters: Box<[BitmapFilter]>) -> Self {
        Self {
            heads: (0..filters.len()).map(|_| VecDeque::new()).collect(),
            children: filters
                .into_vec()
                .into_iter()
                .map(|filter| Box::pin(BitmapCandidateStreamKind::from_filter(filter)))
                .collect(),
            range_emitted: false,
        }
    }
}

impl<'a, BM, SM> BitmapCandidateSubStream<'a, BM, SM> for BitmapCandidateAllStream<'a, BM, SM>
where
    BM: BlobTable,
    SM: MetaStore,
{
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        context: &BitmapCandidateStreamContext<'a, BM, SM>,
    ) -> Poll<Option<QueryResult<Box<[Range<u64>]>>>> {
        let BitmapCandidateStreamContext {
            tables: _tables,
            family: _family,
            from,
            to,
            order,
        } = *context;
        if self.children.is_empty() {
            if self.range_emitted || from >= to {
                return Poll::Ready(None);
            }
            self.range_emitted = true;
            return Poll::Ready(Some(Ok(Box::new([from..to]))));
        }

        loop {
            for index in 0..self.children.len() {
                if !self.heads[index].is_empty() {
                    continue;
                }
                match self.children[index].as_mut().poll_next(cx, context) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(None) => return Poll::Ready(None),
                    Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
                    Poll::Ready(Some(Ok(batch))) => self.heads[index].extend(batch.into_vec()),
                }
            }

            let start = self
                .heads
                .iter()
                .filter_map(|head| head.front())
                .map(|r| r.start)
                .max()
                .unwrap();
            let end = self
                .heads
                .iter()
                .filter_map(|head| head.front())
                .map(|r| r.end)
                .min()
                .unwrap();
            let boundary = match order {
                QueryOrder::Ascending => end,
                QueryOrder::Descending => start,
            };
            for head in &mut self.heads {
                if head.front().is_some_and(|range| match order {
                    QueryOrder::Ascending => range.end == boundary,
                    QueryOrder::Descending => range.start == boundary,
                }) {
                    head.pop_front();
                }
            }
            if start < end {
                return Poll::Ready(Some(Ok(Box::new([start..end]))));
            }
        }
    }
}
