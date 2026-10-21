// Copyright (C) 2025 Category Labs, Inc.
use std::{
    collections::VecDeque,
    ops::Range,
    pin::Pin,
    task::{Context as TaskContext, Poll},
};

use futures::Stream;
use monad_query_schema::{family::Family, tables::bitmap::BitmapTables};
use monad_query_types::store::{blob::table::BlobTable, meta::MetaStore};
use pin_project_lite::pin_project;

use super::BitmapFilter;
use crate::{request::QueryOrder, QueryResult};

mod all;
mod any;
mod value;

use self::value::BitmapCandidateValueStream;
pub(crate) use self::{all::BitmapCandidateAllStream, any::BitmapCandidateAnyStream};

#[derive(Clone, Copy)]
pub(crate) struct BitmapCandidateStreamContext<'a, BM, SM>
where
    BM: BlobTable,
    SM: MetaStore,
{
    pub(crate) tables: &'a BitmapTables<BM, SM>,
    pub(crate) family: Family,
    pub(crate) from: u64,
    pub(crate) to: u64,
    pub(crate) order: QueryOrder,
}

pub(crate) trait BitmapCandidateSubStream<'a, BM, SM>
where
    BM: BlobTable,
    SM: MetaStore,
{
    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        context: &BitmapCandidateStreamContext<'a, BM, SM>,
    ) -> Poll<Option<QueryResult<Box<[Range<u64>]>>>>;
}

pin_project! {
    pub(crate) struct BitmapCandidateStream<'a, BM, SM>
    where
        BM: BlobTable,
        SM: MetaStore,
    {
        context: BitmapCandidateStreamContext<'a, BM, SM>,
        max_batch_size: u64,
        pending: VecDeque<Range<u64>>,
        #[pin]
        kind: BitmapCandidateStreamKind<'a, BM, SM>,
    }
}

pin_project! {
    #[project = BitmapCandidateStreamKindProj]
    pub(crate) enum BitmapCandidateStreamKind<'a, BM, SM>
    where
        BM: BlobTable,
        SM: MetaStore,
    {
        Value { #[pin] stream: BitmapCandidateValueStream<'a, BM, SM> },
        Any { #[pin] stream: BitmapCandidateAnyStream<'a, BM, SM> },
        All { #[pin] stream: BitmapCandidateAllStream<'a, BM, SM> },
    }
}

impl<'a, BM, SM> BitmapCandidateStream<'a, BM, SM>
where
    BM: BlobTable,
    SM: MetaStore,
{
    pub(crate) fn new(
        tables: &'a BitmapTables<BM, SM>,
        family: Family,
        filter: BitmapFilter,
        from: u64,
        to: u64,
        order: QueryOrder,
        max_batch_size: u64,
    ) -> Self {
        let context = BitmapCandidateStreamContext {
            tables,
            family,
            from,
            to,
            order,
        };
        Self {
            kind: BitmapCandidateStreamKind::from_filter(filter),
            context,
            max_batch_size,
            pending: VecDeque::new(),
        }
    }
}

impl<'a, BM, SM> BitmapCandidateStreamKind<'a, BM, SM>
where
    BM: BlobTable,
    SM: MetaStore,
{
    fn from_filter(filter: BitmapFilter) -> Self {
        match filter {
            BitmapFilter::Value(filter) => Self::Value {
                stream: BitmapCandidateValueStream::new(filter),
            },
            BitmapFilter::Any(filters) => Self::Any {
                stream: BitmapCandidateAnyStream::new(filters),
            },
            BitmapFilter::All(filters) => Self::All {
                stream: BitmapCandidateAllStream::new(filters),
            },
        }
    }
}

impl<'a, BM, SM> BitmapCandidateSubStream<'a, BM, SM> for BitmapCandidateStreamKind<'a, BM, SM>
where
    BM: BlobTable,
    SM: MetaStore,
{
    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        context: &BitmapCandidateStreamContext<'a, BM, SM>,
    ) -> Poll<Option<QueryResult<Box<[Range<u64>]>>>> {
        match self.project() {
            BitmapCandidateStreamKindProj::Value { stream } => stream.poll_next(cx, context),
            BitmapCandidateStreamKindProj::Any { stream } => stream.poll_next(cx, context),
            BitmapCandidateStreamKindProj::All { stream } => stream.poll_next(cx, context),
        }
    }
}

impl<'a, BM, SM> Stream for BitmapCandidateStream<'a, BM, SM>
where
    BM: BlobTable,
    SM: MetaStore,
{
    type Item = QueryResult<Box<[Range<u64>]>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            if let Some(range) = this.pending.pop_front() {
                let (batch, remainder) = match this.context.order {
                    QueryOrder::Ascending => {
                        let end = range
                            .start
                            .saturating_add(*this.max_batch_size)
                            .min(range.end);
                        (
                            range.start..end,
                            (end < range.end).then_some(end..range.end),
                        )
                    }
                    QueryOrder::Descending => {
                        let start = range
                            .end
                            .saturating_sub(*this.max_batch_size)
                            .max(range.start);
                        (
                            start..range.end,
                            (range.start < start).then_some(range.start..start),
                        )
                    }
                };
                if let Some(remainder) = remainder {
                    this.pending.push_front(remainder);
                }
                return Poll::Ready(Some(Ok(Box::new([batch]))));
            }

            match this.kind.as_mut().poll_next(cx, &this.context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
                Poll::Ready(Some(Ok(ranges))) => this.pending.extend(ranges.into_vec()),
            }
        }
    }
}
