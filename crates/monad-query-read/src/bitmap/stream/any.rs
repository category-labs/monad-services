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

pub(crate) struct BitmapCandidateAnyStream<'a, BM, SM>
where
    BM: BlobTable,
    SM: MetaStore,
{
    children: Box<[BitmapCandidateAnyStreamChild<'a, BM, SM>]>,
}

impl<'a, BM, SM> BitmapCandidateAnyStream<'a, BM, SM>
where
    BM: BlobTable,
    SM: MetaStore,
{
    pub(crate) fn new(filters: Box<[BitmapFilter]>) -> Self {
        Self {
            children: filters
                .into_vec()
                .into_iter()
                .map(|filter| BitmapCandidateAnyStreamChild {
                    stream: Box::pin(BitmapCandidateStreamKind::from_filter(filter)),
                    head: VecDeque::default(),
                    exhausted: false,
                })
                .collect(),
        }
    }
}

impl<'a, BM, SM> BitmapCandidateSubStream<'a, BM, SM> for BitmapCandidateAnyStream<'a, BM, SM>
where
    BM: BlobTable,
    SM: MetaStore,
{
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        context: &BitmapCandidateStreamContext<'a, BM, SM>,
    ) -> Poll<Option<QueryResult<Box<[Range<u64>]>>>> {
        let Self { children } = self.as_mut().get_mut();

        let BitmapCandidateStreamContext {
            tables: _tables,
            family: _family,
            from: _from,
            to: _to,
            order,
        } = *context;

        for BitmapCandidateAnyStreamChild {
            stream,
            head,
            exhausted,
        } in children.iter_mut()
        {
            if *exhausted || !head.is_empty() {
                continue;
            }

            match stream.as_mut().poll_next(cx, context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => *exhausted = true,
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
                Poll::Ready(Some(Ok(batch))) => head.extend(batch.into_vec()),
            }
        }

        let Some(index) = children
            .iter()
            .enumerate()
            .filter_map(|(index, child)| child.head.front().map(|range| (index, range)))
            .min_by_key(|(_, range)| match order {
                QueryOrder::Ascending => range.start,
                QueryOrder::Descending => u64::MAX - range.end,
            })
            .map(|(index, _)| index)
        else {
            return Poll::Ready(None);
        };

        let mut result = children[index].head.pop_front().expect("selected head");

        loop {
            let mut merged = false;

            for BitmapCandidateAnyStreamChild {
                stream: _,
                head,
                exhausted: _,
            } in self.children.iter_mut()
            {
                loop {
                    let Some(range) = head.front() else { break };

                    let overlaps = match order {
                        QueryOrder::Ascending => range.start <= result.end,
                        QueryOrder::Descending => range.end >= result.start,
                    };

                    if !overlaps {
                        break;
                    }

                    result.start = result.start.min(range.start);
                    result.end = result.end.max(range.end);
                    head.pop_front();
                    merged = true;
                }
            }

            if !merged {
                break;
            }
        }

        Poll::Ready(Some(Ok(Box::new([result]))))
    }
}

struct BitmapCandidateAnyStreamChild<'a, BM, SM>
where
    BM: BlobTable,
    SM: MetaStore,
{
    stream: Pin<Box<BitmapCandidateStreamKind<'a, BM, SM>>>,
    head: VecDeque<Range<u64>>,
    exhausted: bool,
}
