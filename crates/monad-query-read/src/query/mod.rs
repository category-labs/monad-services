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

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
    ops::Range,
};

use futures::StreamExt;
use itertools::{Either, Itertools};
use monad_query_schema::{
    family::Family,
    tables::{
        bitmap::{index::BitmapIndexValue, BitmapTables},
        metadata::{
            block_hash::{MetadataBlockHashKey, MetadataBlockHashValue},
            family::MetadataFamilyKey,
            family_block_index::MetadataFamilyBlockIndexKey,
            MetadataTables,
        },
        state::index::StateIndexKey,
        QueryTables,
    },
};
use monad_query_types::{
    store::{
        blob::table::BlobTable,
        meta::{MetaScanOrder, MetaStore},
    },
    QueryError, QueryResult,
};

use self::family::{
    block::BlockQuery, transaction::TxQuery, BlockRelations, QueryFamily, RelationKey,
};
use crate::{
    bitmap::BitmapCandidateStream,
    config::{BlockRangeLimit, QueryLimits, QueryReaderConfig},
    payload::PayloadReader,
    request::{QueryBound, QueryEnvelope, QueryOrder},
    BlockQueryFilter, FullQueryPageSchema, QueryPageData, TxQueryFilter,
};

pub mod family;

pub(crate) struct QueryExecutionPage {
    pub(crate) from_block: u64,
    pub(crate) to_block: u64,
    pub(crate) cursor_block: u64,

    pub(crate) data: QueryPageData<FullQueryPageSchema>,
}

pub(crate) struct QueryExecutor<'a, BM, MM, PM, PB>
where
    BM: BlobTable,
    MM: MetaStore,
    PM: MetaStore,
    PB: BlobTable,
{
    window_executor: QueryWindowExecutor<'a, BM, MM, PM, PB>,
    from_block: u64,
    to_block: u64,
    order: QueryOrder,
    candidate_batch_load_max_concurrency: NonZeroUsize,
    candidate_batch_max_size: NonZeroUsize,
    target: NonZeroUsize,
}

impl<'a, BM, MM, PM, PB> QueryExecutor<'a, BM, MM, PM, PB>
where
    BM: BlobTable,
    MM: MetaStore,
    PM: MetaStore,
    PB: BlobTable,
{
    pub(crate) async fn new<SM: MetaStore>(
        config: &'a QueryReaderConfig,
        tables: &'a QueryTables<BM, MM, PM, PB, SM>,
        envelope: QueryEnvelope,
    ) -> QueryResult<Self> {
        let QueryReaderConfig {
            limits:
                QueryLimits {
                    max_target,
                    max_block_range,
                },
            candidate_batch_load_max_concurrency,
            candidate_batch_max_size,
        } = config;

        let QueryTables {
            bitmap,
            metadata,
            payload,
            state,
        } = tables;

        tracing::debug!(table = "state_index", "dispatch metadata read");
        let state = state
            .index
            .get(&StateIndexKey)
            .await?
            .ok_or(QueryError::MissingData("missing state index"))?;

        let QueryEnvelope {
            from_block,
            to_block,
            order,
            target,
        } = envelope;

        let indexed_start = state.indexed_start.ok_or(QueryError::NoIndexedData)?;
        let latest = state.next_block_id.saturating_sub(1);
        let resolve_bound = |bound, default| match bound {
            Some(QueryBound::Earliest) => 0,
            Some(QueryBound::Latest) => latest,
            Some(QueryBound::Number(number)) => number,
            None => default,
        };
        let (from_default, to_default) = match order {
            QueryOrder::Ascending => (indexed_start, latest),
            QueryOrder::Descending => (latest, indexed_start),
        };
        let from = resolve_bound(from_block, from_default);
        let to = resolve_bound(to_block, to_default);

        let (from, to) = match order {
            QueryOrder::Ascending => (from, to),
            QueryOrder::Descending => (to, from),
        };

        if from > to {
            return Err(QueryError::RangeInverted { from, to });
        }

        if from < indexed_start || to >= state.next_block_id {
            return Err(QueryError::ResourceNotFound);
        }

        let target = target.map_or(*max_target, |target| target.min(*max_target));

        match max_block_range {
            BlockRangeLimit::Unlimited => {}
            BlockRangeLimit::Bounded(max_block_range_len) => {
                if to - from + 1 > max_block_range_len.get() {
                    return Err(QueryError::InvalidRequest(
                        "block range exceeds configured limit",
                    ));
                }
            }
        }

        Ok(Self {
            window_executor: QueryWindowExecutor {
                bitmap: &bitmap,
                metadata,
                payload: PayloadReader::new(payload),
                order,
            },
            from_block: from,
            to_block: to,
            order,
            candidate_batch_load_max_concurrency: *candidate_batch_load_max_concurrency,
            candidate_batch_max_size: *candidate_batch_max_size,
            target,
        })
    }

    pub(crate) async fn execute<Q>(self, query: &Q) -> QueryResult<QueryExecutionPage>
    where
        Q: QueryFamily,
    {
        let (page_from, page_to) = match self.order {
            QueryOrder::Ascending => (self.from_block, self.to_block),
            QueryOrder::Descending => (self.to_block, self.from_block),
        };

        let (from, to) = self
            .window_executor
            .primary_id_range(Q::family(), self.from_block, self.to_block)
            .await?;

        // TODO(andr-dev): If from==to, should we do a one-block scan?
        if from >= to {
            let cursor_block = match self.order {
                QueryOrder::Ascending => self.from_block,
                QueryOrder::Descending => self.to_block,
            };
            return Ok(QueryExecutionPage {
                from_block: page_from,
                to_block: page_to,
                cursor_block,
                data: {
                    let mut data = QueryPageData {
                        blocks: None,
                        transactions: None,
                        logs: None,
                        traces: None,
                        transfers: None,
                    };
                    query.update_page_data(&mut data, Box::new([]))?;
                    data
                },
            });
        }

        let candidates = BitmapCandidateStream::new(
            self.window_executor.bitmap,
            Q::family(),
            query.bitmap_filter(),
            from,
            to,
            self.order,
            u64::try_from(self.candidate_batch_max_size.get()).expect("usize fits in u64"),
        );

        let mut loaded = candidates
            .map(|result| async {
                let ranges = result?;
                let block_candidates = self
                    .window_executor
                    .resolve_ranges(Q::family(), ranges)
                    .await?;
                self.window_executor
                    .load_candidates(query, block_candidates, self.target)
                    .await
            })
            .buffered(self.candidate_batch_load_max_concurrency.get());

        let mut items = Vec::new();
        let target_index = self.target.get() - 1;

        while let Some(batch) = loaded.next().await.transpose()? {
            items.extend(batch.items);

            let Some(target_item) = items.get(target_index) else {
                continue;
            };
            let target_block = Q::block_number(target_item);

            if batch
                .last_candidate_block_number
                .is_some_and(|block| block != target_block)
            {
                let target_tail_len = items[target_index..]
                    .partition_point(|item| Q::block_number(item) == target_block);

                items.truncate(target_index + target_tail_len);

                break;
            }
        }

        let (related_blocks, related_transactions): (BTreeSet<_>, BTreeSet<_>) = items
            .iter()
            .flat_map(|item| query.relation_keys(item))
            .partition_map(|key| match key {
                RelationKey::Block(number) => Either::Left(number),
                RelationKey::Transaction { block, index } => Either::Right((block, index)),
            });

        let related_block_candidates = related_blocks
            .into_iter()
            .map(|block| BlockCandidate {
                block,
                primary_id: block,
            })
            .collect::<BTreeSet<_>>();

        let related_transaction_blocks = related_transactions
            .iter()
            .map(|(block, _)| *block)
            .collect::<BTreeSet<_>>();

        let related_transaction_metadata = self
            .window_executor
            .metadata
            .family
            .get_many(
                &related_transaction_blocks
                    .iter()
                    .map(|&block_height| MetadataFamilyKey {
                        family: Family::Transaction,
                        block_height,
                    })
                    .collect::<Box<[_]>>(),
            )
            .await?;

        let related_transaction_metadata = related_transaction_blocks
            .into_iter()
            .zip(related_transaction_metadata)
            .collect::<BTreeMap<_, _>>();

        let related_transaction_candidates = related_transactions
            .into_iter()
            .map(|(block, index)| {
                let metadata = related_transaction_metadata
                    .get(&block)
                    .and_then(Option::as_ref)
                    .ok_or(QueryError::MissingData(
                        "missing transaction block metadata",
                    ))?;

                if index >= metadata.count {
                    return Err(QueryError::InvalidRequest("transaction index out of range"));
                }

                Ok(BlockCandidate {
                    block,
                    primary_id: metadata.first_primary_id.as_u64() + u64::from(index),
                })
            })
            .collect::<QueryResult<BTreeSet<_>>>()?;

        let cursor_block = items
            .last()
            .map_or(self.from_block, |item| Q::block_number(item));

        let mut data = QueryPageData {
            blocks: None,
            transactions: None,
            logs: None,
            traces: None,
            transfers: None,
        };

        query.update_page_data(&mut data, items.into_boxed_slice())?;

        if !related_block_candidates.is_empty() {
            if data.blocks.is_some() {
                return Err(QueryError::Internal("query page data field already set"));
            }

            let blocks = self
                .window_executor
                .load_candidates(
                    &BlockQuery::new(BlockQueryFilter::default()),
                    related_block_candidates,
                    NonZeroUsize::MAX,
                )
                .await?;

            data.blocks = Some(blocks.items);
        }

        if !related_transaction_candidates.is_empty() {
            if data.transactions.is_some() {
                return Err(QueryError::Internal("query page data field already set"));
            }

            let transactions = self
                .window_executor
                .load_candidates(
                    &TxQuery::new(TxQueryFilter::default(), BlockRelations::default()),
                    related_transaction_candidates,
                    NonZeroUsize::MAX,
                )
                .await?;

            data.transactions = Some(transactions.items);
        }

        Ok(QueryExecutionPage {
            from_block: page_from,
            to_block: page_to,
            cursor_block,
            data,
        })
    }
}

struct QueryWindowExecutor<'a, BM, MM, PM, PB>
where
    BM: BlobTable,
    MM: MetaStore,
    PM: MetaStore,
    PB: BlobTable,
{
    bitmap: &'a BitmapTables<BM, MM>,
    metadata: &'a MetadataTables<MM>,
    payload: PayloadReader<'a, PM, PB>,
    order: QueryOrder,
}

impl<'a, BM, MM, PM, PB> QueryWindowExecutor<'a, BM, MM, PM, PB>
where
    BM: BlobTable,
    MM: MetaStore,
    PM: MetaStore,
    PB: BlobTable,
{
    async fn primary_id_range(
        &self,
        family: Family,
        from_block: u64,
        to_block: u64,
    ) -> QueryResult<(u64, u64)> {
        tracing::debug!(
            ?family,
            block = from_block,
            table = "metadata_family",
            "dispatch metadata read"
        );
        let from = self
            .metadata
            .family
            .get(&MetadataFamilyKey {
                family,
                block_height: from_block,
            })
            .await?
            .ok_or(QueryError::MissingData("missing family metadata"))?;
        tracing::debug!(
            ?family,
            block = to_block,
            table = "metadata_family",
            "dispatch metadata read"
        );
        let to = self
            .metadata
            .family
            .get(&MetadataFamilyKey {
                family,
                block_height: to_block,
            })
            .await?
            .ok_or(QueryError::MissingData("missing family metadata"))?;
        Ok((
            from.first_primary_id.as_u64(),
            to.first_primary_id.as_u64() + u64::from(to.count),
        ))
    }

    async fn load_candidates<Q>(
        &self,
        query: &Q,
        block_candidates: BTreeSet<BlockCandidate>,
        target: NonZeroUsize,
    ) -> QueryResult<LoadedCandidates<Q::Item>>
    where
        Q: QueryFamily,
    {
        let last_candidate_block_number = match self.order {
            QueryOrder::Ascending => block_candidates.last(),
            QueryOrder::Descending => block_candidates.first(),
        }
        .map(|candidate| candidate.block);

        let mut items = Vec::new();
        let family = Q::family();
        let ids = block_candidates
            .iter()
            .map(|candidate| candidate.primary_id)
            .collect::<Box<[_]>>();
        let rows = self.payload.read_many(family, &ids).await?;

        let payload_items = block_candidates
            .iter()
            .map(|candidate| {
                let bytes = rows
                    .get(&candidate.primary_id)
                    .ok_or(QueryError::MissingData("missing payload"))?;
                Ok::<_, QueryError>((candidate, query.decode(&mut bytes.as_ref())?))
            })
            .collect::<QueryResult<Vec<_>>>()?;

        let block_numbers = block_candidates
            .iter()
            .map(|candidate| candidate.block)
            .collect::<BTreeSet<_>>();
        let block_contexts = futures::future::try_join_all(block_numbers.into_iter().map(
            |block_height| async move {
                let family_key = MetadataFamilyKey {
                    family,
                    block_height,
                };
                let hash_key = MetadataBlockHashKey { block_height };
                tracing::debug!(
                    ?family,
                    block = block_height,
                    tables = "metadata_family,metadata_block_hash",
                    "dispatch block context reads"
                );
                let (family_metadata, block_hash) = futures::try_join!(
                    self.metadata.family.get(&family_key),
                    self.metadata.block_hash.get(&hash_key),
                )?;
                let family_metadata =
                    family_metadata.ok_or(QueryError::MissingData("missing family metadata"))?;
                let MetadataBlockHashValue(block_hash) =
                    block_hash.ok_or(QueryError::MissingData("missing block hash"))?;
                Ok::<_, QueryError>((block_height, (block_hash, family_metadata)))
            },
        ))
        .await?
        .into_iter()
        .collect::<BTreeMap<_, _>>();

        let mut target_block = None;

        for (candidate, payload_item) in self.order.iterate(payload_items) {
            if target_block.is_some_and(|block| candidate.block != block) {
                break;
            }

            let (block_hash, family_metadata) = block_contexts
                .get(&candidate.block)
                .ok_or(QueryError::MissingData("missing block context"))?;

            let item = query.item(
                payload_item,
                *block_hash,
                candidate.block,
                family_metadata,
                candidate.primary_id,
            )?;

            if query.matches(&item) {
                items.push(item);
            }

            if target_block.is_none() && items.len() >= target.get() {
                target_block = Some(candidate.block);
            }
        }

        Ok(LoadedCandidates {
            items: items.into_boxed_slice(),
            last_candidate_block_number,
        })
    }

    async fn resolve_ranges(
        &self,
        family: Family,
        ranges: Box<[Range<u64>]>,
    ) -> QueryResult<BTreeSet<BlockCandidate>> {
        let mut ranges = ranges.into_vec();
        ranges.sort_unstable_by_key(|range| range.start);
        if ranges.is_empty() {
            return Ok(BTreeSet::new());
        }
        if ranges.len() == 1 {
            return self.resolve_by_scan(family, &ranges).await;
        }
        let groups = Self::group_candidate_ranges(ranges.into_iter());

        let mut resolved = BTreeSet::new();
        for group in groups {
            resolved.extend(self.resolve_group(family, &group).await?);
        }
        Ok(resolved)
    }

    async fn resolve_group(
        &self,
        family: Family,
        ranges: &[Range<u64>],
    ) -> QueryResult<BTreeSet<BlockCandidate>> {
        const MAX_INDEX_SCAN_CANDIDATES: usize = 32;
        let candidate_count = Self::group_candidate_count(ranges);
        if candidate_count <= MAX_INDEX_SCAN_CANDIDATES {
            tracing::debug!(
                ?family,
                candidates = candidate_count,
                "dispatch sparse index seeks"
            );
            return self.resolve_by_seek(family, ranges).await;
        }
        self.resolve_by_scan(family, ranges).await
    }

    async fn resolve_by_seek(
        &self,
        family: Family,
        ranges: &[Range<u64>],
    ) -> QueryResult<BTreeSet<BlockCandidate>> {
        let mut resolved = BTreeSet::new();
        for range in ranges {
            for primary_id in range.clone() {
                let (_, value) = self
                    .metadata
                    .family_block_index
                    .seek_le(&MetadataFamilyBlockIndexKey {
                        family,
                        first_primary_id: primary_id,
                    })
                    .await?
                    .ok_or(QueryError::MissingData("missing family block index"))?;
                resolved.insert(BlockCandidate {
                    block: value.block_height,
                    primary_id,
                });
            }
        }
        Ok(resolved)
    }

    async fn resolve_by_scan(
        &self,
        family: Family,
        ranges: &[Range<u64>],
    ) -> QueryResult<BTreeSet<BlockCandidate>> {
        let start = ranges
            .first()
            .expect("scan resolution requires a range")
            .start;
        let end = ranges.last().expect("scan resolution requires a range").end;
        let (start_key, _) = self
            .metadata
            .family_block_index
            .seek_le(&MetadataFamilyBlockIndexKey {
                family,
                first_primary_id: start,
            })
            .await?
            .ok_or(QueryError::MissingData("missing family block index"))?;
        let entries = self
            .metadata
            .family_block_index
            .scan(
                &start_key,
                &MetadataFamilyBlockIndexKey {
                    family,
                    first_primary_id: end - 1,
                },
                MetaScanOrder::Ascending,
                None,
            )
            .await?;
        let mut resolved = BTreeSet::new();
        for range in ranges {
            for primary_id in range.clone() {
                let index = entries
                    .partition_point(|(key, _)| key.first_primary_id <= primary_id)
                    .checked_sub(1)
                    .ok_or(QueryError::MissingData("missing family block index"))?;
                resolved.insert(BlockCandidate {
                    block: entries[index].1.block_height,
                    primary_id,
                });
            }
        }
        Ok(resolved)
    }

    fn group_candidate_ranges(ranges: impl Iterator<Item = Range<u64>>) -> Vec<Vec<Range<u64>>> {
        const MAX_INDEX_SCAN_SPAN: u64 = BitmapIndexValue::PAGE_SPAN;
        let mut groups: Vec<Vec<Range<u64>>> = Vec::new();
        for range in ranges {
            if groups
                .last()
                .is_none_or(|group| range.end.saturating_sub(group[0].start) > MAX_INDEX_SCAN_SPAN)
            {
                groups.push(Vec::new());
            }
            groups
                .last_mut()
                .expect("candidate group was just created")
                .push(range);
        }
        groups
    }

    fn group_candidate_count(ranges: &[Range<u64>]) -> usize {
        ranges
            .iter()
            .map(|range| (range.end - range.start) as usize)
            .sum()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct BlockCandidate {
    block: u64,
    primary_id: u64,
}

struct LoadedCandidates<T> {
    items: Box<[T]>,
    last_candidate_block_number: Option<u64>,
}
