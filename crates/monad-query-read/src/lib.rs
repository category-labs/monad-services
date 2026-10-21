// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

#![recursion_limit = "256"]

use alloy_primitives::{B256, U64};
use alloy_rlp::Decodable;
pub use monad_query_schema::family::StoredTraceQueryItem;
use monad_query_schema::{
    family::{Family, StoredBlockQueryItem},
    tables::{
        metadata::{
            block_hash::{MetadataBlockHashKey, MetadataBlockHashValue},
            family::MetadataFamilyKey,
        },
        QueryTables,
    },
};
use monad_query_types::{
    store::{blob::table::BlobTable, meta::MetaStore},
    QueryError, QueryResult,
};
use serde::Serialize;

pub use self::{
    config::{BlockRangeLimit, QueryLimits, QueryReaderConfig},
    query::family::{
        block::{BlockQueryFilter, BlockQueryItem, BlockQueryRequest},
        log::{LogQueryFilter, LogQueryItem, QueryLogsRequest},
        trace::{TraceQueryItem, TraceQueryRequest, TraceQueryRequestFilter},
        transaction::{QueryTransactionsRequest, TxQueryFilter, TxQueryItem},
        transfer::{TransferQueryFilter, TransferQueryItem, TransferQueryRequest},
        BlockRelations, TransactionRelations,
    },
    request::{QueryBound, QueryEnvelope, QueryOrder},
};
use self::{
    payload::PayloadReader,
    query::{
        family::{
            block::BlockQuery, log::LogQuery, trace::TraceQuery, transaction::TxQuery,
            transfer::TransferQuery,
        },
        QueryExecutionPage, QueryExecutor,
    },
};

mod bitmap;
mod config;
mod payload;
mod query;
mod request;
mod window;

pub trait QueryPageSchema {
    type Block;
    type Transaction;
    type Log;
    type Trace;
    type Transfer;
}

pub struct FullQueryPageSchema;

impl QueryPageSchema for FullQueryPageSchema {
    type Block = BlockQueryItem;
    type Transaction = TxQueryItem;
    type Log = LogQueryItem;
    type Trace = TraceQueryItem;
    type Transfer = TransferQueryItem;
}

#[serde_with::skip_serializing_none]
#[derive(Serialize)]
#[serde(bound(
    serialize = "S::Block: Serialize, S::Transaction: Serialize, S::Log: Serialize, S::Trace: Serialize, S::Transfer: Serialize"
))]
pub struct QueryPageData<S: QueryPageSchema> {
    pub blocks: Option<Box<[S::Block]>>,
    pub transactions: Option<Box<[S::Transaction]>>,
    pub logs: Option<Box<[S::Log]>>,
    pub traces: Option<Box<[S::Trace]>>,
    pub transfers: Option<Box<[S::Transfer]>>,
}

impl<S: QueryPageSchema> QueryPageData<S> {
    pub fn map<T: QueryPageSchema>(
        self,
        blocks: impl FnMut(S::Block) -> T::Block,
        transactions: impl FnMut(S::Transaction) -> T::Transaction,
        logs: impl FnMut(S::Log) -> T::Log,
        traces: impl FnMut(S::Trace) -> T::Trace,
        transfers: impl FnMut(S::Transfer) -> T::Transfer,
    ) -> QueryPageData<T> {
        QueryPageData {
            blocks: map_items(self.blocks, blocks),
            transactions: map_items(self.transactions, transactions),
            logs: map_items(self.logs, logs),
            traces: map_items(self.traces, traces),
            transfers: map_items(self.transfers, transfers),
        }
    }
}

fn map_items<T, U>(items: Option<Box<[T]>>, map: impl FnMut(T) -> U) -> Option<Box<[U]>> {
    items.map(|items| items.into_vec().into_iter().map(map).collect())
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
#[serde(bound(serialize = "QueryPageData<S>: Serialize"))]
pub struct QueryPage<S: QueryPageSchema = FullQueryPageSchema> {
    pub from_block: BlockRef,
    pub to_block: BlockRef,
    pub cursor_block: BlockRef,
    pub data: QueryPageData<S>,
}

impl<S: QueryPageSchema> QueryPage<S> {
    pub fn map<T: QueryPageSchema>(
        self,
        blocks: impl FnMut(S::Block) -> T::Block,
        transactions: impl FnMut(S::Transaction) -> T::Transaction,
        logs: impl FnMut(S::Log) -> T::Log,
        traces: impl FnMut(S::Trace) -> T::Trace,
        transfers: impl FnMut(S::Transfer) -> T::Transfer,
    ) -> QueryPage<T> {
        QueryPage {
            from_block: self.from_block,
            to_block: self.to_block,
            cursor_block: self.cursor_block,
            data: self.data.map(blocks, transactions, logs, traces, transfers),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockRef {
    pub number: U64,
    pub hash: B256,
    pub parent_hash: B256,
}

pub struct QueryReader<BM, MM, PM, PB, SM>
where
    BM: BlobTable,
    MM: MetaStore,
    PM: MetaStore,
    PB: BlobTable,
    SM: MetaStore,
{
    config: QueryReaderConfig,
    tables: QueryTables<BM, MM, PM, PB, SM>,
}

impl<BM, MM, PM, PB, SM> QueryReader<BM, MM, PM, PB, SM>
where
    BM: BlobTable,
    MM: MetaStore,
    PM: MetaStore,
    PB: BlobTable,
    SM: MetaStore,
{
    pub fn new(config: QueryReaderConfig, tables: QueryTables<BM, MM, PM, PB, SM>) -> Self {
        Self { config, tables }
    }

    pub async fn query_blocks(&self, request: BlockQueryRequest) -> QueryResult<QueryPage> {
        let BlockQueryRequest { envelope, filter } = request;

        let execution = QueryExecutor::new(&self.config, &self.tables, envelope)
            .await?
            .execute(&BlockQuery::new(filter))
            .await?;

        self.page(execution).await
    }

    pub async fn query_logs(&self, request: QueryLogsRequest) -> QueryResult<QueryPage> {
        let QueryLogsRequest {
            envelope,
            filter,
            relations,
        } = request;

        let execution = QueryExecutor::new(&self.config, &self.tables, envelope)
            .await?
            .execute(&LogQuery::new(filter, relations))
            .await?;

        self.page(execution).await
    }

    pub async fn query_transactions(
        &self,
        request: QueryTransactionsRequest,
    ) -> QueryResult<QueryPage> {
        let QueryTransactionsRequest {
            envelope,
            filter,
            relations,
        } = request;

        let execution = QueryExecutor::new(&self.config, &self.tables, envelope)
            .await?
            .execute(&TxQuery::new(filter, relations))
            .await?;

        self.page(execution).await
    }

    pub async fn query_transfers(&self, request: TransferQueryRequest) -> QueryResult<QueryPage> {
        let TransferQueryRequest {
            envelope,
            filter,
            relations,
        } = request;

        let execution = QueryExecutor::new(&self.config, &self.tables, envelope)
            .await?
            .execute(&TransferQuery::new(filter, relations))
            .await?;

        self.page(execution).await
    }

    pub async fn query_traces(&self, request: TraceQueryRequest) -> QueryResult<QueryPage> {
        let TraceQueryRequest {
            envelope,
            filter,
            relations,
        } = request;

        let execution = QueryExecutor::new(&self.config, &self.tables, envelope)
            .await?
            .execute(&TraceQuery::new(filter, relations))
            .await?;

        self.page(execution).await
    }

    async fn page(&self, execution: QueryExecutionPage) -> QueryResult<QueryPage> {
        let QueryExecutionPage {
            from_block,
            to_block,
            cursor_block,
            data,
        } = execution;

        let from_block = self.block_ref(from_block).await?;
        let to_block = self.block_ref(to_block).await?;
        let cursor_block = self.block_ref(cursor_block).await?;

        Ok(QueryPage {
            from_block,
            to_block,
            cursor_block,
            data,
        })
    }

    async fn block_ref(&self, block_height: u64) -> QueryResult<BlockRef> {
        let family = self
            .tables
            .metadata
            .family
            .get(&MetadataFamilyKey {
                family: Family::BlockHeader,
                block_height,
            })
            .await?
            .ok_or(QueryError::MissingData("missing block family metadata"))?;

        let MetadataBlockHashValue(hash) = self
            .tables
            .metadata
            .block_hash
            .get(&MetadataBlockHashKey { block_height })
            .await?
            .ok_or(QueryError::MissingData("missing block hash"))?;

        let payload = PayloadReader::new(&self.tables.payload)
            .read_many(Family::BlockHeader, &[family.first_primary_id.as_u64()])
            .await?
            .remove(&family.first_primary_id.as_u64())
            .ok_or(QueryError::MissingData("missing block header payload"))?;

        let header = StoredBlockQueryItem::decode(&mut payload.as_ref())
            .map_err(|_| QueryError::Decode("invalid block header"))?
            .header;

        Ok(BlockRef {
            number: U64::from(block_height),
            hash,
            parent_hash: header.parent_hash,
        })
    }
}
