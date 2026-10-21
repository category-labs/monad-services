// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use monad_query_read::{
    BlockQueryRequest, BlockRelations, QueryLogsRequest, QueryTransactionsRequest,
    TraceQueryRequest, TransactionRelations, TransferQueryRequest,
};
use monad_query_types::QueryError;
use serde_json::value::RawValue;

use crate::{
    data::query::{
        self, BlockFilter, LogFilter, QueryMethod, QueryRequest, TraceFilter, TransferFilter,
        TxFilter,
    },
    query_reader::ConfiguredQueryReader,
    types::{
        eth_json::serialize_result,
        jsonrpc::{JsonRpcError, JsonRpcResult},
    },
};

pub fn chain_data_error(error: QueryError) -> JsonRpcError {
    match error {
        QueryError::InvalidRequest(message) => {
            JsonRpcError::with_message(crate::types::jsonrpc::ErrorCode::InvalidParams, message)
        }
        QueryError::RangeInverted { from, to } => JsonRpcError::with_message(
            crate::types::jsonrpc::ErrorCode::InvalidParams,
            format!("query range is inverted: from {from} is greater than to {to}"),
        ),
        QueryError::ResourceNotFound | QueryError::NoIndexedData => {
            JsonRpcError::new(crate::types::jsonrpc::ErrorCode::ResourceNotFound)
        }
        QueryError::Internal(message) => internal_error(message),
        QueryError::Backend(message) => internal_error(message),
        QueryError::Decode(message) => internal_error(message),
        QueryError::InvalidBlock(message) => internal_error(message),
        QueryError::MissingData(message) => internal_error(message),
    }
}

fn internal_error(message: impl Into<String>) -> JsonRpcError {
    JsonRpcError::internal_error(format!("chain-data query error: {}", message.into()))
}

pub async fn query_blocks(
    reader: &ConfiguredQueryReader,
    request: QueryRequest<BlockFilter>,
) -> JsonRpcResult<Box<RawValue>> {
    let QueryRequest {
        from_block,
        to_block,
        target,
        order,
        filter,
        fields,
    } = request;
    let envelope = query::query_envelope(from_block, to_block, target, order)?;
    if let Some(fields) = fields.as_ref() {
        fields.validate(QueryMethod::Blocks)?;
    }
    let fields = fields.unwrap_or_default();
    let response = reader
        .query_blocks(BlockQueryRequest {
            envelope,
            filter: filter.unwrap_or_default().into_query_filter(),
        })
        .await
        .map_err(chain_data_error)?;
    let output = query::map_query_page(response, &fields);
    serialize_result(output)
}

pub async fn query_transactions(
    reader: &ConfiguredQueryReader,
    request: QueryRequest<TxFilter>,
) -> JsonRpcResult<Box<RawValue>> {
    let QueryRequest {
        from_block,
        to_block,
        target,
        order,
        filter,
        fields,
    } = request;
    let envelope = query::query_envelope(from_block, to_block, target, order)?;
    if let Some(fields) = fields.as_ref() {
        fields.validate(QueryMethod::Transactions)?;
    }
    let fields = fields.unwrap_or_default();
    let relations = BlockRelations {
        blocks: fields.blocks.is_some(),
    };
    let response = reader
        .query_transactions(QueryTransactionsRequest {
            envelope,
            filter: filter.unwrap_or_default().into_query_filter(),
            relations,
        })
        .await
        .map_err(chain_data_error)?;
    let output = query::map_query_page(response, &fields);
    serialize_result(output)
}

pub async fn query_logs(
    reader: &ConfiguredQueryReader,
    request: QueryRequest<LogFilter>,
) -> JsonRpcResult<Box<RawValue>> {
    let QueryRequest {
        from_block,
        to_block,
        target,
        order,
        filter,
        fields,
    } = request;
    let envelope = query::query_envelope(from_block, to_block, target, order)?;
    if let Some(fields) = fields.as_ref() {
        fields.validate(QueryMethod::Logs)?;
    }
    let fields = fields.unwrap_or_default();
    let relations = TransactionRelations {
        blocks: fields.blocks.is_some(),
        transactions: fields.transactions.is_some(),
    };
    let response = reader
        .query_logs(QueryLogsRequest {
            envelope,
            filter: filter.unwrap_or_default().into_query_filter()?,
            relations,
        })
        .await
        .map_err(chain_data_error)?;
    let output = query::map_query_page(response, &fields);
    serialize_result(output)
}

pub async fn query_traces(
    reader: &ConfiguredQueryReader,
    request: QueryRequest<TraceFilter>,
) -> JsonRpcResult<Box<RawValue>> {
    let QueryRequest {
        from_block,
        to_block,
        target,
        order,
        filter,
        fields,
    } = request;
    let envelope = query::query_envelope(from_block, to_block, target, order)?;
    if let Some(fields) = fields.as_ref() {
        fields.validate(QueryMethod::Traces)?;
    }
    let fields = fields.unwrap_or_default();
    let relations = TransactionRelations {
        blocks: fields.blocks.is_some(),
        transactions: fields.transactions.is_some(),
    };
    let response = reader
        .query_traces(TraceQueryRequest {
            envelope,
            filter: filter.unwrap_or_default().into_query_filter(),
            relations,
        })
        .await
        .map_err(chain_data_error)?;
    let output = query::map_query_page(response, &fields);
    serialize_result(output)
}

pub async fn query_transfers(
    reader: &ConfiguredQueryReader,
    request: QueryRequest<TransferFilter>,
) -> JsonRpcResult<Box<RawValue>> {
    let QueryRequest {
        from_block,
        to_block,
        target,
        order,
        filter,
        fields,
    } = request;
    let envelope = query::query_envelope(from_block, to_block, target, order)?;
    if let Some(fields) = fields.as_ref() {
        fields.validate(QueryMethod::Transfers)?;
    }
    let fields = fields.unwrap_or_default();
    let relations = TransactionRelations {
        blocks: fields.blocks.is_some(),
        transactions: fields.transactions.is_some(),
    };
    let response = reader
        .query_transfers(TransferQueryRequest {
            envelope,
            filter: filter.unwrap_or_default().into_query_filter(),
            relations,
        })
        .await
        .map_err(chain_data_error)?;
    let output = query::map_query_page(response, &fields);
    serialize_result(output)
}
