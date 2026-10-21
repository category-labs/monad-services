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

use alloy_primitives::{Address, Bytes, B256, U64};
use monad_query_read::{LogQueryFilter, LogQueryItem as ReadLogQueryItem};
use serde::{Deserialize, Serialize};

use super::{FilterValues, QueryFieldSelection, QueryFilter};
use crate::types::jsonrpc::{JsonRpcError, JsonRpcResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LogQueryField {
    Address,
    BlockHash,
    BlockNumber,
    BlockTimestamp,
    Data,
    LogIndex,
    Removed,
    Topics,
    TransactionHash,
    TransactionIndex,
}

#[serde_with::skip_serializing_none]
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LogQueryItem {
    pub address: Option<Address>,
    pub block_hash: Option<B256>,
    pub block_number: Option<U64>,
    pub block_timestamp: Option<U64>,
    pub data: Option<Bytes>,
    pub log_index: Option<U64>,
    pub removed: Option<bool>,
    pub topics: Option<Vec<B256>>,
    pub transaction_hash: Option<B256>,
    pub transaction_index: Option<U64>,
}

impl LogQueryItem {
    pub fn new(item: ReadLogQueryItem, fields: &QueryFieldSelection) -> Self {
        let ReadLogQueryItem {
            block_hash,
            block_number,
            block_timestamp,
            transaction_hash,
            transaction_index,
            log_index,
            address,
            topics,
            data,
        } = item;

        LogQueryItem {
            address: fields
                .logs_selected(LogQueryField::Address)
                .then_some(address),
            block_hash: fields
                .logs_selected(LogQueryField::BlockHash)
                .then_some(block_hash),
            block_number: fields
                .logs_selected(LogQueryField::BlockNumber)
                .then(|| U64::from(block_number)),
            block_timestamp: fields
                .logs_selected(LogQueryField::BlockTimestamp)
                .then(|| U64::from(block_timestamp)),
            data: fields.logs_selected(LogQueryField::Data).then_some(data),
            log_index: fields
                .logs_selected(LogQueryField::LogIndex)
                .then(|| U64::from(log_index)),
            removed: fields
                .logs_selected(LogQueryField::Removed)
                .then_some(false),
            topics: fields
                .logs_selected(LogQueryField::Topics)
                .then_some(topics),
            transaction_hash: fields
                .logs_selected(LogQueryField::TransactionHash)
                .then_some(transaction_hash),
            transaction_index: fields
                .logs_selected(LogQueryField::TransactionIndex)
                .then(|| U64::from(transaction_index)),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, default)]
pub struct LogFilter {
    pub address: QueryFilter<FilterValues<Address>>,
    pub topics: QueryFilter<Vec<Option<FilterValues<B256>>>>,
}

impl LogFilter {
    pub fn into_query_filter(self) -> JsonRpcResult<LogQueryFilter> {
        let mut filter = LogQueryFilter {
            address: self.address.0.map(|value| value.0),
            topics: Vec::new(),
        };

        if let Some(topics) = self.topics.0 {
            if topics.is_empty() || topics.len() > 4 {
                return Err(JsonRpcError::invalid_params());
            }

            filter.topics = topics
                .into_iter()
                .map(|topic| topic.map(|value| value.0))
                .collect();
        }

        Ok(filter)
    }
}
