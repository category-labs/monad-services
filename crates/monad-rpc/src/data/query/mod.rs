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

use std::{collections::HashSet, hash::Hash, num::NonZeroUsize};

use alloy_eips::BlockNumberOrTag;
use alloy_primitives::U64;
use monad_query_read::{QueryBound, QueryEnvelope, QueryOrder, QueryPage, QueryPageSchema};
use monad_query_types::CallKind;
use serde::Deserialize;

pub use self::{
    block::{BlockFilter, BlockQueryField, BlockQueryItem},
    log::{LogFilter, LogQueryField, LogQueryItem},
    trace::{TraceFilter, TraceQueryField, TraceQueryItem},
    transaction::{TxFilter, TxQueryField, TxQueryItem},
    transfer::{TransferFilter, TransferQueryField, TransferQueryItem},
};
use crate::types::jsonrpc::{JsonRpcError, JsonRpcResult};

mod block;
mod log;
mod trace;
mod transaction;
mod transfer;

#[derive(Debug, Clone)]
pub struct FilterValues<T>(HashSet<T>);

#[derive(Debug)]
pub struct QueryFilter<T>(pub Option<T>);

impl<T> Default for QueryFilter<T> {
    fn default() -> Self {
        Self(None)
    }
}

impl<'de, T> Deserialize<'de> for QueryFilter<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Self(Some(T::deserialize(deserializer)?)))
    }
}

impl<'de, T> Deserialize<'de> for FilterValues<T>
where
    T: Deserialize<'de> + Eq + Hash,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Input<T> {
            Values(Vec<T>),
            Value(T),
        }

        let values = match Input::deserialize(deserializer)? {
            Input::Values(values) if values.is_empty() => {
                return Err(serde::de::Error::custom("filter arrays must not be empty"));
            }
            Input::Values(values) => values,
            Input::Value(value) => vec![value],
        };
        Ok(Self(values.into_iter().collect()))
    }
}

#[derive(Debug, Clone)]
pub enum QueryFieldSet<T> {
    All,
    Fields(HashSet<T>),
}

impl<'de, T: Deserialize<'de> + Eq + Hash> Deserialize<'de> for QueryFieldSet<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Input<T> {
            Values(Vec<T>),
            Value(T),
            All(String),
        }
        match Input::deserialize(deserializer)? {
            Input::Values(values) if values.is_empty() => {
                Err(serde::de::Error::custom("invalid query field set"))
            }
            Input::Values(values) => Ok(Self::Fields(values.into_iter().collect())),
            Input::Value(value) => Ok(Self::Fields([value].into_iter().collect())),
            Input::All(value) if value == "all" => Ok(Self::All),
            Input::All(_) => Err(serde::de::Error::custom("invalid query field set")),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum QueryMethod {
    Blocks,
    Transactions,
    Logs,
    Traces,
    Transfers,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueryFieldSelection {
    pub blocks: Option<QueryFieldSet<BlockQueryField>>,
    pub transactions: Option<QueryFieldSet<TxQueryField>>,
    pub logs: Option<QueryFieldSet<LogQueryField>>,
    pub traces: Option<QueryFieldSet<TraceQueryField>>,
    pub transfers: Option<QueryFieldSet<TransferQueryField>>,
}

impl QueryFieldSelection {
    pub fn validate(&self, method: QueryMethod) -> JsonRpcResult<()> {
        let mut valid = match method {
            QueryMethod::Blocks => self.blocks.is_some(),
            QueryMethod::Transactions => self.transactions.is_some(),
            QueryMethod::Logs => self.logs.is_some(),
            QueryMethod::Traces => self.traces.is_some(),
            QueryMethod::Transfers => self.transfers.is_some(),
        };

        valid &= match method {
            QueryMethod::Blocks => self.transactions.is_none(),
            _ => true,
        };
        valid &= match method {
            QueryMethod::Logs => true,
            _ => self.logs.is_none(),
        };
        valid &= match method {
            QueryMethod::Traces => true,
            _ => self.traces.is_none(),
        };
        valid &= match method {
            QueryMethod::Transfers => true,
            _ => self.transfers.is_none(),
        };
        valid.then_some(()).ok_or_else(JsonRpcError::invalid_params)
    }

    fn selected<T: Eq + Hash>(fields: Option<&QueryFieldSet<T>>, field: T) -> bool {
        fields.is_none_or(|fields| match fields {
            QueryFieldSet::All => true,
            QueryFieldSet::Fields(fields) => fields.contains(&field),
        })
    }

    pub fn blocks_selected(&self, field: BlockQueryField) -> bool {
        Self::selected(self.blocks.as_ref(), field)
    }

    pub fn transactions_selected(&self, field: TxQueryField) -> bool {
        Self::selected(self.transactions.as_ref(), field)
    }

    pub fn logs_selected(&self, field: LogQueryField) -> bool {
        Self::selected(self.logs.as_ref(), field)
    }

    pub fn traces_selected(&self, field: TraceQueryField) -> bool {
        Self::selected(self.traces.as_ref(), field)
    }

    pub fn transfers_selected(&self, field: TransferQueryField) -> bool {
        Self::selected(self.transfers.as_ref(), field)
    }
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueryRequest<F> {
    pub from_block: Option<BlockNumberOrTag>,
    pub to_block: Option<BlockNumberOrTag>,
    #[serde(default, deserialize_with = "deserialize_quantity_string")]
    pub target: Option<U64>,
    #[serde(default)]
    pub order: QueryOrder,
    pub filter: Option<F>,
    pub fields: Option<QueryFieldSelection>,
}

// TODO(andr-dev): Consider allowing JSON numbers
fn deserialize_quantity_string<'de, D>(deserializer: D) -> Result<Option<U64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)?
        .map(|value| {
            crate::types::ethhex::decode_quantity(&value)
                .map(U64::from)
                .map_err(|error| serde::de::Error::custom(format!("{error:?}")))
        })
        .transpose()
}

pub fn query_envelope(
    from_block: Option<BlockNumberOrTag>,
    to_block: Option<BlockNumberOrTag>,
    target: Option<U64>,
    order: QueryOrder,
) -> JsonRpcResult<QueryEnvelope> {
    let from_block = resolve_bound(from_block, "fromBlock")?;
    let to_block = resolve_bound(to_block, "toBlock")?;

    Ok(QueryEnvelope {
        from_block,
        to_block,
        order,
        target: target
            .map(|target| {
                usize::try_from(target.to::<u64>())
                    .ok()
                    .and_then(NonZeroUsize::new)
                    .ok_or_else(JsonRpcError::invalid_params)
            })
            .transpose()?,
    })
}

fn resolve_bound(bound: Option<BlockNumberOrTag>, name: &str) -> JsonRpcResult<Option<QueryBound>> {
    match bound {
        None => Ok(None),
        Some(BlockNumberOrTag::Number(number)) => Ok(Some(QueryBound::Number(number))),
        Some(BlockNumberOrTag::Latest)
        | Some(BlockNumberOrTag::Finalized)
        | Some(BlockNumberOrTag::Safe) => Ok(Some(QueryBound::Latest)),
        Some(BlockNumberOrTag::Earliest) => Ok(Some(QueryBound::Earliest)),
        Some(BlockNumberOrTag::Pending) => Err(JsonRpcError::with_message(
            crate::types::jsonrpc::ErrorCode::InvalidParams,
            format!("{name}: pending is not supported"),
        )),
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmptyFilter {}

pub struct RpcQuerySchema;

impl QueryPageSchema for RpcQuerySchema {
    type Block = BlockQueryItem;
    type Transaction = TxQueryItem;
    type Log = LogQueryItem;
    type Trace = TraceQueryItem;
    type Transfer = TransferQueryItem;
}

pub fn map_query_page(page: QueryPage, fields: &QueryFieldSelection) -> QueryPage<RpcQuerySchema> {
    let mut page = page.map(
        |item| BlockQueryItem::new(item, fields),
        |item| TxQueryItem::new(item, fields),
        |item| LogQueryItem::new(item, fields),
        |item| TraceQueryItem::new(item, fields),
        |item| TransferQueryItem::new(item, fields),
    );

    if fields.blocks.is_some() {
        page.data
            .blocks
            .get_or_insert_with(|| Vec::new().into_boxed_slice());
    }

    if fields.transactions.is_some() {
        page.data
            .transactions
            .get_or_insert_with(|| Vec::new().into_boxed_slice());
    }

    if fields.logs.is_some() {
        page.data
            .logs
            .get_or_insert_with(|| Vec::new().into_boxed_slice());
    }

    if fields.traces.is_some() {
        page.data
            .traces
            .get_or_insert_with(|| Vec::new().into_boxed_slice());
    }

    if fields.transfers.is_some() {
        page.data
            .transfers
            .get_or_insert_with(|| Vec::new().into_boxed_slice());
    }

    page
}

fn call_kind(value: CallKind) -> &'static str {
    match value {
        CallKind::Call => "CALL",
        CallKind::StaticCall => "STATICCALL",
        CallKind::DelegateCall => "DELEGATECALL",
        CallKind::CallCode => "CALLCODE",
        CallKind::Create => "CREATE",
        CallKind::Create2 => "CREATE2",
        CallKind::SelfDestruct => "SELFDESTRUCT",
    }
}
