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

use itertools::Either;

use crate::{QueryError, QueryLimitExceededKind, QueryResult};

/// Common request envelope for query operations.
///
/// Block range semantics depend on [`QueryOrder`]: with ascending order,
/// `from_block` is the lower bound; with descending, `to_block` is the lower bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryEnvelope {
    pub from_block: Option<u64>,
    pub to_block: Option<u64>,
    pub order: QueryOrder,
    /// Target result limit per request. The server completes the current block
    /// before stopping, so actual results may exceed this. Defaults to
    /// [`DEFAULT_QUERY_LIMIT`].
    pub limit: usize,
}

impl Default for QueryEnvelope {
    fn default() -> Self {
        const DEFAULT_QUERY_LIMIT: usize = 100;

        Self {
            from_block: None,
            to_block: None,
            order: QueryOrder::default(),
            limit: DEFAULT_QUERY_LIMIT,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryLimits {
    pub max_limit: usize,
    pub max_block_range: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QueryOrder {
    #[default]
    Ascending,
    Descending,
}

impl QueryOrder {
    pub fn iterate<I>(self, items: I) -> impl Iterator<Item = I::Item>
    where
        I: IntoIterator,
        I::IntoIter: DoubleEndedIterator,
    {
        match self {
            Self::Ascending => Either::Left(items.into_iter()),
            Self::Descending => Either::Right(items.into_iter().rev()),
        }
    }
}

impl QueryLimits {
    pub const UNLIMITED: Self = Self {
        max_limit: usize::MAX,
        max_block_range: u64::MAX,
    };

    pub const fn new(max_limit: usize, max_block_range: u64) -> Self {
        Self {
            max_limit,
            max_block_range,
        }
    }

    pub fn check_limit(&self, limit: usize) -> QueryResult<()> {
        if limit == 0 {
            return Err(QueryError::InvalidRequest("limit must be at least 1"));
        }

        if limit > self.max_limit {
            return Err(QueryError::LimitExceeded {
                kind: QueryLimitExceededKind::Limit,
                max_limit: self.max_limit,
                max_block_range: self.max_block_range,
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::QueryLimits;
    use crate::{QueryError, QueryLimitExceededKind};

    #[test]
    fn check_limit_accepts_and_rejects_boundary_values() {
        let limits = QueryLimits::new(5, 1_000);

        assert!(matches!(
            limits.check_limit(0),
            Err(QueryError::InvalidRequest("limit must be at least 1",))
        ));
        assert!(limits.check_limit(1).is_ok());
        assert!(limits.check_limit(5).is_ok());
        assert!(matches!(
            limits.check_limit(6),
            Err(QueryError::LimitExceeded {
                kind: QueryLimitExceededKind::Limit,
                max_limit: 5,
                max_block_range: 1_000,
            })
        ));
    }
}
