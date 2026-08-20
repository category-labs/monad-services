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

use crate::{QueryError, QueryResult};

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    alloy_rlp::RlpEncodable,
    alloy_rlp::RlpDecodable,
)]
pub struct PrimaryId(u64);

impl PrimaryId {
    pub const ZERO: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn as_u64(self) -> u64 {
        self.0
    }

    pub fn checked_add(self, rhs: u64) -> QueryResult<Self> {
        self.0
            .checked_add(rhs)
            .map(Self)
            .ok_or(QueryError::Decode("primary id overflow"))
    }

    pub fn idx_in_block(self, first: PrimaryId) -> QueryResult<usize> {
        let delta = self
            .0
            .checked_sub(first.0)
            .ok_or(QueryError::Decode("primary id below block start"))?;
        usize::try_from(delta).map_err(|_| QueryError::Decode("primary block index overflow"))
    }
}

/// Defines a `PrimaryId` newtype scoped to one record family, so a family's
/// signatures can't accidentally accept ids minted for another family.
macro_rules! family_id {
    ($name:ident, $family:literal) => {
        #[doc = concat!("`PrimaryId` scoped to the ", $family, " family.")]
        #[derive(
            Debug,
            Clone,
            Copy,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            alloy_rlp::RlpEncodable,
            alloy_rlp::RlpDecodable,
        )]
        pub struct $name(PrimaryId);

        impl From<PrimaryId> for $name {
            fn from(id: PrimaryId) -> Self {
                Self(id)
            }
        }

        impl From<$name> for PrimaryId {
            fn from(id: $name) -> Self {
                id.0
            }
        }
    };
}

family_id!(LogId, "log");
family_id!(TxId, "tx");
family_id!(TraceId, "trace");
