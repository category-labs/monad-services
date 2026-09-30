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

use alloy_primitives::{Address, B256};
use monad_query_types::{QueryError, QueryResult};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use strum::{EnumDiscriminants, EnumIter};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BitmapFilterKey {
    pub value: BitmapFilterValue,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, EnumDiscriminants)]
#[strum_discriminants(derive(IntoPrimitive, TryFromPrimitive, EnumIter))]
#[strum_discriminants(repr(u8))]
pub enum BitmapFilterValue {
    Address(Address),
    From(Address),
    HasTransfer,
    Selector([u8; 4]),
    To(Address),
    TopLevel,
    Topic0(B256),
    Topic1(B256),
    Topic2(B256),
    Topic3(B256),
    Miner(Address),
}

impl BitmapFilterValue {
    pub fn encode(&self) -> Box<[u8]> {
        let mut bytes = Vec::new();

        let tag: u8 = BitmapFilterValueDiscriminants::from(self).into();
        bytes.push(tag);

        match self {
            Self::Address(value) | Self::From(value) | Self::To(value) | Self::Miner(value) => {
                bytes.extend_from_slice(value.as_slice())
            }
            Self::HasTransfer | Self::TopLevel => {}
            Self::Selector(value) => bytes.extend_from_slice(value),
            Self::Topic0(value)
            | Self::Topic1(value)
            | Self::Topic2(value)
            | Self::Topic3(value) => bytes.extend_from_slice(value.as_slice()),
        }

        bytes.into_boxed_slice()
    }

    pub fn decode(bytes: &[u8]) -> QueryResult<Self> {
        let tag = *bytes
            .first()
            .ok_or(QueryError::Decode("empty bitmap filter value"))?;

        let kind = BitmapFilterValueDiscriminants::try_from(tag)
            .map_err(|_| QueryError::Decode("invalid bitmap filter value kind"))?;

        let body = &bytes[1..];

        match kind {
            BitmapFilterValueDiscriminants::Address => {
                if body.len() != 20 {
                    return Err(QueryError::Decode("invalid address bitmap value"));
                }

                Ok(Self::Address(Address::from_slice(body)))
            }
            BitmapFilterValueDiscriminants::From => {
                if body.len() != 20 {
                    return Err(QueryError::Decode("invalid from bitmap value"));
                }

                Ok(Self::From(Address::from_slice(body)))
            }
            BitmapFilterValueDiscriminants::HasTransfer => decode_flag(body, Self::HasTransfer),
            BitmapFilterValueDiscriminants::Selector => {
                if body.len() != 4 {
                    return Err(QueryError::Decode("invalid selector bitmap value"));
                }

                Ok(Self::Selector(body.try_into().unwrap()))
            }
            BitmapFilterValueDiscriminants::To => {
                if body.len() != 20 {
                    return Err(QueryError::Decode("invalid to bitmap value"));
                }

                Ok(Self::To(Address::from_slice(body)))
            }
            BitmapFilterValueDiscriminants::TopLevel => decode_flag(body, Self::TopLevel),
            BitmapFilterValueDiscriminants::Topic0 => decode_topic(body).map(Self::Topic0),
            BitmapFilterValueDiscriminants::Topic1 => decode_topic(body).map(Self::Topic1),
            BitmapFilterValueDiscriminants::Topic2 => decode_topic(body).map(Self::Topic2),
            BitmapFilterValueDiscriminants::Topic3 => decode_topic(body).map(Self::Topic3),
            BitmapFilterValueDiscriminants::Miner => {
                if body.len() != 20 {
                    return Err(QueryError::Decode("invalid miner bitmap value"));
                }

                Ok(Self::Miner(Address::from_slice(body)))
            }
        }
    }
}

fn decode_topic(bytes: &[u8]) -> QueryResult<B256> {
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| QueryError::Decode("invalid topic bitmap value"))?;

    Ok(B256::from(bytes))
}

fn decode_flag<T>(bytes: &[u8], value: T) -> QueryResult<T> {
    if bytes.is_empty() {
        Ok(value)
    } else {
        Err(QueryError::Decode("invalid flag bitmap value"))
    }
}
