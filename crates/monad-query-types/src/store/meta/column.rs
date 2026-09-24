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

use std::borrow::Cow;

use strum::EnumDiscriminants;

#[derive(Debug, Clone, PartialEq, Eq, Hash, EnumDiscriminants)]
#[strum_discriminants(name(CellType))]
pub enum CellValue {
    Bool(bool),
    // TODO(andr-dev): Expand column support to specific integer types
    Number(String),
    Bytes(Box<[u8]>),
}

impl PartialOrd for CellValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CellValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        match (self, other) {
            (Self::Bool(a), Self::Bool(b)) => a.cmp(b),
            (Self::Number(a), Self::Number(b)) => a
                .parse::<i128>()
                .ok()
                .zip(b.parse::<i128>().ok())
                .map_or_else(|| a.cmp(b), |(a, b)| a.cmp(&b)),
            (Self::Bytes(a), Self::Bytes(b)) => a.cmp(b),

            (Self::Bool(_), _) => Ordering::Less,
            (Self::Number(_), Self::Bool(_)) => Ordering::Greater,
            (Self::Number(_), Self::Bytes(_)) => Ordering::Less,
            (Self::Bytes(_), _) => Ordering::Greater,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Cell {
    pub column: Cow<'static, str>,
    pub value: CellValue,
}

impl Cell {
    pub fn new(name: String, value: CellValue) -> Self {
        Self {
            column: Cow::Owned(name),
            value,
        }
    }

    pub fn new_bool(name: &'static str, value: bool) -> Self {
        Self {
            column: Cow::Borrowed(name),
            value: CellValue::Bool(value),
        }
    }

    pub fn new_number(name: &'static str, value: u64) -> Self {
        Self {
            column: Cow::Borrowed(name),
            value: CellValue::Number(value.to_string()),
        }
    }

    pub fn new_bytes(name: &'static str, value: Box<[u8]>) -> Self {
        Self {
            column: Cow::Borrowed(name),
            value: CellValue::Bytes(value),
        }
    }
}

pub type RowCells = Box<[Cell]>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: &'static str,
    pub kind: CellType,
}

impl ColumnDef {
    pub const fn new_bool(name: &'static str) -> Self {
        Self {
            name,
            kind: CellType::Bool,
        }
    }

    pub const fn new_number(name: &'static str) -> Self {
        Self {
            name,
            kind: CellType::Number,
        }
    }

    pub const fn new_byte(name: &'static str) -> Self {
        Self {
            name,
            kind: CellType::Bytes,
        }
    }
}
