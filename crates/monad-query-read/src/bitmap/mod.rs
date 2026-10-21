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

use monad_query_schema::bitmap::BitmapFilterValue;

mod stream;

#[derive(Debug, Clone)]
pub(crate) enum BitmapFilter {
    Value(BitmapFilterValue),
    Any(Box<[BitmapFilter]>),
    All(Box<[BitmapFilter]>),
}

pub(crate) use stream::BitmapCandidateStream;
