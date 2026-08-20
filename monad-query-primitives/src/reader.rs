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

use std::{future::Future, pin::Pin};

use crate::QueryResult;

/// Raw object bytes returned by an [`ExternalBlobReader`].
pub type RawBytes = bytes::Bytes;

/// Read-only byte-range access to external archive objects.
pub trait ExternalBlobReader: Send + Sync + 'static {
    /// Reads `[start, end_exclusive)` of `key`; `Ok(None)` when the object is
    /// absent. Semantics match the store's `BlobStore::read_range`: the end
    /// clamps to EOF, a start strictly past EOF is an error.
    fn read_range(
        &self,
        key: &[u8],
        start: usize,
        end_exclusive: usize,
    ) -> Pin<Box<dyn Future<Output = QueryResult<Option<RawBytes>>> + Send + '_>>;
}
