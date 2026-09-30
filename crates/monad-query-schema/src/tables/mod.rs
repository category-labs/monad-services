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

use monad_query_types::{
    store::{blob::table::BlobTable, meta::MetaStore},
    QueryResult,
};

pub use self::{
    bitmap::{
        index::{BitmapIndexKey, BitmapIndexValue},
        page_stats::{BitmapPageStatsKey, BitmapPageStatsSchema},
        BitmapTables,
    },
    metadata::{
        block_hash::{MetadataBlockHashKey, MetadataBlockHashSchema, MetadataBlockHashValue},
        family::{MetadataFamilyKey, MetadataFamilySchema},
        family_block_index::{
            MetadataFamilyBlockIndexKey, MetadataFamilyBlockIndexSchema,
            MetadataFamilyBlockIndexValue,
        },
        MetadataTables,
    },
    payload::{
        location::{PayloadLocation, PayloadLocationKey, PayloadLocationSchema},
        payload::{PayloadItem, PayloadKey},
        PayloadTables,
    },
    state::{
        index::{StateIndex, StateIndexKey, StateIndexSchema},
        StateTables,
    },
};

pub mod bitmap;
pub mod metadata;
pub mod payload;
pub mod state;

pub const BITMAP_PAGE_SPAN: u64 = bitmap::index::BitmapIndexValue::PAGE_SPAN;

pub struct QueryTables<BM, MM, PM, PB, SM>
where
    BM: BlobTable,
    MM: MetaStore,
    PM: MetaStore,
    PB: BlobTable,
    SM: MetaStore,
{
    pub bitmap: BitmapTables<BM, MM>,
    pub metadata: MetadataTables<MM>,
    pub payload: PayloadTables<PM, PB>,
    pub state: StateTables<SM>,
}

impl<BM, MM, PM, PB, SM> QueryTables<BM, MM, PM, PB, SM>
where
    BM: BlobTable,
    MM: MetaStore,
    PM: MetaStore,
    PB: BlobTable,
    SM: MetaStore,
{
    pub fn new(
        bitmap_index: impl FnOnce() -> BM::Config,
        metadata_block_hash: impl FnOnce() -> MM,
        metadata_family: impl Fn() -> MM,
        payload: impl Fn() -> PB::Config,
        payload_location: impl Fn() -> PM,
        state: impl FnOnce() -> SM,
    ) -> Self {
        Self {
            bitmap: BitmapTables::new(bitmap_index, metadata_family()),
            metadata: MetadataTables::new(
                metadata_block_hash(),
                metadata_family(),
                metadata_family(),
            ),
            payload: PayloadTables::new(payload, payload_location),
            state: StateTables::new(state()),
        }
    }

    pub async fn init(&self) -> QueryResult<()> {
        let Self {
            bitmap,
            metadata,
            payload,
            state,
        } = self;

        metadata.init().await?;
        bitmap.init().await?;
        payload.init().await?;
        state.init().await
    }
}
