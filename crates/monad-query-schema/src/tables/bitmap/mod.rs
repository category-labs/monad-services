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
    store::{
        blob::{
            table::{BlobTable, SchemaBlobTable},
            BlobTableId,
        },
        meta::{table::MetaTable, MetaStore},
    },
    QueryResult,
};

use self::{
    index::{BitmapIndexKey, BitmapIndexValue},
    page_stats::BitmapPageStatsSchema,
};

pub mod index;
pub mod page_stats;

pub struct BitmapTables<BM, SM>
where
    BM: BlobTable,
    SM: MetaStore,
{
    pub index: SchemaBlobTable<BM, BitmapIndexKey, BitmapIndexValue>,
    pub page_stats: MetaTable<SM, BitmapPageStatsSchema>,
}

impl<BM, SM> BitmapTables<BM, SM>
where
    BM: BlobTable,
    SM: MetaStore,
{
    pub fn new(config: impl FnOnce() -> BM::Config, stats: SM) -> Self {
        Self {
            index: SchemaBlobTable::new(BM::new(BlobTableId::new("bitmap_index"), config())),
            page_stats: MetaTable::new(stats),
        }
    }

    pub async fn init(&self) -> QueryResult<()> {
        self.page_stats.init().await
    }
}
