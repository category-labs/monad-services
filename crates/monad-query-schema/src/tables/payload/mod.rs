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
        blob::{partition::PartitionSchemaBlobTable, table::BlobTable, BlobTableId},
        meta::{table::MetaTable, MetaStore},
    },
    QueryResult,
};

use self::{
    location::PayloadLocationSchema,
    payload::{PayloadItem, PayloadKey},
};

pub mod location;
pub mod payload;

pub struct PayloadTables<M, B>
where
    M: MetaStore,
    B: BlobTable,
{
    pub payload: PartitionSchemaBlobTable<B, PayloadKey, PayloadItem>,
    pub payload_location: MetaTable<M, PayloadLocationSchema>,
}

impl<M, B> PayloadTables<M, B>
where
    M: MetaStore,
    B: BlobTable,
{
    const PAYLOAD_TABLE: BlobTableId = BlobTableId::new("payload");

    pub fn new(payload_config: impl Fn() -> B::Config, location_config: impl Fn() -> M) -> Self {
        Self {
            payload: PartitionSchemaBlobTable::new(B::new(Self::PAYLOAD_TABLE, payload_config())),
            payload_location: MetaTable::new(location_config()),
        }
    }

    pub async fn init(&self) -> QueryResult<()> {
        self.payload_location.init().await?;
        Ok(())
    }
}
