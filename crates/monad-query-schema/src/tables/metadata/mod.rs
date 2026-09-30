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
    store::meta::{table::MetaTable, MetaStore},
    QueryResult,
};

use self::{
    block_hash::MetadataBlockHashSchema, family::MetadataFamilySchema,
    family_block_index::MetadataFamilyBlockIndexSchema,
};

pub mod block_hash;
pub mod family;
pub mod family_block_index;

pub struct MetadataTables<M>
where
    M: MetaStore,
{
    pub block_hash: MetaTable<M, MetadataBlockHashSchema>,
    pub family: MetaTable<M, MetadataFamilySchema>,
    pub family_block_index: MetaTable<M, MetadataFamilyBlockIndexSchema>,
}

impl<M> MetadataTables<M>
where
    M: MetaStore,
{
    pub fn new(block: M, family: M, family_block_index: M) -> Self {
        Self {
            block_hash: MetaTable::new(block),
            family: MetaTable::new(family),
            family_block_index: MetaTable::new(family_block_index),
        }
    }

    pub async fn init(&self) -> QueryResult<()> {
        self.block_hash.init().await?;
        self.family.init().await?;
        self.family_block_index.init().await
    }
}
