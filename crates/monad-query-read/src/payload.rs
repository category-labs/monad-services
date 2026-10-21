// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;
use monad_query_schema::{
    family::Family,
    tables::payload::{location::PayloadLocationKey, payload::PayloadKey, PayloadTables},
};
use monad_query_types::{
    store::{
        blob::{
            partition::{PartitionItemBoundaries, PartitionedBlobTable},
            table::BlobTable,
        },
        meta::MetaStore,
    },
    PrimaryId, QueryError,
};

pub(crate) struct PayloadReader<'a, PM, PB>
where
    PM: MetaStore,
    PB: BlobTable,
{
    tables: &'a PayloadTables<PM, PB>,
}

impl<'a, PM, PB> PayloadReader<'a, PM, PB>
where
    PM: MetaStore,
    PB: BlobTable,
{
    pub(crate) fn new(tables: &'a PayloadTables<PM, PB>) -> Self {
        Self { tables }
    }

    pub(crate) async fn read_many(
        &self,
        family: Family,
        ids: &[u64],
    ) -> crate::QueryResult<HashMap<u64, Bytes>> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        let mut location_keys = Vec::with_capacity(ids.len() * 2);
        for &id in ids {
            location_keys.push(PayloadLocationKey {
                family,
                id: PrimaryId::new(id),
            });
            let partition_start = PayloadKey::partition_start(id);
            if id > partition_start {
                location_keys.push(PayloadLocationKey {
                    family,
                    id: PrimaryId::new(id - 1),
                });
            }
        }
        let location_values = self
            .tables
            .payload_location
            .get_many(&location_keys)
            .await?;
        let locations: BTreeMap<u64, u64> = location_keys
            .into_iter()
            .zip(location_values.into_vec())
            .filter_map(|(key, value)| value.map(|value| (key.id.as_u64(), value.payload_end())))
            .collect();
        let mut groups = Vec::new();
        for &id in ids {
            let end = *locations
                .get(&id)
                .ok_or(QueryError::MissingData("missing payload location"))?
                as usize;
            let partition = PayloadKey::partition_number(id);
            let partition_start = PayloadKey::partition_start(id);
            let start = locations
                .range(partition_start..id)
                .next_back()
                .map_or(0, |(_, end)| *end as usize);
            if groups
                .last()
                .is_none_or(|(previous, _)| *previous != partition)
            {
                groups.push((partition, Vec::new()));
            }
            groups.last_mut().unwrap().1.push((id, (start, end)));
        }
        let mut result = HashMap::new();
        for (partition, items) in groups {
            let boundaries =
                PartitionItemBoundaries::new(items.iter().map(|(_, bounds)| *bounds).collect())?;
            let partition_start = PayloadKey::partition_start(items[0].0);
            tracing::debug!(
                ?family,
                partition,
                partition_start,
                item_count = items.len(),
                first_id = items.first().map(|(id, _)| *id),
                last_id = items.last().map(|(id, _)| *id),
                first_bounds = ?items.first().map(|(_, bounds)| bounds),
                last_bounds = ?items.last().map(|(_, bounds)| bounds),
                "dispatch payload blob range read"
            );
            let values = self
                .tables
                .payload
                .get_many(&PayloadKey::from_id(family, partition_start), &boundaries)
                .await?
                .ok_or(QueryError::MissingData("missing payload"))?;
            for ((id, _), value) in items.into_iter().zip(values) {
                result.insert(id, value.bytes);
            }
        }
        Ok(result)
    }
}
