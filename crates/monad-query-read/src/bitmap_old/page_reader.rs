use monad_query_schema::family::Family;
use monad_query_schema::{
    bitmap::{BitmapFilterKey, BitmapFilterValue},
    tables::{bitmap::index::BitmapIndexKey, bitmap::BitmapTables},
};
use monad_query_types::{
    store::{blob::table::BlobTable, meta::MetaStore},
    QueryResult,
};
use roaring::RoaringBitmap;

/// Bitmap results in their storage representation: page base plus local IDs.
#[derive(Debug, Default)]
pub(crate) struct BitmapPageSet {
    pub(crate) pages: Vec<(u64, RoaringBitmap)>,
}

impl BitmapPageSet {
    pub(crate) fn ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.pages
            .iter()
            .flat_map(|(base, bitmap)| bitmap.iter().map(move |id| *base + u64::from(id)))
    }
}

pub struct BitmapPageReader<'a, M: BlobTable, S: MetaStore> {
    tables: &'a BitmapTables<M, S>,
}

impl<'a, M: BlobTable, S: MetaStore> BitmapPageReader<'a, M, S> {
    pub fn new(tables: &'a BitmapTables<M, S>) -> Self {
        Self { tables }
    }

    pub async fn read(
        &self,
        family: Family,
        filter: &BitmapFilterValue,
        page: u64,
    ) -> QueryResult<RoaringBitmap> {
        Ok(self
            .tables
            .index
            .get(&BitmapIndexKey {
                family,
                filter: BitmapFilterKey {
                    value: filter.clone(),
                },
                page,
            })
            .await?
            .map_or_else(RoaringBitmap::new, |value| value.bitmap))
    }
}
