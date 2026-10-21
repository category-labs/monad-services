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

#![recursion_limit = "256"]

use std::{env, num::NonZeroUsize, process::ExitCode};

use monad_query_read::{
    BlockQueryRequest, BlockRelations, QueryBound, QueryEnvelope, QueryLimits, QueryOrder,
    QueryReader, QueryReaderConfig, QueryTransactionsRequest, TxQueryFilter,
};
use monad_query_schema::tables::QueryTables;
use monad_query_store::{
    blob::{rocksdb::RocksDbBlobStore, BlobStoreBackend},
    meta::sqlite::{SqliteMetaStore, SqliteMetaStoreConfig},
};
use monad_query_types::store::blob::table::CodecBlobTable;

type BlobTable = CodecBlobTable<BlobStoreBackend>;
type Tables = QueryTables<BlobTable, SqliteMetaStore, SqliteMetaStore, BlobTable, SqliteMetaStore>;
type Reader = QueryReader<BlobTable, SqliteMetaStore, SqliteMetaStore, BlobTable, SqliteMetaStore>;

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = env::args().collect();
    let kind = args.get(1).map(String::as_str).unwrap_or("block");
    let from_block = args
        .get(2)
        .map(|value| value.parse().map(QueryBound::Number))
        .transpose()?;
    let to_block = args
        .get(3)
        .map(|value| value.parse().map(QueryBound::Number))
        .transpose()?;
    let target = args.get(4).map(|value| value.parse()).transpose()?;

    let metadata = SqliteMetaStore::new_with_config(SqliteMetaStoreConfig::new(
        "sqlite://./data/mainnet/metadata.sqlite?mode=rw",
    ))
    .await?;

    let blob = BlobStoreBackend::RocksDb(RocksDbBlobStore::new_read_only(
        "./data/mainnet/blob.rocksdb",
    )?);

    let tables: Tables = QueryTables::new(
        || blob.clone(),
        || metadata.clone(),
        || metadata.clone(),
        || blob.clone(),
        || metadata.clone(),
        || metadata.clone(),
    );

    let reader = Reader::new(
        QueryReaderConfig {
            limits: QueryLimits::UNLIMITED,
            candidate_batch_load_max_concurrency: NonZeroUsize::new(4).unwrap(),
            candidate_batch_max_size: NonZeroUsize::new(64).unwrap(),
        },
        tables,
    );

    let envelope = QueryEnvelope {
        from_block,
        to_block,
        target,
        order: QueryOrder::Ascending,
    };

    match kind {
        "block" | "blocks" => {
            let page = reader.query_blocks(BlockQueryRequest { envelope }).await?;
            println!("{} blocks", page.data.blocks.map_or(0, |items| items.len()));
        }
        "tx" | "transactions" => {
            let page = reader
                .query_transactions(QueryTransactionsRequest {
                    envelope,
                    filter: TxQueryFilter::default(),
                    relations: BlockRelations::default(),
                })
                .await?;
            println!(
                "{} transactions",
                page.data.transactions.map_or(0, |items| items.len())
            );
        }
        _ => return Err(format!("unknown query kind: {kind}").into()),
    }

    Ok(())
}
