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

use clap::{Parser, Subcommand};

use crate::{
    archive::cli::ArchiveRunCliArgs, block_writer::cli::ArchiveBlockWriterCli,
    check::cli::ArchiveCheckCli, index::cli::ArchiveIndexCliArgs,
};

#[derive(Debug, Parser)]
#[command(name = "monad-archive", about = "Monad archive tools")]
pub struct ArchiveCli {
    #[command(subcommand)]
    pub command: ArchiveCliCommand,
}

#[derive(Debug, Subcommand)]
pub enum ArchiveCliCommand {
    /// Archive blocks, receipts and traces to the archive sink
    Archive(ArchiveRunCliArgs),
    /// Index archived blocks for RPC queries
    Index(ArchiveIndexCliArgs),
    /// Check archive consistency across replicas
    Check(ArchiveCheckCli),
    /// Write block data out to a filesystem path
    BlockWriter(ArchiveBlockWriterCli),
}
