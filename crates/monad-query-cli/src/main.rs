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

use std::path::PathBuf;

use clap::{CommandFactory, FromArgMatches, Parser, Subcommand, ValueHint};
use eyre::Result;

mod index;
mod shared;
mod validate;

#[derive(Debug, Parser)]
#[command(version)]
struct MonadQueryIndexerCli {
    #[arg(long, value_hint = ValueHint::FilePath)]
    config: PathBuf,

    #[command(subcommand)]
    command: MonadQueryIndexerCliCommand,
}

#[derive(Debug, Subcommand)]
enum MonadQueryIndexerCliCommand {
    Index(self::index::MonadQueryCliIndexArgs),
    Validate(self::validate::MonadQueryCliValidateArgs),
}

pub fn main() -> Result<()> {
    let mut cmd = MonadQueryIndexerCli::command();

    let MonadQueryIndexerCli { config, command } =
        MonadQueryIndexerCli::from_arg_matches_mut(&mut cmd.get_matches_mut())
            .unwrap_or_else(|e| cmd.error(e.kind(), e).exit());

    drop(cmd);

    match command {
        MonadQueryIndexerCliCommand::Index(args) => self::index::run(config, args),
        MonadQueryIndexerCliCommand::Validate(args) => self::validate::run(config, args),
    }
}
