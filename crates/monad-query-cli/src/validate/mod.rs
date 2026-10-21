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

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    io::IsTerminal,
    path::PathBuf,
    time::{Duration, Instant},
};

use alloy_rlp::Decodable;
use crossterm::{
    event::{self, Event, KeyCode, KeyModifiers},
    style::Stylize,
};
use eyre::{bail, Result, WrapErr};
use monad_query_schema::{
    bitmap::{BitmapFilterKey, BitmapFilterValue},
    family::{
        BitmapIndexable, Family, FamilyMetadata, StoredBlockQueryItem, StoredLogQueryItem,
        StoredTraceQueryItem, StoredTraceQueryItemWithAncestorStatus, StoredTxQueryItem,
    },
    tables::{
        bitmap::{
            index::{BitmapIndexKey, BitmapIndexValue},
            page_stats::BitmapPageStatsKey,
        },
        metadata::{
            block_hash::MetadataBlockHashKey, family::MetadataFamilyKey,
            family_block_index::MetadataFamilyBlockIndexKey,
        },
        payload::{
            location::PayloadLocationKey,
            payload::{PayloadItem, PayloadKey},
        },
        state::index::StateIndexKey,
    },
};
use monad_query_types::{
    store::blob::partition::{PartitionItemBoundaries, PartitionedBlobTable},
    PrimaryId,
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Layout},
    widgets::{Block, Gauge, Padding, Paragraph},
    Terminal, TerminalOptions, Viewport,
};
use strum::IntoEnumIterator;
use tracing::{error, info, warn};

pub(super) use self::cli::MonadQueryCliValidateArgs;
use crate::shared::{
    self, config::MonadQueryServiceConfig, with_async_runtime, with_tracing, Tables,
};

mod cli;

type BitmapPageKey = (Family, BitmapFilterValue, u64);
type ExpectedBitmap = BTreeMap<BitmapPageKey, BTreeSet<u32>>;
type BitmapCache = BTreeMap<BitmapPageKey, (BitmapIndexValue, u32)>;

pub(super) fn run(config: PathBuf, args: MonadQueryCliValidateArgs) -> Result<()> {
    with_tracing(|| {
        let progress = if args.log {
            Progress::Log(LogProgress { warnings: 0 })
        } else {
            Progress::Inline(InlineProgress::new(args.start_block, args.end_block)?)
        };
        with_async_runtime(|| run_async(config, args, progress))
    })
}

async fn run_async(
    config: PathBuf,
    args: MonadQueryCliValidateArgs,
    mut progress: Progress,
) -> Result<()> {
    let block = args.start_block;
    let result = run_validation(config, args, &mut progress).await;
    if let Err(error) = &result {
        if let Err(progress_error) = progress.abort(block, &error.to_string()) {
            error!(?progress_error, "failed to render validation failure");
        }
    }
    result
}

async fn run_validation(
    config: PathBuf,
    args: MonadQueryCliValidateArgs,
    progress: &mut Progress,
) -> Result<()> {
    let config = MonadQueryServiceConfig::load(&config).await?;
    let tables = shared::open_tables(config.storage, true).await?;
    let state = tables
        .state
        .index
        .get(&StateIndexKey)
        .await?
        .ok_or_else(|| eyre::eyre!("missing state index"))?;
    let last_block_id = state
        .next_block_id
        .checked_sub(1)
        .filter(|block| state.indexed_start.is_none_or(|start| *block >= start));
    let end_block = args
        .end_block
        .or(last_block_id)
        .ok_or_else(|| eyre::eyre!("no indexed blocks"))?;
    if args.start_block > end_block {
        bail!("validation start block exceeds end block");
    }

    progress.started(args.start_block, Some(end_block))?;
    let mut expected_family_ids = BTreeMap::new();
    if args.start_block > 0 {
        for family in Family::iter() {
            if let Some(metadata) = tables
                .metadata
                .family
                .get(&MetadataFamilyKey {
                    family,
                    block_height: args.start_block - 1,
                })
                .await?
            {
                let next_primary_id = metadata
                    .first_primary_id
                    .as_u64()
                    .checked_add(u64::from(metadata.count))
                    .ok_or_else(|| eyre::eyre!("family primary ID range overflows"))?;
                expected_family_ids.insert(family, next_primary_id);
            }
        }
    }
    let mut expected_bitmap = ExpectedBitmap::new();
    let mut bitmap_cache = BitmapCache::new();
    let mut issues = 0;
    const METADATA_BATCH_SIZE: u64 = 64;
    for batch_start in (args.start_block..=end_block).step_by(METADATA_BATCH_SIZE as usize) {
        let batch_end = batch_start
            .saturating_add(METADATA_BATCH_SIZE - 1)
            .min(end_block);
        let blocks = (batch_start..=batch_end).collect::<Vec<_>>();
        let families = [
            Family::BlockHeader,
            Family::Transaction,
            Family::Log,
            Family::Trace,
        ];
        let mut family_metadata = BTreeMap::new();
        for family in families {
            let keys = blocks
                .iter()
                .map(|&block_height| MetadataFamilyKey {
                    family,
                    block_height,
                })
                .collect::<Vec<_>>();
            for (block, metadata) in blocks
                .iter()
                .copied()
                .zip(tables.metadata.family.get_many(&keys).await?)
            {
                family_metadata.insert((block, family), metadata);
            }
        }
        for block in blocks {
            if progress.interrupted()? {
                return Ok(());
            }
            let mut block_issues = 0;
            if tables
                .metadata
                .block_hash
                .get(&MetadataBlockHashKey {
                    block_height: block,
                })
                .await?
                .is_none()
            {
                progress.issue(block, "missing block hash")?;
                block_issues += 1;
            }
            for family in Family::iter() {
                let metadata = family_metadata.remove(&(block, family)).flatten();
                match metadata {
                    Some(metadata) => {
                        if expected_family_ids
                            .get(&family)
                            .is_some_and(|&expected| expected != metadata.first_primary_id.as_u64())
                        {
                            progress.issue(block, "non-contiguous family primary IDs")?;
                            block_issues += 1;
                        }
                        let next_primary_id = metadata
                            .first_primary_id
                            .as_u64()
                            .checked_add(u64::from(metadata.count))
                            .ok_or_else(|| eyre::eyre!("family primary ID range overflows"))?;
                        expected_family_ids.insert(family, next_primary_id);
                        if metadata.count > 0 {
                            match tables
                                .metadata
                                .family_block_index
                                .seek_le(&MetadataFamilyBlockIndexKey {
                                    family,
                                    first_primary_id: metadata.first_primary_id.as_u64(),
                                })
                                .await?
                            {
                                Some((key, value))
                                    if key.first_primary_id
                                        == metadata.first_primary_id.as_u64()
                                        && value.block_height == block => {}
                                _ => {
                                    progress.issue(block, "family block index mismatch")?;
                                    block_issues += 1;
                                }
                            }
                        }
                        match validate_payload(
                            &tables,
                            family,
                            block,
                            metadata,
                            &mut expected_bitmap,
                            &mut bitmap_cache,
                            progress,
                        )
                        .await
                        {
                            Ok(()) => {}
                            Err(error) => {
                                progress.issue(block, &error.to_string())?;
                                block_issues += 1;
                            }
                        }
                    }
                    None => {
                        progress.issue(block, "missing family metadata")?;
                        block_issues += 1;
                    }
                }
            }
            issues += block_issues;
            progress.block(block)?;
        }
    }
    if state.indexed_start == Some(args.start_block) && last_block_id == Some(end_block) {
        validate_bitmap_exact(expected_bitmap, bitmap_cache)?;
    }
    let final_state = tables
        .state
        .index
        .get(&StateIndexKey)
        .await?
        .ok_or_else(|| eyre::eyre!("missing final state index"))?;
    if final_state.next_block_id != state.next_block_id
        || final_state.next_tx_id != state.next_tx_id
        || final_state.next_log_id != state.next_log_id
        || final_state.next_trace_id != state.next_trace_id
    {
        progress.issue(end_block, "state changed during validation")?;
        issues += 1;
    }
    if last_block_id == Some(end_block)
        && (expected_family_ids.get(&Family::Transaction).copied() != Some(state.next_tx_id)
            || expected_family_ids.get(&Family::Log).copied() != Some(state.next_log_id)
            || expected_family_ids.get(&Family::Trace).copied() != Some(state.next_trace_id))
    {
        progress.issue(end_block, "family frontiers disagree with state index")?;
        issues += 1;
    }
    progress.finished(end_block - args.start_block + 1, issues)?;
    if issues != 0 {
        bail!("validation found issues");
    }
    Ok(())
}

async fn validate_payload(
    tables: &Tables,
    family: Family,
    block: u64,
    metadata: FamilyMetadata,
    expected_bitmap: &mut ExpectedBitmap,
    bitmap_cache: &mut BitmapCache,
    progress: &mut Progress,
) -> Result<()> {
    let first = metadata.first_primary_id.as_u64();
    let ids = (0..u64::from(metadata.count))
        .map(|offset| {
            first
                .checked_add(offset)
                .ok_or_else(|| eyre::eyre!("payload ID range overflows"))
        })
        .collect::<Result<Vec<_>>>()?;
    let keys = ids
        .iter()
        .flat_map(|&id| {
            let mut keys = vec![id];
            let partition_start = PayloadKey::partition_start(id);
            if id > partition_start {
                keys.push(id - 1);
            }
            keys
        })
        .collect::<BTreeSet<_>>();
    let location_keys = keys
        .iter()
        .map(|&id| PayloadLocationKey {
            family,
            id: PrimaryId::new(id),
        })
        .collect::<Vec<_>>();
    let location_values = tables
        .payload
        .payload_location
        .get_many(&location_keys)
        .await?;
    let locations = keys
        .into_iter()
        .zip(location_values)
        .filter_map(|(id, value)| value.map(|value| (id, value.payload_end())))
        .collect::<BTreeMap<_, _>>();

    let mut partitions = BTreeMap::<u64, Vec<(usize, usize)>>::new();
    for &id in &ids {
        let end = locations
            .get(&id)
            .copied()
            .ok_or_else(|| eyre::eyre!("missing payload location for {family:?} id {id}"))?;
        let partition = PayloadKey::partition_start(id);
        let start = if id == partition {
            0
        } else {
            locations.get(&(id - 1)).copied().ok_or_else(|| {
                eyre::eyre!("missing preceding payload location for {family:?} id {id}")
            })?
        };
        let start = usize::try_from(start)?;
        let end = usize::try_from(end)?;
        if start > end {
            bail!("invalid payload bounds for {family:?} id {id}");
        }
        partitions.entry(partition).or_default().push((start, end));
    }

    let mut payload_items = Vec::<(u64, PayloadItem)>::new();
    for (partition, boundaries) in partitions {
        let boundaries = PartitionItemBoundaries::new(boundaries.into_boxed_slice())?;
        let values = tables
            .payload
            .payload
            .get_many(&PayloadKey::from_id(family, partition), &boundaries)
            .await?
            .ok_or_else(|| {
                eyre::eyre!("missing payload blob for {family:?} partition {partition}")
            })?;
        if values.len() != boundaries.iter().count() {
            bail!("payload item count mismatch for {family:?} partition {partition}");
        }
        let partition_ids = ids
            .iter()
            .copied()
            .filter(|id| PayloadKey::partition_start(*id) == partition)
            .collect::<Vec<_>>();
        payload_items.extend(partition_ids.into_iter().zip(values));
    }

    match family {
        Family::BlockHeader => {
            let expected_hash = tables
                .metadata
                .block_hash
                .get(&MetadataBlockHashKey {
                    block_height: block,
                })
                .await?
                .ok_or_else(|| eyre::eyre!("missing block hash"))?;
            for (_, item) in &payload_items {
                let mut bytes = &item.bytes[..];
                let header = StoredBlockQueryItem::decode(&mut bytes)?.header;
                if header.number != block || header.hash_slow() != expected_hash.0 {
                    bail!("block header payload mismatch for block {block}");
                }
            }
        }
        Family::Transaction | Family::Log | Family::Trace => {}
    }
    validate_bitmap_membership(
        tables,
        family,
        block,
        payload_items,
        expected_bitmap,
        bitmap_cache,
        progress,
    )
    .await
}

async fn validate_bitmap_membership(
    tables: &Tables,
    family: Family,
    block: u64,
    items: Vec<(u64, PayloadItem)>,
    expected_bitmap: &mut ExpectedBitmap,
    bitmap_cache: &mut BitmapCache,
    progress: &mut Progress,
) -> Result<()> {
    let mut expected = Vec::new();
    match family {
        Family::BlockHeader => {
            for (id, item) in items {
                let mut bytes = &item.bytes[..];
                expected.push((
                    id,
                    StoredBlockQueryItem::decode(&mut bytes)?.bitmap_filters()?,
                ));
            }
        }
        Family::Transaction => {
            for (id, item) in items {
                let mut bytes = &item.bytes[..];
                expected.push((id, StoredTxQueryItem::decode(&mut bytes)?.bitmap_filters()?));
            }
        }
        Family::Log => {
            for (id, item) in items {
                let mut bytes = &item.bytes[..];
                expected.push((
                    id,
                    StoredLogQueryItem::decode(&mut bytes)?.bitmap_filters()?,
                ));
            }
        }
        Family::Trace => {
            let ids = items.iter().map(|(id, _)| *id).collect::<Vec<_>>();
            let mut traces = Vec::with_capacity(items.len());
            for (_, item) in items {
                traces.push(StoredTraceQueryItem::decode(&mut &item.bytes[..])?);
            }
            for (id, trace) in ids
                .into_iter()
                .zip(StoredTraceQueryItemWithAncestorStatus::from_traces(&traces).iter())
            {
                expected.push((id, trace.bitmap_filters()?));
            }
        }
    }

    for (id, filters) in expected {
        let page = id / BitmapIndexValue::PAGE_SPAN * BitmapIndexValue::PAGE_SPAN;
        for filter in filters {
            let key = (family, filter.clone(), page);
            expected_bitmap
                .entry(key.clone())
                .or_default()
                .insert((id - page) as u32);
            if !bitmap_cache.contains_key(&key) {
                let bitmap = tables
                    .bitmap
                    .index
                    .get(&BitmapIndexKey {
                        family,
                        filter: BitmapFilterKey {
                            value: filter.clone(),
                        },
                        page,
                    })
                    .await?
                    .ok_or_else(|| eyre::eyre!("missing bitmap page for {family:?} id {id}"))?;
                let stats = tables
                    .bitmap
                    .page_stats
                    .get(&BitmapPageStatsKey {
                        family,
                        filter,
                        page,
                    })
                    .await?
                    .ok_or_else(|| {
                        eyre::eyre!("missing bitmap page stats for {family:?} id {id}")
                    })?;
                if u64::from(stats) != bitmap.bitmap.len() as u64 {
                    progress.warning(
                        block,
                        &format!("bitmap page stats mismatch for {family:?} page {page}"),
                    )?;
                }
                bitmap_cache.insert(key.clone(), (bitmap, stats));
            }
            let (bitmap, _) = &bitmap_cache[&key];
            if bitmap.id_start.as_u64() > id
                || bitmap.id_end.as_u64() < id
                || bitmap.id_start.as_u64() / BitmapIndexValue::PAGE_SPAN
                    != page / BitmapIndexValue::PAGE_SPAN
                || !bitmap.bitmap.contains((id - page) as u32)
            {
                bail!("bitmap bounds or membership mismatch for {family:?} id {id}");
            }
        }
    }
    Ok(())
}

fn validate_bitmap_exact(expected: ExpectedBitmap, cache: BitmapCache) -> Result<()> {
    let mut errors = Vec::new();
    for (key, expected_bits) in expected {
        let (family, filter, page) = &key;
        let Some((bitmap, _)) = cache.get(&key) else {
            errors.push(format!(
                "bitmap page was not loaded for {family:?} page {page}"
            ));
            continue;
        };
        let actual_bits = bitmap.bitmap.iter().collect::<BTreeSet<_>>();
        if actual_bits != expected_bits {
            let missing = expected_bits
                .difference(&actual_bits)
                .take(5)
                .collect::<Vec<_>>();
            let orphaned = actual_bits
                .difference(&expected_bits)
                .take(5)
                .collect::<Vec<_>>();
            let expected_sample = expected_bits.iter().take(12).collect::<Vec<_>>();
            let actual_sample = actual_bits.iter().take(12).collect::<Vec<_>>();
            errors.push(format!(
                "bitmap mismatch for {family:?} filter {filter:?} page {page}: expected {}, actual {}, missing {missing:?}, orphaned {orphaned:?}, expected sample {expected_sample:?}, actual sample {actual_sample:?}",
                expected_bits.len(),
                actual_bits.len(),
            ));
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        bail!("bitmap validation errors:\n{}", errors.join("\n"));
    }
}

enum Progress {
    Inline(InlineProgress),
    Log(LogProgress),
}

impl Progress {
    fn started(&mut self, start: u64, end: Option<u64>) -> Result<()> {
        match self {
            Self::Inline(progress) => progress.started(start, end),
            Self::Log(progress) => progress.started(start, end),
        }
    }
    fn issue(&mut self, block: u64, message: &str) -> Result<()> {
        match self {
            Self::Inline(progress) => progress.issue(block, message),
            Self::Log(progress) => progress.issue(block, message),
        }
    }
    fn warning(&mut self, block: u64, message: &str) -> Result<()> {
        match self {
            Self::Inline(progress) => progress.warning(block, message),
            Self::Log(progress) => progress.warning(block, message),
        }
    }
    fn block(&mut self, block: u64) -> Result<()> {
        match self {
            Self::Inline(progress) => progress.block(block),
            Self::Log(progress) => progress.block(block),
        }
    }
    fn finished(&mut self, checked: u64, issues: usize) -> Result<()> {
        match self {
            Self::Inline(progress) => progress.finished(checked, issues),
            Self::Log(progress) => progress.finished(checked, issues),
        }
    }
    fn interrupted(&mut self) -> Result<bool> {
        match self {
            Self::Inline(_) => Ok(event::poll(Duration::ZERO)?
                && matches!(
                    event::read()?,
                    Event::Key(key)
                        if key.code == KeyCode::Char('c')
                            && key.modifiers.contains(KeyModifiers::CONTROL)
                )),
            Self::Log(_) => Ok(false),
        }
    }
    fn abort(&mut self, block: u64, message: &str) -> Result<()> {
        match self {
            Self::Inline(progress) => {
                progress.issue(block, &format!("validation aborted: {message}"))?;
                progress.finished(progress.checked, progress.issues.len())
            }
            Self::Log(progress) => {
                progress.issue(block, &format!("validation aborted: {message}"))?;
                progress.finished(0, 1)
            }
        }
    }
}

struct LogProgress {
    warnings: usize,
}
impl LogProgress {
    fn started(&mut self, start: u64, end: Option<u64>) -> Result<()> {
        info!(start_block = start, ?end, "starting validation");
        Ok(())
    }
    fn issue(&mut self, block: u64, message: &str) -> Result<()> {
        error!(block, message, "validation issue");
        Ok(())
    }
    fn warning(&mut self, block: u64, message: &str) -> Result<()> {
        self.warnings += 1;
        warn!(block, message, "validation warning");
        Ok(())
    }
    fn block(&mut self, block: u64) -> Result<()> {
        info!(block, "validated block");
        Ok(())
    }
    fn finished(&mut self, checked: u64, errors: usize) -> Result<()> {
        info!(
            checked,
            warnings = self.warnings,
            errors,
            "validation complete"
        );
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Severity {
    Warning,
    Error,
}

struct ValidationIssue {
    severity: Severity,
    block: u64,
    message: String,
}

struct InlineProgress {
    terminal: Terminal<CrosstermBackend<std::io::Stdout>>,
    start: u64,
    end: Option<u64>,
    checked: u64,
    issues: Vec<ValidationIssue>,
    recent_issues: VecDeque<usize>,
    started_at: Instant,
    restored: bool,
}

impl InlineProgress {
    const RECENT_ISSUES: usize = 5;

    fn new(start: u64, end: Option<u64>) -> Result<Self> {
        Ok(Self {
            terminal: ratatui::try_init_with_options(TerminalOptions {
                viewport: Viewport::Inline(7),
            })?,
            start,
            end,
            checked: 0,
            issues: Vec::new(),
            recent_issues: VecDeque::new(),
            started_at: Instant::now(),
            restored: false,
        })
    }

    fn draw(&mut self) -> Result<()> {
        use ratatui::{
            style::{Color, Modifier, Style},
            text::{Line, Span},
        };

        let percent = self.end.map_or(0, |end| {
            let total = end.saturating_sub(self.start).saturating_add(1);
            self.checked
                .saturating_mul(100)
                .checked_div(total)
                .unwrap_or(0) as u16
        });
        let recent = self.recent_issues.iter().map(|&index| {
            let issue = &self.issues[index];
            let (icon, color) = match issue.severity {
                Severity::Warning => ("⚠", Color::Yellow),
                Severity::Error => ("✗", Color::LightRed),
            };
            Line::from(vec![
                Span::styled(
                    format!(" {icon} block {}: ", issue.block),
                    Style::default().fg(color).add_modifier(Modifier::BOLD),
                ),
                Span::raw(issue.message.clone()),
            ])
        });
        let total = self.end.map(|end| end.saturating_sub(self.start) + 1);
        let mut lines = Vec::new();
        if !self.issues.is_empty() {
            lines.push(Line::from(Span::styled(
                "Recent warnings and errors",
                Style::default().add_modifier(Modifier::BOLD),
            )));
            lines.extend(recent);
        }
        self.terminal
            .draw(|frame| {
                let [progress, details] =
                    Layout::vertical([Constraint::Length(3), Constraint::Min(1)])
                        .areas(frame.area());
                let progress_block = Block::bordered();
                let progress_inner = progress_block.inner(progress);
                frame.render_widget(progress_block, progress);
                let warning_count = self
                    .issues
                    .iter()
                    .filter(|issue| issue.severity == Severity::Warning)
                    .count();
                let error_count = self.issues.len() - warning_count;
                let status_text = format!(
                    "{}/{} | ⚠ {} | ✗ {} | {}",
                    self.checked,
                    total.unwrap_or(0),
                    warning_count,
                    error_count,
                    format_duration(self.started_at.elapsed())
                );
                let status_width = status_text.chars().count() as u16;
                let [bar, status] =
                    Layout::horizontal([Constraint::Min(1), Constraint::Length(status_width)])
                        .areas(progress_inner);
                let warning_style = if warning_count == 0 {
                    Style::default().fg(Color::DarkGray)
                } else {
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD)
                };
                let error_style = if error_count == 0 {
                    Style::default().fg(Color::DarkGray)
                } else {
                    Style::default()
                        .fg(Color::LightRed)
                        .add_modifier(Modifier::BOLD)
                };
                frame.render_widget(
                    Paragraph::new(Line::from(vec![
                        Span::raw(format!("{}/{} | ", self.checked, total.unwrap_or(0))),
                        Span::styled(format!("⚠ {warning_count}"), warning_style),
                        Span::raw(" | "),
                        Span::styled(format!("✗ {error_count}"), error_style),
                        Span::raw(format!(" | {}", format_duration(self.started_at.elapsed()))),
                    ]))
                    .alignment(Alignment::Right),
                    status,
                );
                frame.render_widget(Gauge::default().percent(percent), bar);
                frame.render_widget(
                    Paragraph::new(lines).block(Block::new().padding(Padding::left(1))),
                    details,
                );
            })
            .wrap_err("draw validation progress")?;
        Ok(())
    }

    fn started(&mut self, start: u64, end: Option<u64>) -> Result<()> {
        self.start = start;
        self.end = end;
        self.draw()
    }

    fn issue(&mut self, block: u64, message: &str) -> Result<()> {
        self.record(Severity::Error, block, message)
    }

    fn warning(&mut self, block: u64, message: &str) -> Result<()> {
        self.record(Severity::Warning, block, message)
    }

    fn record(&mut self, severity: Severity, block: u64, message: &str) -> Result<()> {
        self.issues.push(ValidationIssue {
            severity,
            block,
            message: message.to_owned(),
        });
        self.recent_issues.push_back(self.issues.len() - 1);
        if self.recent_issues.len() > Self::RECENT_ISSUES {
            self.recent_issues.pop_front();
        }
        self.draw()
    }

    fn block(&mut self, _block: u64) -> Result<()> {
        self.checked += 1;
        self.draw()
    }

    fn restore_terminal(&mut self) {
        if !self.restored {
            ratatui::restore();
            self.restored = true;
        }
    }

    fn finished(&mut self, checked: u64, _issues: usize) -> Result<()> {
        self.checked = checked;
        self.draw()?;
        self.terminal
            .clear()
            .wrap_err("clear validation progress")?;
        self.restore_terminal();
        let color = std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none();
        let warning_count = self
            .issues
            .iter()
            .filter(|issue| issue.severity == Severity::Warning)
            .count();
        let error_count = self.issues.len() - warning_count;
        if error_count == 0 && warning_count == 0 {
            let heading = "Validation passed";
            let heading = if color {
                heading.green().bold().to_string()
            } else {
                heading.to_owned()
            };
            println!("{heading}: checked {checked} blocks.");
        } else {
            let heading = if error_count == 0 {
                "Validation completed with warnings"
            } else {
                "Validation failed"
            };
            let heading = if color && error_count > 0 {
                heading.red().bold().to_string()
            } else if color {
                heading.yellow().bold().to_string()
            } else {
                heading.to_owned()
            };
            println!("\n{heading}: {warning_count} warning(s), {error_count} error(s)");
            self.issues
                .sort_by_key(|issue| issue.severity != Severity::Warning);
            for (index, issue) in self.issues.iter().enumerate() {
                let label = match (issue.severity, color) {
                    (Severity::Warning, true) => "warning".yellow().bold().to_string(),
                    (Severity::Warning, false) => "warning".to_owned(),
                    (Severity::Error, true) => "error".red().bold().to_string(),
                    (Severity::Error, false) => "error".to_owned(),
                };
                print!("{} ", index + 1);
                println!("{label}: {}", issue.message);
                println!("    block: {}", issue.block);
            }
        }
        Ok(())
    }
}

fn format_duration(duration: Duration) -> String {
    let seconds = duration.as_secs();
    format!(
        "{}:{:02}:{:02}",
        seconds / 3600,
        seconds / 60 % 60,
        seconds % 60
    )
}

impl Drop for InlineProgress {
    fn drop(&mut self) {
        self.restore_terminal();
    }
}
