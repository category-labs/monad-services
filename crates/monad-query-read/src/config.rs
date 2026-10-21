use std::num::{NonZeroU64, NonZeroUsize};

#[derive(Debug, Clone, Copy)]
pub struct QueryReaderConfig {
    pub limits: QueryLimits,
    pub candidate_batch_load_max_concurrency: NonZeroUsize,
    pub candidate_batch_max_size: NonZeroUsize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryLimits {
    pub max_target: NonZeroUsize,
    pub max_block_range: BlockRangeLimit,
}

impl QueryLimits {
    pub const UNLIMITED: Self = Self {
        max_target: NonZeroUsize::MAX,
        max_block_range: BlockRangeLimit::Unlimited,
    };

    pub const fn new(max_target: NonZeroUsize, max_block_range: BlockRangeLimit) -> Self {
        Self {
            max_target,
            max_block_range,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockRangeLimit {
    Unlimited,
    Bounded(NonZeroU64),
}

impl BlockRangeLimit {
    pub const fn from_u64(value: u64) -> Self {
        match NonZeroU64::new(value) {
            Some(limit) => Self::Bounded(limit),
            None => Self::Unlimited,
        }
    }
}

impl std::fmt::Display for BlockRangeLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unlimited => f.write_str("unlimited"),
            Self::Bounded(limit) => limit.fmt(f),
        }
    }
}
