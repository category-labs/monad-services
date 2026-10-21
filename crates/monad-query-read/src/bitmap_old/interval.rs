use std::ops::Range;

use super::page_reader::BitmapPageSet;

/// Compact, sorted, disjoint, half-open candidate ID intervals.
#[derive(Debug)]
pub struct BitmapCandidateIdIntervals {
    ranges: Vec<Range<u64>>,
}

impl BitmapCandidateIdIntervals {
    #[allow(clippy::single_range_in_vec_init)]
    pub(crate) fn new(start: u64, end: u64) -> Self {
        assert!(start < end);
        Self {
            ranges: [start..end].into(),
        }
    }

    pub(crate) fn empty() -> Self {
        Self { ranges: Vec::new() }
    }

    pub(crate) fn union(&mut self, pages: &BitmapPageSet) {
        let mut incoming = Vec::new();
        let mut start = None;
        let mut previous: u64 = 0;
        for id in pages.ids() {
            assert!(id < u64::MAX);
            match start {
                None => start = Some(id),
                Some(begin) if id > previous.saturating_add(1) => {
                    incoming.push(begin..previous + 1);
                    start = Some(id);
                }
                Some(_) => {}
            }
            previous = id;
        }
        if let Some(begin) = start {
            incoming.push(begin..previous + 1);
        }

        incoming.append(&mut self.ranges);
        incoming.sort_unstable_by_key(|range| range.start);
        self.ranges.clear();
        for range in incoming {
            if let Some(last) = self.ranges.last_mut() {
                if range.start <= last.end {
                    last.end = last.end.max(range.end);
                    continue;
                }
            }
            self.ranges.push(range);
        }
    }

    #[allow(dead_code)]
    pub(crate) fn intersect(&mut self, pages: &BitmapPageSet) {
        let mut incoming = Vec::new();
        let mut start = None;
        let mut previous: u64 = 0;
        for id in pages.ids() {
            assert!(id < u64::MAX);
            match start {
                None => start = Some(id),
                Some(begin) if id > previous.saturating_add(1) => {
                    incoming.push(begin..previous + 1);
                    start = Some(id);
                }
                Some(_) => {}
            }
            previous = id;
        }
        if let Some(begin) = start {
            incoming.push(begin..previous + 1);
        }

        let mut result = Vec::new();
        let (mut left, mut right) = (0, 0);
        while left < self.ranges.len() && right < incoming.len() {
            let start = self.ranges[left].start.max(incoming[right].start);
            let end = self.ranges[left].end.min(incoming[right].end);
            if start < end {
                result.push(start..end);
            }
            if self.ranges[left].end <= incoming[right].end {
                left += 1;
            } else {
                right += 1;
            }
        }
        self.ranges = result;
    }

    pub(crate) fn union_intervals(&mut self, other: &Self) {
        let mut ranges = std::mem::take(&mut self.ranges);
        ranges.extend(other.ranges.iter().cloned());
        ranges.sort_unstable_by_key(|range| range.start);
        for range in ranges {
            if let Some(last) = self.ranges.last_mut() {
                if range.start <= last.end {
                    last.end = last.end.max(range.end);
                    continue;
                }
            }
            self.ranges.push(range);
        }
    }

    pub(crate) fn intersect_intervals(&mut self, other: &Self) {
        let mut result = Vec::new();
        let (mut left, mut right) = (0, 0);
        while left < self.ranges.len() && right < other.ranges.len() {
            let start = self.ranges[left].start.max(other.ranges[right].start);
            let end = self.ranges[left].end.min(other.ranges[right].end);
            if start < end {
                result.push(start..end);
            }
            if self.ranges[left].end <= other.ranges[right].end {
                left += 1;
            } else {
                right += 1;
            }
        }
        self.ranges = result;
    }

    pub(crate) fn ranges(&self) -> impl Iterator<Item = Range<u64>> + '_ {
        self.ranges.iter().cloned()
    }
}

#[cfg(test)]
mod tests {
    use roaring::RoaringBitmap;

    use super::{super::page_reader::BitmapPageSet, BitmapCandidateIdIntervals};

    fn pages(ids: &[u32]) -> BitmapPageSet {
        BitmapPageSet {
            pages: vec![(0, ids.iter().copied().collect::<RoaringBitmap>())],
        }
    }

    #[test]
    fn promotes_and_joins_adjacent_ids() {
        let mut intervals = BitmapCandidateIdIntervals::empty();
        intervals.union(&pages(&[1000, 1001, 1003, 1004]));
        intervals.union(&pages(&[1002]));
        assert_eq!(intervals.ranges().collect::<Vec<_>>(), vec![1000..1005]);
    }

    #[test]
    fn intersects_storage_ids_with_candidate_range() {
        let mut intervals = BitmapCandidateIdIntervals::new(1000, 1010);
        intervals.intersect(&pages(&[1001, 1002, 1004, 1005, 1006, 1009]));
        assert_eq!(
            intervals.ranges().collect::<Vec<_>>(),
            vec![1001..1003, 1004..1007, 1009..1010]
        );
    }
}
