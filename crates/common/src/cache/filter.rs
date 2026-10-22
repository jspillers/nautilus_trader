// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Intersection of index bucket sets for filtered cache queries.

use ahash::AHashSet;
use smallvec::SmallVec;

// At most four index filters plus the lifecycle bucket
pub(super) type FilterSets<'a, K> = SmallVec<[&'a AHashSet<K>; 5]>;

// Filter sources resolved from an order or position query.
//
// Captures the three states of a multi-key index intersection without committing to an owned
// result set: no filters at all (the caller iterates the bucket directly), one or more filter
// sources resolved successfully (intersect them lazily), or one filter resolved to no entries
// at all (the result is unconditionally empty).
pub(super) enum FilterSources<'a, K> {
    Unfiltered,
    Empty,
    Sets(FilterSets<'a, K>),
}

// Intersects a non-empty collection of filter sources by sorting them ascending by length and
// driving the loop from the smallest set, collecting one `AHashSet` of matching keys.
fn intersect_filter_sources<K>(mut sources: FilterSets<'_, K>) -> AHashSet<K>
where
    K: Copy + Eq + std::hash::Hash,
{
    debug_assert!(!sources.is_empty());
    sources.sort_unstable_by_key(|s| s.len());
    let driver = sources[0];
    let rest = &sources[1..];

    driver
        .iter()
        .filter(|id| rest.iter().all(|s| s.contains(id)))
        .copied()
        .collect()
}

// Intersects `bucket` with one or more filter sources.
//
// For exactly one filter source, iterates the larger of (bucket, filter) and looks up in the
// smaller. The larger set scans linearly (HW-prefetcher friendly) and the smaller stays hot in
// cache, which empirically beats the size-ordered approach when the smaller filter is too
// large to fit in L1 (e.g., a 20k-entry venue filter against a 100k-entry bucket). For two or
// more filters the size-ordered driver is reinstated and the bucket joins the source list.
pub(super) fn intersect_pair_or_many<'a, K>(
    bucket: &'a AHashSet<K>,
    mut sources: FilterSets<'a, K>,
) -> AHashSet<K>
where
    K: Copy + Eq + std::hash::Hash,
{
    debug_assert!(!sources.is_empty());

    if sources.len() == 1 {
        let filter = sources[0];

        let (larger, smaller) = if bucket.len() >= filter.len() {
            (bucket, filter)
        } else {
            (filter, bucket)
        };

        return larger.intersection(smaller).copied().collect();
    }

    sources.push(bucket);
    intersect_filter_sources(sources)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn five_filter_sources_remain_inline_and_intersect_exactly() {
        let sets = [
            AHashSet::from_iter([1, 2, 3]),
            AHashSet::from_iter([2, 3]),
            AHashSet::from_iter([0, 2]),
            AHashSet::from_iter([2]),
            AHashSet::from_iter([2, 4, 6]),
        ];

        for count in 1..=4 {
            let filters: FilterSets<'_, u64> = sets[..count].iter().collect();
            assert!(!filters.spilled());
            let expected: AHashSet<_> = sets[4]
                .iter()
                .copied()
                .filter(|id| sets[..count].iter().all(|s| s.contains(id)))
                .collect();
            assert_eq!(intersect_pair_or_many(&sets[4], filters), expected);
        }
        let filters: FilterSets<'_, u64> = sets.iter().collect();
        assert!(!filters.spilled());
        assert_eq!(intersect_filter_sources(filters), AHashSet::from_iter([2]));
    }
}
