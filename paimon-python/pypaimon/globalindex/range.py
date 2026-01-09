################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""Range utilities for global index."""

from dataclasses import dataclass
from typing import List


@dataclass
class Range:
    """Represents a range [from_, to] inclusive."""

    from_: int
    to: int

    def __post_init__(self):
        if self.from_ > self.to:
            raise ValueError(f"Invalid range: from ({self.from_}) > to ({self.to})")

    def contains(self, value: int) -> bool:
        """Check if the range contains the given value."""
        return self.from_ <= value <= self.to

    def overlaps(self, other: 'Range') -> bool:
        """Check if this range overlaps with another range."""
        return Range.intersect(self.from_, self.to, other.from_, other.to)

    def exclude(self, ranges: List['Range']) -> List['Range']:
        """
        Exclude the given ranges from this range.
        Returns a list of non-overlapping ranges.
        """
        if not ranges:
            return [self]

        # Sort ranges by start
        sorted_ranges = sorted(ranges, key=lambda r: r.from_)
        result = []
        current_start = self.from_

        for r in sorted_ranges:
            if r.to < current_start:
                # Range is completely before current position, skip
                continue
            if r.from_ > self.to:
                # Range is completely after our range, stop
                break

            if r.from_ > current_start:
                # There's a gap before this range
                result.append(Range(current_start, min(r.from_ - 1, self.to)))

            # Move current_start past this range
            current_start = max(current_start, r.to + 1)

        # Add remaining part after all ranges
        if current_start <= self.to:
            result.append(Range(current_start, self.to))

        return result

    @staticmethod
    def intersect(start1: int, end1: int, start2: int, end2: int) -> bool:
        """Check if two ranges intersect."""
        return not (end1 < start2 or end2 < start1)

    @staticmethod
    def sort_and_merge_overlap(ranges: List['Range'], merge: bool = True) -> List['Range']:
        """
        Sort ranges and optionally merge overlapping ones.
        
        Args:
            ranges: List of ranges to sort and merge
            merge: If True, merge overlapping ranges
            
        Returns:
            Sorted (and optionally merged) list of ranges
        """
        if not ranges:
            return []

        # Sort by start
        sorted_ranges = sorted(ranges, key=lambda r: r.from_)

        if not merge:
            return sorted_ranges

        # Merge overlapping ranges
        result = [sorted_ranges[0]]
        for r in sorted_ranges[1:]:
            last = result[-1]
            if r.from_ <= last.to + 1:
                # Merge with last range
                result[-1] = Range(last.from_, max(last.to, r.to))
            else:
                result.append(r)

        return result

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Range):
            return False
        return self.from_ == other.from_ and self.to == other.to

    def __hash__(self) -> int:
        return hash((self.from_, self.to))

    def __repr__(self) -> str:
        return f"Range({self.from_}, {self.to})"
