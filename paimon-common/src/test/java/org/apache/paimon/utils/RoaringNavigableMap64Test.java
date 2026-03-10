/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.utils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RoaringNavigableMap64}. */
public class RoaringNavigableMap64Test {

    @Test
    public void testAddRangeBasic() {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        bitmap.addRange(new Range(5, 10));

        // Verify the range [5, 10] is added (inclusive on both ends)
        assertThat(bitmap.getLongCardinality()).isEqualTo(6);
        assertThat(bitmap.contains(4)).isFalse();
        assertThat(bitmap.contains(5)).isTrue();
        assertThat(bitmap.contains(7)).isTrue();
        assertThat(bitmap.contains(10)).isTrue();
        assertThat(bitmap.contains(11)).isFalse();
    }

    @Test
    public void testAddRangeSingleElement() {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        bitmap.addRange(new Range(100, 100));

        // A range where from == to should add exactly one element
        assertThat(bitmap.getLongCardinality()).isEqualTo(1);
        assertThat(bitmap.contains(99)).isFalse();
        assertThat(bitmap.contains(100)).isTrue();
        assertThat(bitmap.contains(101)).isFalse();
    }

    @Test
    public void testAddRangeMultipleNonOverlapping() {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        bitmap.addRange(new Range(0, 5));
        bitmap.addRange(new Range(10, 15));
        bitmap.addRange(new Range(20, 25));

        // Verify cardinality: 6 + 6 + 6 = 18
        assertThat(bitmap.getLongCardinality()).isEqualTo(18);

        // Verify gaps are not filled
        assertThat(bitmap.contains(6)).isFalse();
        assertThat(bitmap.contains(9)).isFalse();
        assertThat(bitmap.contains(16)).isFalse();
        assertThat(bitmap.contains(19)).isFalse();

        // Verify ranges contain expected values
        assertThat(bitmap.contains(0)).isTrue();
        assertThat(bitmap.contains(5)).isTrue();
        assertThat(bitmap.contains(10)).isTrue();
        assertThat(bitmap.contains(15)).isTrue();
        assertThat(bitmap.contains(20)).isTrue();
        assertThat(bitmap.contains(25)).isTrue();

        // Verify toRangeList reconstructs the ranges correctly
        List<Range> ranges = bitmap.toRangeList();
        assertThat(ranges).hasSize(3);
        assertThat(ranges.get(0)).isEqualTo(new Range(0, 5));
        assertThat(ranges.get(1)).isEqualTo(new Range(10, 15));
        assertThat(ranges.get(2)).isEqualTo(new Range(20, 25));
    }

    @Test
    public void testAddRangeLargeValues() {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        // Test with values beyond Integer.MAX_VALUE
        long start = Integer.MAX_VALUE + 100L;
        long end = Integer.MAX_VALUE + 200L;
        bitmap.addRange(new Range(start, end));

        assertThat(bitmap.getLongCardinality()).isEqualTo(101);
        assertThat(bitmap.contains(start - 1)).isFalse();
        assertThat(bitmap.contains(start)).isTrue();
        assertThat(bitmap.contains(start + 50)).isTrue();
        assertThat(bitmap.contains(end)).isTrue();
        assertThat(bitmap.contains(end + 1)).isFalse();

        // Verify iteration order
        List<Long> values = new ArrayList<>();
        bitmap.iterator().forEachRemaining(values::add);
        assertThat(values).hasSize(101);
        assertThat(values.get(0)).isEqualTo(start);
        assertThat(values.get(100)).isEqualTo(end);
    }

    @Test
    public void testIntersectsRangeBasic() {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        bitmap.addRange(new Range(10, 20));

        // Exact overlap
        assertThat(bitmap.intersectsRange(10, 20)).isTrue();
        // Partial overlap at start
        assertThat(bitmap.intersectsRange(5, 15)).isTrue();
        // Partial overlap at end
        assertThat(bitmap.intersectsRange(15, 25)).isTrue();
        // Query range fully contains bitmap range
        assertThat(bitmap.intersectsRange(0, 100)).isTrue();
        // Query range fully inside bitmap range
        assertThat(bitmap.intersectsRange(12, 18)).isTrue();
        // Single point overlap at boundary
        assertThat(bitmap.intersectsRange(10, 10)).isTrue();
        assertThat(bitmap.intersectsRange(20, 20)).isTrue();
        // No overlap -- below
        assertThat(bitmap.intersectsRange(0, 9)).isFalse();
        // No overlap -- above
        assertThat(bitmap.intersectsRange(21, 30)).isFalse();
    }

    @Test
    public void testIntersectsRangeEmptyBitmap() {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        assertThat(bitmap.intersectsRange(0, 100)).isFalse();
    }

    @Test
    public void testIntersectsRangeDisjointRanges() {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        bitmap.addRange(new Range(0, 5));
        bitmap.addRange(new Range(20, 25));

        // Query falls in the gap
        assertThat(bitmap.intersectsRange(6, 19)).isFalse();
        // Query overlaps first range only
        assertThat(bitmap.intersectsRange(3, 8)).isTrue();
        // Query overlaps second range only
        assertThat(bitmap.intersectsRange(18, 22)).isTrue();
        // Query spans both ranges
        assertThat(bitmap.intersectsRange(4, 21)).isTrue();
    }

    @Test
    public void testIntersectsRangeWithSingleElement() {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        bitmap.add(42);

        assertThat(bitmap.intersectsRange(42, 42)).isTrue();
        assertThat(bitmap.intersectsRange(40, 44)).isTrue();
        assertThat(bitmap.intersectsRange(0, 41)).isFalse();
        assertThat(bitmap.intersectsRange(43, 100)).isFalse();
    }

    @Test
    public void testIntersectsRangeStartAtZero() {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        bitmap.add(0);
        bitmap.add(1);

        assertThat(bitmap.intersectsRange(0, 0)).isTrue();
        assertThat(bitmap.intersectsRange(0, 1)).isTrue();
        assertThat(bitmap.intersectsRange(2, 5)).isFalse();
    }

    @Test
    public void testIntersectsRangeInvalidArgument() {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        assertThatThrownBy(() -> bitmap.intersectsRange(10, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rangeMin");
    }

    @Test
    public void testToArrayInRangeBasic() {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        bitmap.addRange(new Range(10, 20));

        // Exact range
        assertThat(bitmap.toArrayInRange(10, 20)).hasSize(11);
        // Sub-range
        assertThat(bitmap.toArrayInRange(12, 15)).containsExactly(12L, 13L, 14L, 15L);
        // Wider range
        assertThat(bitmap.toArrayInRange(0, 100)).hasSize(11);
        // No overlap
        assertThat(bitmap.toArrayInRange(0, 9)).isEmpty();
        assertThat(bitmap.toArrayInRange(21, 30)).isEmpty();
        // Single element boundary
        assertThat(bitmap.toArrayInRange(10, 10)).containsExactly(10L);
        assertThat(bitmap.toArrayInRange(20, 20)).containsExactly(20L);
    }

    @Test
    public void testToArrayInRangeDisjointBitmap() {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        bitmap.add(5);
        bitmap.add(10);
        bitmap.add(100);

        // Range covers only some elements
        assertThat(bitmap.toArrayInRange(5, 10)).containsExactly(5L, 10L);
        assertThat(bitmap.toArrayInRange(6, 99)).containsExactly(10L);
        assertThat(bitmap.toArrayInRange(0, 1000)).containsExactly(5L, 10L, 100L);
    }

    @Test
    public void testToArrayInRangeEmpty() {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        assertThat(bitmap.toArrayInRange(0, 100)).isEmpty();
    }

    @Test
    public void testToArrayInRangeStartAtZero() {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        bitmap.add(0);
        bitmap.add(1);
        bitmap.add(2);

        assertThat(bitmap.toArrayInRange(0, 1)).containsExactly(0L, 1L);
        assertThat(bitmap.toArrayInRange(0, 0)).containsExactly(0L);
    }

    @Test
    public void testToArrayInRangeInvalidArgument() {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        assertThatThrownBy(() -> bitmap.toArrayInRange(10, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rangeMin");
    }
}
