// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package search

import (
	"slices"
	"testing"
)

func TestIntersect(t *testing.T) {
	for _, tt := range []struct{ lhs, rhs, expect []RowRange }{
		{
			lhs:    []RowRange{{From: 0, Count: 4}},
			rhs:    []RowRange{{From: 2, Count: 6}},
			expect: []RowRange{{From: 2, Count: 2}},
		},
		{
			lhs:    []RowRange{{From: 0, Count: 4}},
			rhs:    []RowRange{{From: 6, Count: 8}},
			expect: []RowRange{},
		},
		{
			lhs:    []RowRange{{From: 0, Count: 4}},
			rhs:    []RowRange{{From: 0, Count: 4}},
			expect: []RowRange{{From: 0, Count: 4}},
		},
		{
			lhs:    []RowRange{{From: 0, Count: 4}, {From: 8, Count: 2}},
			rhs:    []RowRange{{From: 2, Count: 9}},
			expect: []RowRange{{From: 2, Count: 2}, {From: 8, Count: 2}},
		},
		{
			lhs:    []RowRange{{From: 0, Count: 1}, {From: 4, Count: 1}},
			rhs:    []RowRange{{From: 2, Count: 1}, {From: 6, Count: 1}},
			expect: []RowRange{},
		},
		{
			lhs:    []RowRange{{From: 0, Count: 2}, {From: 2, Count: 2}},
			rhs:    []RowRange{{From: 1, Count: 2}, {From: 3, Count: 2}},
			expect: []RowRange{{From: 1, Count: 3}},
		},
		{
			lhs:    []RowRange{{From: 0, Count: 2}, {From: 5, Count: 2}},
			rhs:    []RowRange{{From: 0, Count: 10}},
			expect: []RowRange{{From: 0, Count: 2}, {From: 5, Count: 2}},
		},
		{
			lhs:    []RowRange{{From: 0, Count: 2}, {From: 3, Count: 1}, {From: 5, Count: 2}, {From: 12, Count: 10}},
			rhs:    []RowRange{{From: 0, Count: 10}, {From: 15, Count: 32}},
			expect: []RowRange{{From: 0, Count: 2}, {From: 3, Count: 1}, {From: 5, Count: 2}, {From: 15, Count: 7}},
		},
		{
			lhs:    []RowRange{},
			rhs:    []RowRange{{From: 0, Count: 10}},
			expect: []RowRange{},
		},
		{
			lhs:    []RowRange{{From: 0, Count: 10}},
			rhs:    []RowRange{},
			expect: []RowRange{},
		},
		{
			lhs:    []RowRange{},
			rhs:    []RowRange{},
			expect: []RowRange{},
		},
		{
			lhs:    []RowRange{{From: 0, Count: 2}},
			rhs:    []RowRange{{From: 0, Count: 1}, {From: 1, Count: 1}, {From: 2, Count: 1}},
			expect: []RowRange{{From: 0, Count: 2}},
		},
	} {
		t.Run("", func(t *testing.T) {
			if res := intersectRowRanges(tt.lhs, tt.rhs); !slices.Equal(res, tt.expect) {
				t.Fatalf("Expected %v to match %v", res, tt.expect)
			}
		})
	}
}

func TestComplement(t *testing.T) {
	for _, tt := range []struct{ lhs, rhs, expect []RowRange }{
		{
			lhs:    []RowRange{{From: 4, Count: 3}},
			rhs:    []RowRange{{From: 2, Count: 1}, {From: 5, Count: 2}},
			expect: []RowRange{{From: 2, Count: 1}},
		},
		{
			lhs:    []RowRange{{From: 2, Count: 4}},
			rhs:    []RowRange{{From: 0, Count: 7}},
			expect: []RowRange{{From: 0, Count: 2}, {From: 6, Count: 1}},
		},
		{
			lhs:    []RowRange{{From: 2, Count: 4}},
			rhs:    []RowRange{{From: 3, Count: 7}},
			expect: []RowRange{{From: 6, Count: 4}},
		},
		{
			lhs:    []RowRange{{From: 8, Count: 10}},
			rhs:    []RowRange{{From: 3, Count: 7}},
			expect: []RowRange{{From: 3, Count: 5}},
		},
		{
			lhs:    []RowRange{{From: 16, Count: 10}},
			rhs:    []RowRange{{From: 3, Count: 7}},
			expect: []RowRange{{From: 3, Count: 7}},
		},
		{
			lhs:    []RowRange{{From: 1, Count: 2}, {From: 4, Count: 2}},
			rhs:    []RowRange{{From: 2, Count: 2}, {From: 5, Count: 8}},
			expect: []RowRange{{From: 3, Count: 1}, {From: 6, Count: 7}},
		},
		// Empty input cases
		{
			lhs:    []RowRange{},
			rhs:    []RowRange{{From: 1, Count: 5}},
			expect: []RowRange{{From: 1, Count: 5}},
		},
		{
			lhs:    []RowRange{{From: 1, Count: 5}},
			rhs:    []RowRange{},
			expect: []RowRange{},
		},
		{
			lhs:    []RowRange{},
			rhs:    []RowRange{},
			expect: []RowRange{},
		},
		// Adjacent ranges
		{
			lhs:    []RowRange{{From: 1, Count: 3}},
			rhs:    []RowRange{{From: 1, Count: 3}, {From: 4, Count: 2}},
			expect: []RowRange{{From: 4, Count: 2}},
		},
		// Ranges with gaps
		{
			lhs:    []RowRange{{From: 1, Count: 2}, {From: 5, Count: 2}},
			rhs:    []RowRange{{From: 0, Count: 8}},
			expect: []RowRange{{From: 0, Count: 1}, {From: 3, Count: 2}, {From: 7, Count: 1}},
		},
		// Zero-count ranges
		{
			lhs:    []RowRange{{From: 1, Count: 0}},
			rhs:    []RowRange{{From: 1, Count: 5}},
			expect: []RowRange{{From: 1, Count: 5}},
		},
		// Completely disjoint ranges
		{
			lhs:    []RowRange{{From: 1, Count: 2}},
			rhs:    []RowRange{{From: 5, Count: 2}},
			expect: []RowRange{{From: 5, Count: 2}},
		},
		// Multiple overlapping ranges
		{
			lhs:    []RowRange{{From: 1, Count: 3}, {From: 4, Count: 3}, {From: 8, Count: 2}},
			rhs:    []RowRange{{From: 0, Count: 11}},
			expect: []RowRange{{From: 0, Count: 1}, {From: 7, Count: 1}, {From: 10, Count: 1}},
		},
	} {
		t.Run("", func(t *testing.T) {
			if res := complementRowRanges(tt.lhs, tt.rhs); !slices.Equal(res, tt.expect) {
				t.Fatalf("Expected %v to match %v", res, tt.expect)
			}
		})
	}
}

func TestSimplify(t *testing.T) {
	for _, tt := range []struct{ in, expect []RowRange }{
		{
			in: []RowRange{
				{From: 0, Count: 15},
				{From: 4, Count: 4},
			},
			expect: []RowRange{
				{From: 0, Count: 15},
			},
		},
		{
			in: []RowRange{
				{From: 4, Count: 4},
				{From: 4, Count: 2},
			},
			expect: []RowRange{
				{From: 4, Count: 4},
			},
		},
		{
			in: []RowRange{
				{From: 0, Count: 4},
				{From: 1, Count: 5},
				{From: 8, Count: 10},
			},
			expect: []RowRange{
				{From: 0, Count: 6},
				{From: 8, Count: 10},
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			if res := simplify(tt.in); !slices.Equal(res, tt.expect) {
				t.Fatalf("Expected %v to match %v", res, tt.expect)
			}
		})
	}
}

func TestOverlaps(t *testing.T) {
	for _, tt := range []struct {
		a, b   RowRange
		expect bool
	}{
		// Identical ranges
		{
			a:      RowRange{From: 0, Count: 5},
			b:      RowRange{From: 0, Count: 5},
			expect: true,
		},
		// Completely disjoint ranges
		{
			a:      RowRange{From: 0, Count: 3},
			b:      RowRange{From: 5, Count: 3},
			expect: false,
		},
		// Adjacent ranges (should not overlap as ranges are half-open)
		{
			a:      RowRange{From: 0, Count: 3},
			b:      RowRange{From: 3, Count: 3},
			expect: false,
		},
		// One range completely contains the other
		{
			a:      RowRange{From: 0, Count: 10},
			b:      RowRange{From: 2, Count: 5},
			expect: true,
		},
		// Partial overlap from left
		{
			a:      RowRange{From: 0, Count: 5},
			b:      RowRange{From: 3, Count: 5},
			expect: true,
		},
		// Partial overlap from right
		{
			a:      RowRange{From: 3, Count: 5},
			b:      RowRange{From: 0, Count: 5},
			expect: true,
		},
		// Zero-count ranges
		{
			a:      RowRange{From: 0, Count: 0},
			b:      RowRange{From: 0, Count: 5},
			expect: false,
		},
		{
			a:      RowRange{From: 0, Count: 5},
			b:      RowRange{From: 0, Count: 0},
			expect: false,
		},
		// Negative ranges (edge case)
		{
			a:      RowRange{From: -5, Count: 5},
			b:      RowRange{From: -3, Count: 5},
			expect: true,
		},
	} {
		t.Run("", func(t *testing.T) {
			if res := tt.a.Overlaps(tt.b); res != tt.expect {
				t.Fatalf("Expected %v.Overlaps(%v) to be %v, got %v", tt.a, tt.b, tt.expect, res)
			}
			// Test symmetry
			if res := tt.b.Overlaps(tt.a); res != tt.expect {
				t.Fatalf("Expected %v.Overlaps(%v) to be %v, got %v", tt.b, tt.a, tt.expect, res)
			}
		})
	}
}
