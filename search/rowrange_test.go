// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package search

import (
	"slices"
	"testing"
)

func TestIntersect(t *testing.T) {
	for _, tt := range []struct{ lhs, rhs, expect []rowRange }{
		{
			lhs:    []rowRange{{from: 0, count: 4}},
			rhs:    []rowRange{{from: 2, count: 6}},
			expect: []rowRange{{from: 2, count: 2}},
		},
		{
			lhs:    []rowRange{{from: 0, count: 4}},
			rhs:    []rowRange{{from: 6, count: 8}},
			expect: []rowRange{},
		},
		{
			lhs:    []rowRange{{from: 0, count: 4}},
			rhs:    []rowRange{{from: 0, count: 4}},
			expect: []rowRange{{from: 0, count: 4}},
		},
		{
			lhs:    []rowRange{{from: 0, count: 4}, {from: 8, count: 2}},
			rhs:    []rowRange{{from: 2, count: 9}},
			expect: []rowRange{{from: 2, count: 2}, {from: 8, count: 2}},
		},
		{
			lhs:    []rowRange{{from: 0, count: 1}, {from: 4, count: 1}},
			rhs:    []rowRange{{from: 2, count: 1}, {from: 6, count: 1}},
			expect: []rowRange{},
		},
		{
			lhs:    []rowRange{{from: 0, count: 2}, {from: 2, count: 2}},
			rhs:    []rowRange{{from: 1, count: 2}, {from: 3, count: 2}},
			expect: []rowRange{{from: 1, count: 3}},
		},
		{
			lhs:    []rowRange{{from: 0, count: 2}, {from: 5, count: 2}},
			rhs:    []rowRange{{from: 0, count: 10}},
			expect: []rowRange{{from: 0, count: 2}, {from: 5, count: 2}},
		},
		{
			lhs:    []rowRange{{from: 0, count: 2}, {from: 3, count: 1}, {from: 5, count: 2}, {from: 12, count: 10}},
			rhs:    []rowRange{{from: 0, count: 10}, {from: 15, count: 32}},
			expect: []rowRange{{from: 0, count: 2}, {from: 3, count: 1}, {from: 5, count: 2}, {from: 15, count: 7}},
		},
		{
			lhs:    []rowRange{},
			rhs:    []rowRange{{from: 0, count: 10}},
			expect: []rowRange{},
		},
		{
			lhs:    []rowRange{{from: 0, count: 10}},
			rhs:    []rowRange{},
			expect: []rowRange{},
		},
		{
			lhs:    []rowRange{},
			rhs:    []rowRange{},
			expect: []rowRange{},
		},
		{
			lhs:    []rowRange{{from: 0, count: 2}},
			rhs:    []rowRange{{from: 0, count: 1}, {from: 1, count: 1}, {from: 2, count: 1}},
			expect: []rowRange{{from: 0, count: 2}},
		},
	} {
		t.Run("", func(t *testing.T) {
			if res := intersectRowRanges(tt.lhs, tt.rhs); !slices.Equal(res, tt.expect) {
				t.Fatalf("Expected %q to match %q", res, tt.expect)
			}
		})
	}
}

func TestSimplify(t *testing.T) {
	for _, tt := range []struct{ in, expect []rowRange }{
		{
			in: []rowRange{
				{from: 0, count: 15},
				{from: 4, count: 4},
			},
			expect: []rowRange{
				{from: 0, count: 15},
			},
		},
		{
			in: []rowRange{
				{from: 4, count: 4},
				{from: 4, count: 2},
			},
			expect: []rowRange{
				{from: 4, count: 4},
			},
		},
		{
			in: []rowRange{
				{from: 0, count: 4},
				{from: 1, count: 5},
				{from: 8, count: 10},
			},
			expect: []rowRange{
				{from: 0, count: 6},
				{from: 8, count: 10},
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
