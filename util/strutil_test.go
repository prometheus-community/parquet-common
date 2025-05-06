package util

import (
	"reflect"
	"sort"
	"testing"
)

func TestMergeSlices(t *testing.T) {
	tests := []struct {
		name  string
		limit int
		input [][]string
		want  []string
	}{
		{
			name:  "empty input",
			limit: 0,
			input: nil,
			want:  nil,
		},
		{
			name:  "single slice",
			limit: 0,
			input: [][]string{{"a", "b", "c"}},
			want:  []string{"a", "b", "c"},
		},
		{
			name:  "two sorted slices with duplicates",
			limit: 0,
			input: [][]string{{"a", "b", "c"}, {"b", "c", "d"}},
			want:  []string{"a", "b", "c", "d"},
		},
		{
			name:  "limit truncates result",
			limit: 3,
			input: [][]string{{"a", "b", "c"}, {"d", "e"}},
			want:  []string{"a", "b", "c"},
		},
		{
			name:  "multiple sorted slices",
			limit: 0,
			input: [][]string{{"a"}, {"b"}, {"c"}, {"d"}},
			want:  []string{"a", "b", "c", "d"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MergeSlices(tt.limit, tt.input...)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMergeUnsortedSlices(t *testing.T) {
	tests := []struct {
		name  string
		limit int
		input [][]string
		want  []string
	}{
		{
			name:  "unsorted slices with duplicates",
			limit: 0,
			input: [][]string{{"c", "a"}, {"b", "a"}},
			want:  []string{"a", "b", "c"},
		},
		{
			name:  "limit applied to unsorted slices",
			limit: 2,
			input: [][]string{{"c", "a"}, {"b", "a"}},
			want:  []string{"a", "b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MergeUnsortedSlices(tt.limit, tt.input...)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
			// Ensure result is sorted
			if !sort.StringsAreSorted(got) {
				t.Errorf("result is not sorted: %v", got)
			}
		})
	}
}
