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
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/prometheus-community/parquet-common/storage"
)

func buildFile[T any](t testing.TB, rows []T) storage.ParquetShard {
	buf := bytes.NewBuffer(nil)
	w := parquet.NewGenericWriter[T](buf, parquet.PageBufferSize(10))
	for _, row := range rows {
		if _, err := w.Write([]T{row}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	bkt, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	reader := bytes.NewReader(buf.Bytes())
	require.NoError(t, bkt.Upload(context.Background(), "pipe/0.labels.parquet", reader))

	// Lets create a mocked chunks file
	_, err = reader.Seek(0, io.SeekStart)
	require.NoError(t, err)
	require.NoError(t, bkt.Upload(context.Background(), "pipe/0.chunks.parquet", reader))

	bucketOpener := storage.NewParquetBucketOpener(bkt)
	shard, err := storage.NewParquetShardOpener(
		context.Background(), "pipe", bucketOpener, bucketOpener, 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	return shard
}

func mustNewFastRegexMatcher(t testing.TB, s string) *labels.FastRegexMatcher {
	res, err := labels.NewFastRegexMatcher(s)
	if err != nil {
		t.Fatalf("unable to build fast regex matcher: %s", err)
	}
	return res
}

func BenchmarkConstraints(b *testing.B) {
	type s struct {
		A      string `parquet:",optional,dict"`
		B      string `parquet:",optional,dict"`
		Random string `parquet:",optional,dict"`
	}

	var rows []s

	for a := 0; a < 500; a++ {
		for b := 0; b < 500; b++ {
			rows = append(rows, s{
				A:      strings.Repeat(strconv.FormatInt(int64(a), 10), 20)[:20],
				B:      strings.Repeat(strconv.FormatInt(int64(b), 10), 20)[:20],
				Random: strings.Repeat(strconv.FormatInt(int64(100*a+b), 10), 20)[:20],
			})
		}
	}

	shard := buildFile(b, rows)

	tests := []struct {
		c []Constraint
	}{
		{
			c: []Constraint{
				Equal("A", parquet.ValueOf(rows[0].A)),
				Equal("B", parquet.ValueOf(rows[0].B)),
				Equal("Random", parquet.ValueOf(rows[0].Random)),
			},
		},
		{
			c: []Constraint{
				Equal("A", parquet.ValueOf(rows[len(rows)-1].A)),
				Equal("B", parquet.ValueOf(rows[len(rows)-1].B)),
				Equal("Random", parquet.ValueOf(rows[len(rows)-1].Random)),
			},
		},
		{
			c: []Constraint{
				Equal("A", parquet.ValueOf(rows[0].A)),
				Equal("B", parquet.ValueOf(rows[0].B)),
				Regex("Random", mustNewFastRegexMatcher(b, rows[0].Random)),
			},
		},
		{
			c: []Constraint{
				Equal("A", parquet.ValueOf(rows[len(rows)-1].A)),
				Equal("B", parquet.ValueOf(rows[len(rows)-1].B)),
				Regex("Random", mustNewFastRegexMatcher(b, rows[len(rows)-1].Random)),
			},
		},
	}

	for _, tt := range tests {
		b.Run(fmt.Sprintf("%s", tt.c), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				if err := Initialize(shard.LabelsFile(), tt.c...); err != nil {
					b.Fatal(err)
				}
				for i := range shard.LabelsFile().RowGroups() {
					rr, err := Filter(context.Background(), shard, i, tt.c...)
					if err != nil {
						b.Fatal(err)
					}
					require.NotNil(b, rr)
				}
			}
		})
	}
}

func TestContextCancelled(t *testing.T) {
	type s struct {
		A string `parquet:",optional,dict"`
	}

	var rows []s

	for a := 0; a < 50000; a++ {
		rows = append(rows, s{
			A: strings.Repeat(strconv.FormatInt(int64(a), 10), 20)[:20],
		})
	}

	shard := buildFile(t, rows)

	for _, c := range []Constraint{
		Equal("A", parquet.ValueOf(rows[len(rows)-1].A)),
		Regex("A", mustNewFastRegexMatcher(t, rows[len(rows)-1].A)),
		Not(Equal("A", parquet.ValueOf(rows[len(rows)-1].A))),
	} {
		if err := Initialize(shard.LabelsFile(), c); err != nil {
			t.Fatal(err)
		}

		for i := range shard.LabelsFile().RowGroups() {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err := Filter(ctx, shard, i, c)
			require.ErrorContains(t, err, "context canceled")
		}
	}
}

func TestFilter(t *testing.T) {
	type expectation struct {
		constraints []Constraint
		expect      []RowRange
	}
	type testcase[T any] struct {
		rows         []T
		expectations []expectation
	}

	t.Run("", func(t *testing.T) {
		type s struct {
			A string `parquet:",optional,dict"`
			B string `parquet:",optional,dict"`
			C string `parquet:",optional,dict"`
		}
		for _, tt := range []testcase[s]{
			{
				rows: []s{
					{A: "1", B: "2", C: "a"},
					{A: "3", B: "4", C: "b"},
					{A: "7", B: "12", C: "c"},
					{A: "9", B: "22", C: "d"},
					{A: "0", B: "1", C: "e"},
					{A: "7", B: "1", C: "f"},
					{A: "7", B: "1", C: "g"},
					{A: "0", B: "1", C: "h"},
				},
				expectations: []expectation{
					{
						constraints: []Constraint{
							Equal("A", parquet.ValueOf("7")),
							Equal("C", parquet.ValueOf("g")),
						},
						expect: []RowRange{
							{From: 6, Count: 1},
						},
					},
					{
						constraints: []Constraint{
							Equal("A", parquet.ValueOf("7")),
						},
						expect: []RowRange{
							{From: 2, Count: 1},
							{From: 5, Count: 2},
						},
					},
					{
						constraints: []Constraint{
							Equal("A", parquet.ValueOf("7")), Not(Equal("B", parquet.ValueOf("1"))),
						},
						expect: []RowRange{
							{From: 2, Count: 1},
						},
					},
					{
						constraints: []Constraint{
							Equal("A", parquet.ValueOf("7")), Not(Equal("C", parquet.ValueOf("c"))),
						},
						expect: []RowRange{
							{From: 5, Count: 2},
						},
					},
					{
						constraints: []Constraint{
							Not(Equal("A", parquet.ValueOf("227"))),
						},
						expect: []RowRange{
							{From: 0, Count: 8},
						},
					},
					{
						constraints: []Constraint{
							Regex("C", mustNewFastRegexMatcher(t, "a|c|d")),
						},
						expect: []RowRange{
							{From: 0, Count: 1},
							{From: 2, Count: 2},
						},
					},
				},
			},
			{
				rows: []s{
					{A: "1", B: "2"},
					{A: "1", B: "3"},
					{A: "1", B: "4"},
					{A: "1", B: "4"},
					{A: "1", B: "5"},
					{A: "1", B: "5"},
					{A: "2", B: "5"},
					{A: "2", B: "5"},
					{A: "2", B: "5"},
					{A: "3", B: "5"},
					{A: "3", B: "6"},
					{A: "3", B: "2"},
				},
				expectations: []expectation{
					{
						constraints: []Constraint{
							Not(Equal("A", parquet.ValueOf("3"))),
						},
						expect: []RowRange{
							{From: 0, Count: 9},
						},
					},
					{
						constraints: []Constraint{
							Not(Equal("A", parquet.ValueOf("3"))),
							Equal("B", parquet.ValueOf("5")),
						},
						expect: []RowRange{
							{From: 4, Count: 5},
						},
					},
					{
						constraints: []Constraint{
							Not(Equal("A", parquet.ValueOf("3"))),
							Not(Equal("A", parquet.ValueOf("1"))),
						},
						expect: []RowRange{
							{From: 6, Count: 3},
						},
					},
					{
						constraints: []Constraint{
							Equal("A", parquet.ValueOf("2")),
							Not(Equal("B", parquet.ValueOf("5"))),
						},
						expect: []RowRange{},
					},
					{
						constraints: []Constraint{
							Equal("A", parquet.ValueOf("2")),
							Not(Equal("B", parquet.ValueOf("5"))),
						},
						expect: []RowRange{},
					},
					{
						constraints: []Constraint{
							Equal("A", parquet.ValueOf("3")),
							Not(Equal("B", parquet.ValueOf("2"))),
						},
						expect: []RowRange{
							{From: 9, Count: 2},
						},
					},
				},
			},
			{
				rows: []s{
					{A: "1", B: "1"},
					{A: "1", B: "2"},
					{A: "2", B: "1"},
					{A: "2", B: "2"},
					{A: "1", B: "1"},
					{A: "1", B: "2"},
					{A: "2", B: "1"},
					{A: "2", B: "2"},
				},
				expectations: []expectation{
					{
						constraints: []Constraint{
							Not(Equal("A", parquet.ValueOf("1"))),
							Not(Equal("B", parquet.ValueOf("2"))),
						},
						expect: []RowRange{
							{From: 2, Count: 1},
							{From: 6, Count: 1},
						},
					},
				},
			},
			{
				rows: []s{
					{C: "foo"},
					{C: "bar"},
					{C: "foo"},
					{C: "buz"},
				},
				expectations: []expectation{
					{
						constraints: []Constraint{
							Regex("C", mustNewFastRegexMatcher(t, "f.*")),
						},
						expect: []RowRange{
							{From: 0, Count: 1},
							{From: 2, Count: 1},
						},
					},
					{
						constraints: []Constraint{
							Regex("C", mustNewFastRegexMatcher(t, "b.*")),
						},
						expect: []RowRange{
							{From: 1, Count: 1},
							{From: 3, Count: 1},
						},
					},
					{
						constraints: []Constraint{
							Regex("C", mustNewFastRegexMatcher(t, "f.*|b.*")),
						},
						expect: []RowRange{
							{From: 0, Count: 4},
						},
					},
				},
			},
			{
				rows: []s{
					{A: "1", B: "1"},
					{A: "1", B: "2"},
					{A: "2", B: "1"},
					{A: "2", B: "2"},
					{A: "1", B: "1"},
				},
				expectations: []expectation{
					{
						constraints: []Constraint{
							Equal("A", parquet.ValueOf("1")),
							Equal("B", parquet.ValueOf("1")),
						},
						expect: []RowRange{
							{From: 0, Count: 1},
							{From: 4, Count: 1},
						},
					},
				},
			},
			{
				rows: []s{
					{A: "1", B: "1"},
					{A: "1", B: "2"},
				},
				expectations: []expectation{
					{
						constraints: []Constraint{
							Equal("A", parquet.ValueOf("1")),
							Equal("None", parquet.ValueOf("?")),
						},
						expect: []RowRange{},
					},
					{
						constraints: []Constraint{
							Equal("A", parquet.ValueOf("1")),
							Equal("None", parquet.ValueOf("")),
						},
						expect: []RowRange{
							{From: 0, Count: 2},
						},
					},
					{
						constraints: []Constraint{
							Equal("A", parquet.ValueOf("1")),
							Regex("None", mustNewFastRegexMatcher(t, "f.*|b.*")),
						},
						expect: []RowRange{},
					},
					{
						constraints: []Constraint{
							Equal("A", parquet.ValueOf("1")),
							Regex("None", mustNewFastRegexMatcher(t, "f.*|b.*|")),
						},
						expect: []RowRange{
							{From: 0, Count: 2},
						},
					},
				},
			},
			{
				rows: []s{
					{A: "1", C: "a"},
					{A: "2", C: "b"},
					{A: "2"},
					{A: "3", C: "b"},
					{A: "4"},
					{A: "5"},
				},
				expectations: []expectation{
					{
						constraints: []Constraint{
							Equal("C", parquet.ValueOf("")),
						},
						expect: []RowRange{
							{From: 2, Count: 1},
							{From: 4, Count: 2},
						},
					},
					{
						constraints: []Constraint{
							Equal("A", parquet.ValueOf("2")),
							Equal("C", parquet.ValueOf("")),
						},
						expect: []RowRange{
							{From: 2, Count: 1},
						},
					},
				},
			},
		} {

			shard := buildFile(t, tt.rows)
			for _, expectation := range tt.expectations {
				t.Run("", func(t *testing.T) {
					if err := Initialize(shard.LabelsFile(), expectation.constraints...); err != nil {
						t.Fatal(err)
					}
					for i := range shard.LabelsFile().RowGroups() {
						rr, err := Filter(context.Background(), shard, i, expectation.constraints...)
						if err != nil {
							t.Fatal(err)
						}
						if !slices.Equal(rr, expectation.expect) {
							t.Fatalf("expected %+v, got %+v", expectation.expect, rr)
						}
					}
				})
			}
		}
	})
}
