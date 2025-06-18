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
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	prom_storage "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/storage"
	"github.com/prometheus-community/parquet-common/util"
)

func TestMaterializeE2E(t *testing.T) {
	st := teststorage.New(t)
	ctx := context.Background()
	t.Cleanup(func() { _ = st.Close() })

	bkt, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = bkt.Close() })

	cfg := util.DefaultTestConfig()
	data := util.GenerateTestData(t, st, ctx, cfg)

	// Convert to Parquet
	shard := convertToParquet(t, ctx, bkt, data, st.Head(), nil, nil)

	t.Run("QueryByUniqueLabel", func(t *testing.T) {
		eq := Equal(schema.LabelToColumn("unique"), parquet.ValueOf("unique_0"))
		found := query(t, data.MinTime, data.MaxTime, shard, eq)
		require.Len(t, found, cfg.TotalMetricNames)

		for _, series := range found {
			require.Equal(t, series.Labels().Get("unique"), "unique_0")
			require.Contains(t, data.SeriesHash, series.Labels().Hash())
		}
	})

	t.Run("QueryByMetricName", func(t *testing.T) {
		for i := 0; i < 50; i++ {
			name := fmt.Sprintf("metric_%d", rand.Int()%cfg.TotalMetricNames)
			eq := Equal(schema.LabelToColumn(labels.MetricName), parquet.ValueOf(name))

			found := query(t, data.MinTime, data.MaxTime, shard, eq)
			require.Len(t, found, cfg.MetricsPerMetricName, fmt.Sprintf("metric_%d", i))

			for _, series := range found {
				require.Equal(t, series.Labels().Get(labels.MetricName), name)
				require.Contains(t, data.SeriesHash, series.Labels().Hash())

				totalSamples := 0
				ci := series.Iterator(nil)
				for ci.Next() {
					si := ci.At().Chunk.Iterator(nil)
					for si.Next() != chunkenc.ValNone {
						totalSamples++
					}
				}
				require.Equal(t, totalSamples, cfg.NumberOfSamples)
			}
		}
	})

	t.Run("QueryByTimeRange", func(t *testing.T) {
		colDuration := time.Hour
		c1 := Equal(schema.LabelToColumn(labels.MetricName), parquet.ValueOf("metric_0"))
		c2 := Equal(schema.LabelToColumn("unique"), parquet.ValueOf("unique_0"))

		// Test first column only
		found := query(t, data.MinTime, data.MinTime+colDuration.Milliseconds()-1, shard, c1, c2)
		require.Len(t, found, 1)
		require.Len(t, pullChunks(t, found[0]), 1)

		// Test first two columns
		found = query(t, data.MinTime, data.MinTime+(2*colDuration).Milliseconds()-1, shard, c1, c2)
		require.Len(t, found, 1)
		require.Len(t, pullChunks(t, found[0]), 2)

		// Query outside the range
		found = query(t, data.MinTime+(9*colDuration).Milliseconds(), data.MinTime+(10*colDuration).Milliseconds()-1, shard, c1, c2)
		require.Len(t, found, 0)
	})

	t.Run("ContextCancelled", func(t *testing.T) {
		s, err := shard.TSDBSchema()
		require.NoError(t, err)
		d := schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool())
		m, err := NewMaterializer(ctx, s, d, shard, 10, 10*1024)
		require.NoError(t, err)
		rr := []RowRange{{from: int64(0), count: shard.LabelsFile().RowGroups()[0].NumRows()}}
		ctx, cancel := context.WithCancel(ctx)
		cancel()
		_, err = m.Materialize(ctx, 0, data.MinTime, data.MaxTime, false, rr)
		require.ErrorContains(t, err, "context canceled")
	})

	t.Run("Should not race when multiples download multiples page in parallel", func(t *testing.T) {
		s, err := shard.TSDBSchema()
		require.NoError(t, err)
		d := schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool())
		m, err := NewMaterializer(ctx, s, d, shard, 10, -1)
		require.NoError(t, err)
		rr := []RowRange{{from: int64(0), count: shard.LabelsFile().RowGroups()[0].NumRows()}}
		set, err := m.Materialize(ctx, 0, data.MinTime, data.MaxTime, false, rr)
		require.NoError(t, err)
		set.Close()
	})
}

func convertToParquet(t testing.TB, ctx context.Context, bkt objstore.Bucket, data util.TestData, h convert.Convertible, convOpts []convert.ConvertOption, shardOpts []storage.ShardOption) storage.ParquetShard {
	defaultOpts := []convert.ConvertOption{
		convert.WithName("shard"),
		convert.WithColDuration(time.Hour), // force more than 1 data col
		convert.WithRowGroupSize(500),
		convert.WithPageBufferSize(300), // force creating multiples pages
	}
	convOpts = append(defaultOpts, convOpts...)
	shards, err := convert.ConvertTSDBBlock(
		ctx,
		bkt,
		data.MinTime,
		data.MaxTime,
		[]convert.Convertible{h},
		convOpts...,
	)
	require.NoError(t, err)
	require.Equal(t, 1, shards)

	bucketOpener := storage.NewParquetBucketOpener(bkt)
	shard, err := storage.NewParquetShardOpener(ctx, "shard", bucketOpener, bucketOpener, 0, shardOpts...)
	require.NoError(t, err)

	return shard
}

func query(t *testing.T, mint, maxt int64, shard storage.ParquetShard, constraints ...Constraint) []prom_storage.ChunkSeries {
	ctx := context.Background()
	for _, c := range constraints {
		require.NoError(t, c.init(shard.LabelsFile()))
	}

	s, err := shard.TSDBSchema()
	require.NoError(t, err)
	d := schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool())
	m, err := NewMaterializer(ctx, s, d, shard, 10, 10*1024)
	require.NoError(t, err)

	found := make([]prom_storage.ChunkSeries, 0, 100)
	for i, group := range shard.LabelsFile().RowGroups() {
		rr, err := Filter(context.Background(), group, constraints...)
		total := int64(0)
		for _, r := range rr {
			total += r.count
		}
		require.NoError(t, err)
		series, err := m.Materialize(ctx, i, mint, maxt, false, rr)
		require.NoError(t, err)
		found = append(found, pullSeries(t, series)...)
		series.Close()
	}
	return found
}

func pullChunks(t *testing.T, chnks prom_storage.ChunkSeries) []chunks.Meta {
	it := chnks.Iterator(nil)
	var metas []chunks.Meta
	for it.Next() {
		metas = append(metas, it.At())
	}
	if err := it.Err(); err != nil {
		t.Fatalf("error iterating chunks: %v", err)
	}
	return metas
}

func pullSeries(t *testing.T, series prom_storage.ChunkSeriesSet) []prom_storage.ChunkSeries {
	var found []prom_storage.ChunkSeries
	for series.Next() {
		found = append(found, series.At())
	}
	if err := series.Err(); err != nil {
		t.Fatalf("error iterating series: %v", err)
	}
	return found
}

func BenchmarkMaterialize(b *testing.B) {
	ctx := context.Background()
	st := teststorage.New(b)
	b.Cleanup(func() { _ = st.Close() })

	bkt, err := filesystem.NewBucket(b.TempDir())
	if err != nil {
		b.Fatal("error creating bucket: ", err)
	}
	b.Cleanup(func() { _ = bkt.Close() })

	cfg := util.DefaultTestConfig()
	cfg.NumberOfSamples = 1500 // non-trivial chunks
	data := util.GenerateTestData(b, st, ctx, cfg)
	bktw := &bucketWrapper{
		Bucket: bkt,
		// Simulate object store latency. The Materializer
		// can parallelize GetRange calls to help with this.
		readLatency: time.Millisecond * 5,
	}

	shard := convertToParquet(b, ctx, bktw, data, st.Head(), []convert.ConvertOption{
		// We are benchmarking a single row group, so let's fit it all in a single one
		convert.WithRowGroupSize(len(data.SeriesHash)),
	}, nil)

	s, err := shard.TSDBSchema()
	if err != nil {
		b.Fatal("error getting schema: ", err)
	}

	// We will benchmark the first row group only.
	totalRows := shard.LabelsFile().RowGroups()[0].NumRows()

	testCases := []struct {
		name        string
		rr          []RowRange
		concurrency int
		skipChunks  bool
	}{
		{
			name:        "AllRows",
			concurrency: 1,
			rr:          []RowRange{{from: 0, count: totalRows}},
		},
		{
			name:        "AllRowsConcur4",
			concurrency: 4,
			rr:          []RowRange{{from: 0, count: totalRows}},
		},
		{
			name:        "Interleaved",
			concurrency: 1,
			rr: []RowRange{
				{from: 0, count: totalRows / 10},
				{from: totalRows / 5, count: totalRows / 10},
				{from: totalRows * 2 / 5, count: totalRows / 10},
				{from: totalRows * 3 / 5, count: totalRows / 10},
				{from: totalRows * 4 / 5, count: totalRows / 10},
			},
		},
		{
			name:        "Sparse",
			concurrency: 1,
			rr: []RowRange{
				{from: 0, count: 50},
				{from: totalRows / 4, count: 50},
				{from: totalRows / 2, count: 50},
				{from: totalRows * 3 / 4, count: 50},
			},
		},
		{
			name:        "SingleRow",
			concurrency: 1,
			rr:          []RowRange{{from: totalRows / 2, count: 1}},
		},
		{
			name:        "SkipChunks",
			concurrency: 1,
			rr:          []RowRange{{from: 0, count: totalRows}},
			skipChunks:  true,
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			d := schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool())
			m, err := NewMaterializer(ctx, s, d, shard, tc.concurrency, 10*1024)
			if err != nil {
				b.Fatal("error creating materializer: ", err)
			}
			// Warm up
			s, err := m.Materialize(ctx, 0, data.MinTime, data.MaxTime, tc.skipChunks, tc.rr)
			require.NoError(b, err)
			s.Close()
			b.ReportAllocs()
			b.ResetTimer()
			bktw.getRangeCalls.Store(0)
			for i := 0; i < b.N; i++ {
				start := time.Now()
				series, err := m.Materialize(ctx, 0, data.MinTime, data.MaxTime, tc.skipChunks, tc.rr)
				if err != nil {
					b.Fatal("error materializing: ", err)
				}
				firstChunk := true
				for series.Next() {
					s := series.At()
					_ = s.Labels()
					it := s.Iterator(nil)
					for it.Next() {
						chk := it.At()
						if firstChunk {
							firstChunk = false
							b.ReportMetric(float64(time.Since(start).Nanoseconds()/int64(b.N)), "ns_to_first_chunk/op")
						}
						_ = chk.Chunk.NumSamples()
					}
					if it.Err() != nil {
						b.Fatal("error iterating chunks: ", it.Err())
					}
				}
				if err := series.Err(); err != nil {
					b.Fatal("error iterating series: ", err)
				}
				series.Close()
			}
			b.ReportMetric(float64(bktw.getRangeCalls.Load())/float64(b.N), "range_calls/op")
		})
	}
}

type bucketWrapper struct {
	objstore.Bucket
	readLatency   time.Duration
	getRangeCalls atomic.Int64
}

func (b *bucketWrapper) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if b.readLatency > 0 {
		time.Sleep(b.readLatency)
	}
	return b.Bucket.Get(ctx, name)
}

func (b *bucketWrapper) GetRange(ctx context.Context, name string, offset, length int64) (io.ReadCloser, error) {
	b.getRangeCalls.Add(1)
	if b.readLatency > 0 {
		time.Sleep(b.readLatency)
	}
	return b.Bucket.GetRange(ctx, name, offset, length)
}

func (b *bucketWrapper) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	if b.readLatency > 0 {
		time.Sleep(b.readLatency)
	}
	return b.Bucket.Attributes(ctx, name)
}
