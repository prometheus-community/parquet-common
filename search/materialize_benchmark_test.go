package search

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/util"
)

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
	cfg.NumberOfSamples = 20_000 // non-trivial chunks
	data := util.GenerateTestData(b, st, ctx, cfg)
	bktw := &bucketWrapper{
		Bucket: bkt,
		// Simulate object store latency. The Materializer
		// can parallelize GetRange calls to help with this.
		readLatency: time.Millisecond * 5,
	}

	shard := convertToParquet(b, ctx, bktw, data, st.Head())

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
			rr:          []RowRange{{From: 0, Count: totalRows}},
		},
		{
			name:        "AllRowsConcur4",
			concurrency: 4,
			rr:          []RowRange{{From: 0, Count: totalRows}},
		},
		{
			name:        "Interleaved",
			concurrency: 1,
			rr: []RowRange{
				{From: 0, Count: totalRows / 10},
				{From: totalRows / 5, Count: totalRows / 10},
				{From: totalRows * 2 / 5, Count: totalRows / 10},
				{From: totalRows * 3 / 5, Count: totalRows / 10},
				{From: totalRows * 4 / 5, Count: totalRows / 10},
			},
		},
		{
			name:        "Sparse",
			concurrency: 1,
			rr: []RowRange{
				{From: 0, Count: 50},
				{From: totalRows / 4, Count: 50},
				{From: totalRows / 2, Count: 50},
				{From: totalRows * 3 / 4, Count: 50},
			},
		},
		{
			name:        "SingleRow",
			concurrency: 1,
			rr:          []RowRange{{From: totalRows / 2, Count: 1}},
		},
		{
			name:        "SkipChunks",
			concurrency: 1,
			rr:          []RowRange{{From: 0, Count: totalRows}},
			skipChunks:  true,
		},
	}

	for _, tc := range testCases {
		d := schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool())
		m, err := NewMaterializer(s, d, shard, 10, UnlimitedQuota(), UnlimitedQuota(), UnlimitedQuota(), NoopMaterializedSeriesFunc, NoopMaterializedLabelsFilterCallback)
		if err != nil {
			b.Fatal("error creating materializer: ", err)
		}
		for _, materializeStrategy := range []string{"Materialize", "MaterializeIter"} {

			b.Run(fmt.Sprintf("%s/func=%s", tc.name, materializeStrategy), func(b *testing.B) {
				// Warm up
				_, err = m.Materialize(ctx, nil, 0, data.MinTime, data.MaxTime, false, tc.rr)
				require.NoError(b, err)
				//s.Close()

				seriesIter, err := m.MaterializeIter(ctx, nil, 0, data.MinTime, data.MaxTime, false, tc.rr)
				require.NoError(b, err)
				seriesIter.Close()

				var materializeFunc func()
				if materializeStrategy == "MaterializeIter" {
					materializeFunc = func() {
						var m1, m2 runtime.MemStats
						runtime.GC()
						runtime.ReadMemStats(&m1)
						start := time.Now()

						seriesIter, err := m.MaterializeIter(ctx, nil, 0, data.MinTime, data.MaxTime, false, tc.rr)
						if err != nil {
							b.Fatal("error materializing: ", err)
						}

						firstChunk := true
						for seriesIter.Next() {
							s := seriesIter.At()
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
						if err := seriesIter.Err(); err != nil {
							b.Fatal("error iterating series: ", err)
						}

						runtime.ReadMemStats(&m2)
						heapAllocDiff := m2.HeapAlloc - m1.HeapAlloc
						heapInUseDiff := m2.HeapInuse - m1.HeapInuse
						b.ReportMetric(float64(heapAllocDiff/uint64(b.N)), "B-alloc-diff")
						b.ReportMetric(float64(heapInUseDiff/uint64(b.N)), "B-inuse-diff")
						seriesIter.Close()
					}
				} else if materializeStrategy == "Materialize" {
					materializeFunc = func() {
						var m1, m2 runtime.MemStats
						runtime.GC()
						runtime.ReadMemStats(&m1)
						start := time.Now()

						seriesSet, err := m.Materialize(ctx, nil, 0, data.MinTime, data.MaxTime, false, tc.rr)
						if err != nil {
							b.Fatal("error materializing: ", err)
						}

						firstChunk := true
						for _, s := range seriesSet {
							//s := seriesSet.At()
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
						runtime.ReadMemStats(&m2)
						heapAllocDiff := m2.HeapAlloc - m1.HeapAlloc
						heapInUseDiff := m2.HeapInuse - m1.HeapInuse
						b.ReportMetric(float64(heapAllocDiff/uint64(b.N)), "B-alloc-diff")
						b.ReportMetric(float64(heapInUseDiff/uint64(b.N)), "B-inuse-diff")
					}
				}

				b.ReportAllocs()
				//b.ResetTimer()
				bktw.getRangeCalls.Store(0)
				for i := 0; i < b.N; i++ {
					materializeFunc()
				}
				b.ReportMetric(float64(bktw.getRangeCalls.Load())/float64(b.N), "range_calls/op")
			})
		}
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
