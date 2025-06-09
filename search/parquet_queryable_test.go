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
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	prom_storage "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/prometheus/prometheus/util/testutil"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/storage"
)

func TestPromQLAcceptance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping, because 'short' flag was set")
	}

	opts := promql.EngineOpts{
		Timeout:                  1 * time.Hour,
		MaxSamples:               1e10,
		EnableNegativeOffset:     true,
		EnableAtModifier:         true,
		NoStepSubqueryIntervalFn: func(_ int64) int64 { return 30 * time.Second.Milliseconds() },
		LookbackDelta:            5 * time.Minute,
		EnableDelayedNameRemoval: true,
	}

	engine := promql.NewEngine(opts)
	t.Cleanup(func() { _ = engine.Close() })

	promqltest.RunBuiltinTestsWithStorage(&parallelTest{T: t}, engine, func(tt testutil.T) prom_storage.Storage {
		return &acceptanceTestStorage{t: t, st: teststorage.New(tt)}
	})
}

type parallelTest struct {
	*testing.T
}

func (s *parallelTest) Run(name string, t func(*testing.T)) bool {
	return s.T.Run(name+"-concurrent", func(tt *testing.T) {
		tt.Parallel()
		s.T.Run(name, t)
	})
}

type acceptanceTestStorage struct {
	t  *testing.T
	st *teststorage.TestStorage
}

func (st *acceptanceTestStorage) Appender(ctx context.Context) prom_storage.Appender {
	return st.st.Appender(ctx)
}

func (st *acceptanceTestStorage) ChunkQuerier(int64, int64) (prom_storage.ChunkQuerier, error) {
	return nil, errors.New("unimplemented")
}

func (st *acceptanceTestStorage) Querier(from, to int64) (prom_storage.Querier, error) {
	if st.st.Head().NumSeries() == 0 {
		// parquet-go panics when writing an empty parquet file
		return st.st.Querier(from, to)
	}
	bkt, err := filesystem.NewBucket(st.t.TempDir())
	if err != nil {
		st.t.Fatalf("unable to create bucket: %s", err)
	}
	st.t.Cleanup(func() { _ = bkt.Close() })

	h := st.st.Head()
	data := testData{minTime: h.MinTime(), maxTime: h.MaxTime()}
	block := convertToParquet(st.t, context.Background(), bkt, data, h)

	q, err := createQueryable(block)
	if err != nil {
		st.t.Fatalf("unable to create queryable: %s", err)
	}
	return q.Querier(from, to)
}

type countingBucket struct {
	objstore.Bucket

	nGet       atomic.Int32
	nGetRange  atomic.Int32
	bsGetRange atomic.Int64
}

func (b *countingBucket) ResetCounters() {
	b.nGet.Store(0)
	b.nGetRange.Store(0)
	b.bsGetRange.Store(0)
}

func (b *countingBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	b.nGet.Add(1)
	return b.Bucket.Get(ctx, name)
}

func (b *countingBucket) GetRange(ctx context.Context, name string, off int64, length int64) (io.ReadCloser, error) {
	b.nGetRange.Add(1)
	b.bsGetRange.Add(length)
	return b.Bucket.GetRange(ctx, name, off, length)
}

func (st *acceptanceTestStorage) Close() error {
	return st.st.Close()
}

func (st *acceptanceTestStorage) StartTime() (int64, error) {
	return st.st.StartTime()
}

func TestQueryable(t *testing.T) {
	st := teststorage.New(t)
	ctx := context.Background()
	t.Cleanup(func() { _ = st.Close() })

	bkt, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = bkt.Close() })

	cfg := defaultTestConfig()
	data := generateTestData(t, st, ctx, cfg)

	ir, err := st.Head().Index()
	require.NoError(t, err)

	testCases := map[string]struct {
		ops []storage.ShardOption
	}{
		"default": {
			ops: []storage.ShardOption{},
		},
		"skipBloomFilters": {
			ops: []storage.ShardOption{
				storage.WithFileOptions(
					parquet.SkipBloomFilters(true),
				),
			},
		},
	}

	for n, tc := range testCases {
		t.Run(n, func(t *testing.T) {
			// Convert to Parquet
			shard := convertToParquet(t, ctx, bkt, data, st.Head(), tc.ops...)

			t.Run("QueryByUniqueLabel", func(t *testing.T) {
				matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "unique", "unique_0")}
				sFound := queryWithQueryable(t, data.minTime, data.maxTime, shard, nil, matchers...)
				totalFound := 0
				for _, series := range sFound {
					require.Equal(t, series.Labels().Get("unique"), "unique_0")
					require.Contains(t, data.seriesHash, series.Labels().Hash())
					totalFound++
				}
				require.Equal(t, cfg.totalMetricNames, totalFound)
			})

			t.Run("QueryByMetricName", func(t *testing.T) {
				for i := 0; i < 50; i++ {
					name := fmt.Sprintf("metric_%d", rand.Int()%cfg.totalMetricNames)
					matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, name)}
					sFound := queryWithQueryable(t, data.minTime, data.maxTime, shard, nil, matchers...)
					totalFound := 0
					for _, series := range sFound {
						totalFound++
						require.Equal(t, series.Labels().Get(labels.MetricName), name)
						require.Contains(t, data.seriesHash, series.Labels().Hash())
					}
					require.Equal(t, cfg.metricsPerMetricName, totalFound)
				}
			})

			t.Run("QueryByUniqueLabel and SkipChunks=true", func(t *testing.T) {
				matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "unique", "unique_0")}
				hints := &prom_storage.SelectHints{
					Func: "series",
				}
				sFound := queryWithQueryable(t, data.minTime, data.maxTime, shard, hints, matchers...)
				totalFound := 0
				for _, series := range sFound {
					totalFound++
					require.Equal(t, series.Labels().Get("unique"), "unique_0")
					require.Contains(t, data.seriesHash, series.Labels().Hash())
				}
				require.Equal(t, cfg.totalMetricNames, totalFound)
			})

			t.Run("LabelNames", func(t *testing.T) {
				queryable, err := createQueryable(shard)
				require.NoError(t, err)
				querier, err := queryable.Querier(data.minTime, data.maxTime)
				require.NoError(t, err)

				t.Run("Without Matchers", func(t *testing.T) {
					lNames, _, err := querier.LabelNames(context.Background(), nil)
					require.NoError(t, err)
					require.NotEmpty(t, lNames)
					expectedLabelNames, err := ir.LabelNames(context.Background())
					require.NoError(t, err)
					require.Equal(t, expectedLabelNames, lNames)
				})

				t.Run("With Matchers", func(t *testing.T) {
					lNames, _, err := querier.LabelNames(context.Background(), nil, labels.MustNewMatcher(labels.MatchEqual, "random_name_0", "random_value_0"))
					require.NoError(t, err)
					require.NotEmpty(t, lNames)
					expectedLabelNames, err := ir.LabelNames(context.Background(), labels.MustNewMatcher(labels.MatchEqual, "random_name_0", "random_value_0"))
					require.NoError(t, err)
					require.Equal(t, expectedLabelNames, lNames)
				})
			})

			t.Run("LabelValues", func(t *testing.T) {
				queryable, err := createQueryable(shard)
				require.NoError(t, err)
				querier, err := queryable.Querier(data.minTime, data.maxTime)
				require.NoError(t, err)
				t.Run("Without Matchers", func(t *testing.T) {
					lValues, _, err := querier.LabelValues(context.Background(), labels.MetricName, nil)
					require.NoError(t, err)
					expectedLabelValues, err := ir.SortedLabelValues(context.Background(), labels.MetricName)
					require.NoError(t, err)
					require.Equal(t, expectedLabelValues, lValues)
				})

				t.Run("With Matchers", func(t *testing.T) {
					lValues, _, err := querier.LabelValues(context.Background(), labels.MetricName, nil, labels.MustNewMatcher(labels.MatchEqual, "random_name_0", "random_value_0"))
					require.NoError(t, err)
					expectedLabelValues, err := ir.SortedLabelValues(context.Background(), labels.MetricName, labels.MustNewMatcher(labels.MatchEqual, "random_name_0", "random_value_0"))
					require.NoError(t, err)
					require.Equal(t, expectedLabelValues, lValues)
				})
			})
		})
	}
}

func TestQueryableWithEmptyMatcher(t *testing.T) {
	opts := promql.EngineOpts{
		Timeout:                  1 * time.Hour,
		MaxSamples:               1e10,
		EnableNegativeOffset:     true,
		EnableAtModifier:         true,
		NoStepSubqueryIntervalFn: func(_ int64) int64 { return 30 * time.Second.Milliseconds() },
		LookbackDelta:            5 * time.Minute,
		EnableDelayedNameRemoval: true,
	}

	engine := promql.NewEngine(opts)
	t.Cleanup(func() { _ = engine.Close() })

	load := `load 30s
			    http_requests_total{pod="nginx-1", route="/"} 0+1x5
			    http_requests_total{pod="nginx-2"} 0+2x5
			    http_requests_total{pod="nginx-3", route="/"} 0+3x5
			    http_requests_total{pod="nginx-4"} 0+4x5

eval instant at 60s http_requests_total{route=""}
	{__name__="http_requests_total", pod="nginx-2"} 4
	{__name__="http_requests_total", pod="nginx-4"} 8

eval instant at 60s http_requests_total{route=~""}
	{__name__="http_requests_total", pod="nginx-2"} 4
	{__name__="http_requests_total", pod="nginx-4"} 8

eval instant at 60s http_requests_total{route!~".+"}
	{__name__="http_requests_total", pod="nginx-2"} 4
	{__name__="http_requests_total", pod="nginx-4"} 8

eval instant at 60s http_requests_total{route!=""}
	{__name__="http_requests_total", pod="nginx-1", route="/"} 2
	{__name__="http_requests_total", pod="nginx-3", route="/"} 6

eval instant at 60s http_requests_total{route!~""}
	{__name__="http_requests_total", pod="nginx-1", route="/"} 2
	{__name__="http_requests_total", pod="nginx-3", route="/"} 6

eval instant at 60s http_requests_total{route=~".+"}
	{__name__="http_requests_total", pod="nginx-1", route="/"} 2
	{__name__="http_requests_total", pod="nginx-3", route="/"} 6
`

	promqltest.RunTestWithStorage(t, load, engine, func(tt testutil.T) prom_storage.Storage {
		return &acceptanceTestStorage{t: t, st: teststorage.New(tt)}
	})
}

func queryWithQueryable(t *testing.T, mint, maxt int64, shard *storage.ParquetShard, hints *prom_storage.SelectHints, matchers ...*labels.Matcher) []prom_storage.Series {
	ctx := context.Background()
	queryable, err := createQueryable(shard)
	require.NoError(t, err)
	querier, err := queryable.Querier(mint, maxt)
	require.NoError(t, err)
	ss := querier.Select(ctx, true, hints, matchers...)

	found := make([]prom_storage.Series, 0, 100)
	for ss.Next() {
		found = append(found, ss.At())
	}
	if ss.Err() != nil {
		t.Fatalf("error during query: %v", ss.Err())
	}
	return found
}

func createQueryable(shard *storage.ParquetShard) (prom_storage.Queryable, error) {
	d := schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool())
	return NewParquetQueryable(d, func(ctx context.Context, mint, maxt int64) ([]*storage.ParquetShard, error) {
		return []*storage.ParquetShard{shard}, nil
	})
}

var benchmarkCases = []struct {
	name     string
	matchers []*labels.Matcher
}{
	{
		name: "SingleMetricAllSeries",
		matchers: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric_1"),
		},
	},
	{
		name: "SingleMetricReducedSeries",
		matchers: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric_1"),
			labels.MustNewMatcher(labels.MatchEqual, "instance", "instance-1"),
		},
	},
	{
		name: "SingleMetricOneSeries",
		matchers: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric_1"),
			labels.MustNewMatcher(labels.MatchEqual, "instance", "instance-2"),
			labels.MustNewMatcher(labels.MatchEqual, "region", "region-1"),
			labels.MustNewMatcher(labels.MatchEqual, "zone", "zone-3"),
			labels.MustNewMatcher(labels.MatchEqual, "service", "service-10"),
			labels.MustNewMatcher(labels.MatchEqual, "environment", "environment-1"),
		},
	},
	{
		name: "SingleMetricSparseSeries",
		matchers: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric_1"),
			labels.MustNewMatcher(labels.MatchEqual, "service", "service-1"),
			labels.MustNewMatcher(labels.MatchEqual, "environment", "environment-0"),
		},
	},
	{
		name: "NonExistentSeries",
		matchers: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric_1"),
			labels.MustNewMatcher(labels.MatchEqual, "environment", "non-existent-environment"),
		},
	},
	{
		name: "MultipleMetricsRange",
		matchers: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchRegexp, "__name__", "test_metric_[1-5]"),
		},
	},
	{
		name: "MultipleMetricsSparse",
		matchers: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchRegexp, "__name__", "test_metric_(1|5|10|15|20)"),
		},
	},
	{
		name: "NegativeRegexSingleMetric",
		matchers: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric_1"),
			labels.MustNewMatcher(labels.MatchNotRegexp, "instance", "(instance-1.*|instance-2.*)"),
		},
	},
	{
		name: "NegativeRegexMultipleMetrics",
		matchers: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchRegexp, "__name__", "test_metric_[1-3]"),
			labels.MustNewMatcher(labels.MatchNotRegexp, "instance", "(instance-1.*|instance-2.*)"),
		},
	},
	{
		name: "ExpensiveRegexSingleMetric",
		matchers: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric_1"),
			labels.MustNewMatcher(labels.MatchRegexp, "instance", "(container-1|instance-2|container-3|instance-4|container-5)"),
		},
	},
	{
		name: "ExpensiveRegexMultipleMetrics",
		matchers: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchRegexp, "__name__", "test_metric_[1-3]"),
			labels.MustNewMatcher(labels.MatchRegexp, "instance", "(container-1|container-2|container-3|container-4|container-5)"),
		},
	},
}

func BenchmarkSelect(b *testing.B) {
	ctx := context.Background()

	st := teststorage.New(b)
	b.Cleanup(func() { _ = st.Close() })
	bkt, err := filesystem.NewBucket(b.TempDir())
	if err != nil {
		b.Fatal("error creating bucket: ", err)
	}
	b.Cleanup(func() { _ = bkt.Close() })

	app := st.Appender(ctx)

	// 5 metrics × 100 instances × 5 regions × 10 zones × 20 services × 3 environments = 1,500,000 series
	metrics := 5
	instances := 100
	regions := 5
	zones := 10
	services := 20
	environments := 3

	totalSeries := metrics * instances * regions * zones * services * environments
	b.Logf("Generating %d series (%d metrics × %d instances × %d regions × %d zones × %d services × %d environments)",
		totalSeries, metrics, instances, regions, zones, services, environments)

	seriesCount := 0
	for m := range metrics {
		for i := range instances {
			for r := range regions {
				for z := range zones {
					for s := range services {
						for e := range environments {
							lbls := labels.FromStrings(
								"__name__", fmt.Sprintf("test_metric_%d", m),
								"instance", fmt.Sprintf("instance-%d", i),
								"region", fmt.Sprintf("region-%d", r),
								"zone", fmt.Sprintf("zone-%d", z),
								"service", fmt.Sprintf("service-%d", s),
								"environment", fmt.Sprintf("environment-%d", e),
							)
							_, _ = app.Append(0, lbls, 0, rand.Float64())
							seriesCount++
						}
					}
				}
			}
		}
	}
	if err := app.Commit(); err != nil {
		b.Fatal("error committing samples: ", err)
	}

	h := st.Head()
	require.Equal(b, totalSeries, int(h.NumSeries()), "Expected number of series does not match")

	cbkt := &countingBucket{Bucket: bkt}
	data := testData{minTime: h.MinTime(), maxTime: h.MaxTime()}
	block := convertToParquetForBench(b, ctx, cbkt, data, h)
	queryable, err := createQueryable(block)
	require.NoError(b, err, "unable to create queryable")

	q, err := queryable.Querier(0, 120)
	require.NoError(b, err, "unable to create querier")

	for _, bc := range benchmarkCases {
		b.Run(bc.name, func(b *testing.B) {
			cbkt.ResetCounters()
			b.ReportAllocs()
			b.ResetTimer()

			var series int
			for i := 0; i < b.N; i++ {
				ss := q.Select(ctx, true, &prom_storage.SelectHints{}, bc.matchers...)
				for ss.Next() {
					series++
					s := ss.At()
					it := s.Iterator(nil)
					for it.Next() != chunkenc.ValNone {
					}
				}
				if err := ss.Err(); err != nil {
					b.Error(err)
				}
			}

			b.ReportMetric(float64(series)/float64(b.N), "series/op")
			b.ReportMetric(float64(cbkt.nGet.Load())/float64(b.N), "get/op")
			b.ReportMetric(float64(cbkt.nGetRange.Load())/float64(b.N), "get_range/op")
			b.ReportMetric(float64(cbkt.bsGetRange.Load())/float64(b.N), "bytes_get_range/op")
		})
	}
}

func convertToParquetForBench(tb testing.TB, ctx context.Context, bkt objstore.Bucket, data testData, h convert.Convertible, opts ...storage.ShardOption) *storage.ParquetShard {
	colDuration := time.Hour
	shards, err := convert.ConvertTSDBBlock(
		ctx,
		bkt,
		data.minTime,
		data.maxTime,
		[]convert.Convertible{h},
		convert.WithName("shard"),
		convert.WithColDuration(colDuration),
		convert.WithRowGroupSize(500),
		convert.WithPageBufferSize(300),
	)
	if err != nil {
		tb.Fatalf("error converting to parquet: %v", err)
	}
	if shards != 1 {
		tb.Fatalf("expected 1 shard, got %d", shards)
	}

	shard, err := storage.OpenParquetShard(ctx, bkt, "shard", 0, opts...)
	if err != nil {
		tb.Fatalf("error opening parquet shard: %v", err)
	}

	return shard
}
