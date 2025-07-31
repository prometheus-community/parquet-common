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

package queryable

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync"
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

	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/search"
	"github.com/prometheus-community/parquet-common/storage"
	"github.com/prometheus-community/parquet-common/util"
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
	bkt, err := newBucket(st.t.TempDir())
	if err != nil {
		st.t.Fatalf("unable to create bucket: %s", err)
	}
	st.t.Cleanup(func() { _ = bkt.Close() })

	h := st.st.Head()
	data := util.TestData{MinTime: h.MinTime(), MaxTime: h.MaxTime()}
	block := convertToParquet(st.t, context.Background(), bkt, data, h, nil)

	q, err := createQueryable(block)
	if err != nil {
		st.t.Fatalf("unable to create queryable: %s", err)
	}
	return q.Querier(from, to)
}

type countingBucket struct {
	*bucket

	nGet       atomic.Int32
	nGetRange  atomic.Int32
	bsGetRange atomic.Int64
}

func newCountingBucket(bkt *bucket) *countingBucket {
	return &countingBucket{bucket: bkt}
}

func (b *countingBucket) ResetCounters() {
	b.nGet.Store(0)
	b.nGetRange.Store(0)
	b.bsGetRange.Store(0)
}

func (b *countingBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	b.nGet.Add(1)
	return b.bucket.Get(ctx, name)
}

func (b *countingBucket) GetRange(ctx context.Context, name string, off int64, length int64) (io.ReadCloser, error) {
	b.nGetRange.Add(1)
	b.bsGetRange.Add(length)
	return b.bucket.GetRange(ctx, name, off, length)
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

	bkt, err := newBucket(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = bkt.Close() })

	cfg := util.DefaultTestConfig()
	data := util.GenerateTestData(t, st, ctx, cfg)

	ir, err := st.Head().Index()
	require.NoError(t, err)

	testCases := map[string]struct {
		storageOpts []storage.FileOption
		convertOpts []convert.ConvertOption
	}{
		"default": {
			storageOpts: []storage.FileOption{},
			convertOpts: defaultConvertOpts,
		},
		"skipBloomFilters": {
			storageOpts: []storage.FileOption{
				storage.WithFileOptions(
					parquet.SkipBloomFilters(true),
					parquet.OptimisticRead(true),
				),
			},
			convertOpts: defaultConvertOpts,
		},
		"multipleSortingColumns": {
			storageOpts: []storage.FileOption{},
			convertOpts: []convert.ConvertOption{
				convert.WithName("shard"),
				convert.WithColDuration(time.Hour),
				convert.WithRowGroupSize(500),
				convert.WithPageBufferSize(300),
				convert.WithSortBy(fmt.Sprintf("%s,%s", labels.MetricName, "label_name_1")),
			},
		},
	}

	for n, tc := range testCases {
		t.Run(n, func(t *testing.T) {
			shard := convertToParquet(t, ctx, bkt, data, st.Head(), tc.convertOpts, tc.storageOpts...)

			t.Run("QueryByUniqueLabel", func(t *testing.T) {
				matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "unique", "unique_0")}
				sFound := queryWithQueryable(t, data.MinTime, data.MaxTime, shard, nil, matchers...)
				totalFound := 0
				for _, series := range sFound {
					require.Equal(t, series.Labels().Get("unique"), "unique_0")
					require.Contains(t, data.SeriesHash, series.Labels().Hash())
					totalFound++
				}
				require.Equal(t, cfg.TotalMetricNames, totalFound)
			})

			t.Run("QueryByMetricName", func(t *testing.T) {
				for i := 0; i < 50; i++ {
					name := fmt.Sprintf("metric_%d", rand.Int()%cfg.TotalMetricNames)
					matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, name)}
					sFound := queryWithQueryable(t, data.MinTime, data.MaxTime, shard, nil, matchers...)
					totalFound := 0
					for _, series := range sFound {
						totalFound++
						require.Equal(t, series.Labels().Get(labels.MetricName), name)
						require.Contains(t, data.SeriesHash, series.Labels().Hash())
					}
					require.Equal(t, cfg.MetricsPerMetricName, totalFound)
				}
			})

			t.Run("QueryByMultipleLabels", func(t *testing.T) {
				for i := 0; i < 50; i++ {
					name := fmt.Sprintf("metric_%d", rand.Int()%cfg.TotalMetricNames)
					matchers := []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, name),
						labels.MustNewMatcher(labels.MatchEqual, "label_name_1", "label_value_1"),
					}
					sFound := queryWithQueryable(t, data.MinTime, data.MaxTime, shard, nil, matchers...)
					totalFound := 0
					for _, series := range sFound {
						totalFound++
						require.Equal(t, series.Labels().Get(labels.MetricName), name)
						require.Equal(t, series.Labels().Get("label_name_1"), "label_value_1")
						require.Contains(t, data.SeriesHash, series.Labels().Hash())
					}
					require.Equal(t, cfg.MetricsPerMetricName, totalFound)
				}
			})

			t.Run("QueryByUniqueLabel and SkipChunks=true", func(t *testing.T) {
				matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "unique", "unique_0")}
				hints := &prom_storage.SelectHints{
					Func: "series",
				}
				sFound := queryWithQueryable(t, data.MinTime, data.MaxTime, shard, hints, matchers...)
				totalFound := 0
				for _, series := range sFound {
					totalFound++
					require.Equal(t, series.Labels().Get("unique"), "unique_0")
					require.Contains(t, data.SeriesHash, series.Labels().Hash())
				}
				require.Equal(t, cfg.TotalMetricNames, totalFound)
			})

			t.Run("LabelNames", func(t *testing.T) {
				queryable, err := createQueryable(shard)
				require.NoError(t, err)
				querier, err := queryable.Querier(data.MinTime, data.MaxTime)
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
				querier, err := queryable.Querier(data.MinTime, data.MaxTime)
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

			t.Run("RowCountQuota", func(t *testing.T) {
				// Test with limited row count quota
				limitedRowQuota := func(ctx context.Context) int64 {
					return 10 // Only allow 10 rows
				}
				queryable, err := createQueryable(shard, WithRowCountLimitFunc(limitedRowQuota))
				require.NoError(t, err)
				querier, err := queryable.Querier(data.MinTime, data.MaxTime)
				require.NoError(t, err)

				// Try to query more rows than quota allows
				matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "unique", "unique_0")}
				ss := querier.Select(ctx, true, nil, matchers...)

				// This should fail due to row count quota
				for ss.Next() {
					_ = ss.At()
				}
				require.Error(t, ss.Err())
				require.Contains(t, ss.Err().Error(), "would fetch too many rows")
				require.True(t, search.IsResourceExhausted(ss.Err()))

				// Test with sufficient quota
				sufficientRowQuota := func(ctx context.Context) int64 {
					return 1000 // Allow 1000 rows
				}
				queryable, err = createQueryable(shard, WithRowCountLimitFunc(sufficientRowQuota))
				require.NoError(t, err)
				querier, err = queryable.Querier(data.MinTime, data.MaxTime)
				require.NoError(t, err)

				ss = querier.Select(ctx, true, nil, matchers...)
				var series []prom_storage.Series
				for ss.Next() {
					series = append(series, ss.At())
				}
				require.NoError(t, ss.Err())
				require.NotEmpty(t, series)
			})

			t.Run("ChunkBytesQuota", func(t *testing.T) {
				// Test with limited chunk bytes quota
				limitedChunkQuota := func(ctx context.Context) int64 {
					return 100 // Only allow 100 bytes
				}
				queryable, err := createQueryable(shard, WithChunkBytesLimitFunc(limitedChunkQuota))
				require.NoError(t, err)
				querier, err := queryable.Querier(data.MinTime, data.MaxTime)
				require.NoError(t, err)

				// Try to query chunks that exceed the quota
				matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "unique", "unique_0")}
				ss := querier.Select(ctx, true, nil, matchers...)

				// This should fail due to chunk bytes quota
				for ss.Next() {
					_ = ss.At()
				}
				require.Error(t, ss.Err())
				require.Contains(t, ss.Err().Error(), "would fetch too many chunk bytes")
				require.True(t, search.IsResourceExhausted(ss.Err()))

				// Test with sufficient quota
				sufficientChunkQuota := func(ctx context.Context) int64 {
					return 1000000 // Allow 1MB
				}
				queryable, err = createQueryable(shard, WithChunkBytesLimitFunc(sufficientChunkQuota))
				require.NoError(t, err)
				querier, err = queryable.Querier(data.MinTime, data.MaxTime)
				require.NoError(t, err)

				ss = querier.Select(ctx, true, nil, matchers...)
				var series []prom_storage.Series
				for ss.Next() {
					series = append(series, ss.At())
				}
				require.NoError(t, ss.Err())
				require.NotEmpty(t, series)
			})

			t.Run("DataBytesQuota", func(t *testing.T) {
				// Test with limited data bytes quota
				limitedDataQuota := func(ctx context.Context) int64 {
					return 100 // Only allow 100 bytes
				}
				queryable, err := createQueryable(shard, WithDataBytesLimitFunc(limitedDataQuota))
				require.NoError(t, err)
				querier, err := queryable.Querier(data.MinTime, data.MaxTime)
				require.NoError(t, err)

				// Try to query data that exceeds the quota
				matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "unique", "unique_0")}
				ss := querier.Select(ctx, true, nil, matchers...)

				// This should fail due to data bytes quota
				for ss.Next() {
					_ = ss.At()
				}
				require.Error(t, ss.Err())
				require.Contains(t, ss.Err().Error(), "would fetch too many data bytes")
				require.True(t, search.IsResourceExhausted(ss.Err()))

				// Test with sufficient quota
				sufficientDataQuota := func(ctx context.Context) int64 {
					return 1000000 // Allow 1MB
				}
				queryable, err = createQueryable(shard, WithDataBytesLimitFunc(sufficientDataQuota))
				require.NoError(t, err)
				querier, err = queryable.Querier(data.MinTime, data.MaxTime)
				require.NoError(t, err)

				ss = querier.Select(ctx, true, nil, matchers...)
				var series []prom_storage.Series
				for ss.Next() {
					series = append(series, ss.At())
				}
				require.NoError(t, ss.Err())
				require.NotEmpty(t, series)
			})

			t.Run("MaterializedSeriesCallback", func(t *testing.T) {
				// Test callback that counts materialized series
				var seriesCount int
				mtx := sync.Mutex{}
				seriesCallback := func(ctx context.Context, series []prom_storage.ChunkSeries) error {
					mtx.Lock()
					seriesCount += len(series)
					mtx.Unlock()
					return nil
				}

				queryable, err := createQueryable(shard, WithMaterializedSeriesCallback(seriesCallback))
				require.NoError(t, err)
				querier, err := queryable.Querier(data.MinTime, data.MaxTime)
				require.NoError(t, err)

				// Query some series to trigger the callback
				matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "unique", "unique_0")}
				ss := querier.Select(ctx, true, nil, matchers...)

				var foundSeries []prom_storage.Series
				for ss.Next() {
					foundSeries = append(foundSeries, ss.At())
				}
				require.NoError(t, ss.Err())
				require.NotEmpty(t, foundSeries)
				require.Equal(t, len(foundSeries), seriesCount, "Callback should receive the same number of series")

				// Test callback that returns an error
				errorCallback := func(ctx context.Context, series []prom_storage.ChunkSeries) error {
					return fmt.Errorf("callback error")
				}

				queryable, err = createQueryable(shard, WithMaterializedSeriesCallback(errorCallback))
				require.NoError(t, err)
				querier, err = queryable.Querier(data.MinTime, data.MaxTime)
				require.NoError(t, err)

				ss = querier.Select(ctx, true, nil, matchers...)
				for ss.Next() {
					_ = ss.At()
				}
				require.Error(t, ss.Err())
				require.Contains(t, ss.Err().Error(), "callback error")
			})

			t.Run("MaterializedLabelsFilterCallback", func(t *testing.T) {
				// Query series that should be filtered by the callback
				matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "unique", "unique_0")}

				// Test callback that returns empty results by filtering out all series
				emptyFilter := func(ctx context.Context, hints *prom_storage.SelectHints) (search.MaterializedLabelsFilter, bool) {
					return &allRejectingFilter{}, true
				}

				queryable, err := createQueryable(shard, WithMaterializedLabelsFilterCallback(emptyFilter))
				require.NoError(t, err)
				querier, err := queryable.Querier(data.MinTime, data.MaxTime)
				require.NoError(t, err)

				ss := querier.Select(ctx, true, nil, matchers...)
				var emptySeries []prom_storage.Series
				for ss.Next() {
					emptySeries = append(emptySeries, ss.At())
				}
				require.NoError(t, ss.Err())
				require.Empty(t, emptySeries, "Callback should filter out all series")

				// Test callback that filters out series with specific labels
				specificFilter := func(ctx context.Context, hints *prom_storage.SelectHints) (search.MaterializedLabelsFilter, bool) {
					return &randomName0RejectingFilter{}, true
				}

				queryable, err = createQueryable(shard, WithMaterializedLabelsFilterCallback(specificFilter))
				require.NoError(t, err)
				querier, err = queryable.Querier(data.MinTime, data.MaxTime)
				require.NoError(t, err)

				ss = querier.Select(ctx, true, nil, matchers...)
				var filteredSeries []prom_storage.Series
				for ss.Next() {
					filteredSeries = append(filteredSeries, ss.At())
				}
				require.NoError(t, ss.Err())

				// Verify that series with "random_name_0" label were filtered out
				for _, series := range filteredSeries {
					require.Empty(t, series.Labels().Get("random_name_0"), "Series with random_name_0 should be filtered out")
				}

				// Test callback that returns false (no filtering)
				noopFilter := func(ctx context.Context, hints *prom_storage.SelectHints) (search.MaterializedLabelsFilter, bool) {
					return nil, false
				}

				queryable, err = createQueryable(shard, WithMaterializedLabelsFilterCallback(noopFilter))
				require.NoError(t, err)
				querier, err = queryable.Querier(data.MinTime, data.MaxTime)
				require.NoError(t, err)

				ss = querier.Select(ctx, true, nil, matchers...)
				var allSeries []prom_storage.Series
				for ss.Next() {
					allSeries = append(allSeries, ss.At())
				}
				require.NoError(t, ss.Err())
				require.NotEmpty(t, allSeries, "No filtering should return all series")
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

func queryWithQueryable(t *testing.T, mint, maxt int64, shard storage.ParquetShard, hints *prom_storage.SelectHints, matchers ...*labels.Matcher) []prom_storage.Series {
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
	return found
}

func createQueryable(shard storage.ParquetShard, opts ...QueryableOpts) (prom_storage.Queryable, error) {
	d := schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool())
	return NewParquetQueryable(d, func(ctx context.Context, mint, maxt int64) ([]storage.ParquetShard, error) {
		return []storage.ParquetShard{shard}, nil
	}, opts...)
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
	bkt, err := newBucket(b.TempDir())
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

	cbkt := newCountingBucket(bkt)
	data := util.TestData{MinTime: h.MinTime(), MaxTime: h.MaxTime()}
	block := convertToParquetForBenchWithCountingBucket(b, ctx, bkt, cbkt, data, h, nil)
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

var defaultConvertOpts = []convert.ConvertOption{
	convert.WithName("shard"),
	convert.WithColDuration(time.Hour),
	convert.WithRowGroupSize(500),
	convert.WithPageBufferSize(300),
}

func convertToParquet(t *testing.T, ctx context.Context, bkt *bucket, data util.TestData, h convert.Convertible, convertOpts []convert.ConvertOption, opts ...storage.FileOption) storage.ParquetShard {
	if convertOpts == nil {
		convertOpts = defaultConvertOpts
	}

	shards, err := convert.ConvertTSDBBlock(
		ctx,
		bkt,
		data.MinTime,
		data.MaxTime,
		[]convert.Convertible{h},
		convertOpts...,
	)
	if err != nil {
		t.Fatalf("error converting to parquet: %v", err)
	}
	if shards != 1 {
		t.Fatalf("expected 1 shard, got %d", shards)
	}

	bucketOpener := storage.NewParquetBucketOpener(bkt)
	shard, err := storage.NewParquetShardOpener(
		ctx, "shard", bucketOpener, bucketOpener, 0, opts...,
	)
	if err != nil {
		t.Fatalf("error opening parquet shard: %v", err)
	}

	return shard
}

func convertToParquetForBenchWithCountingBucket(tb testing.TB, ctx context.Context, bkt *bucket, cbkt *countingBucket, data util.TestData, h convert.Convertible, convertOpts []convert.ConvertOption, opts ...storage.FileOption) storage.ParquetShard {
	if convertOpts == nil {
		convertOpts = defaultConvertOpts
	}

	shards, err := convert.ConvertTSDBBlock(
		ctx,
		bkt,
		data.MinTime,
		data.MaxTime,
		[]convert.Convertible{h},
		convertOpts...,
	)
	if err != nil {
		tb.Fatalf("error converting to parquet: %v", err)
	}
	if shards != 1 {
		tb.Fatalf("expected 1 shard, got %d", shards)
	}

	bucketOpener := storage.NewParquetBucketOpener(cbkt)
	shard, err := storage.NewParquetShardOpener(
		ctx, "shard", bucketOpener, bucketOpener, 0,
	)
	if err != nil {
		tb.Fatalf("error opening parquet shard: %v", err)
	}

	return shard
}

// allRejectingFilter is a MaterializedLabelsFilter that rejects all series
type allRejectingFilter struct{}

func (f *allRejectingFilter) Filter(ls labels.Labels) bool {
	return false // Reject all series
}

func (f *allRejectingFilter) Close() {
	// No cleanup needed
}

// randomName0RejectingFilter is a MaterializedLabelsFilter that rejects series with random_name_0 label
type randomName0RejectingFilter struct{}

func (f *randomName0RejectingFilter) Filter(ls labels.Labels) bool {
	// Reject series that doesn't have "random_name_0" label
	return ls.Get("random_name_0") == ""
}

func (f *randomName0RejectingFilter) Close() {
	// No cleanup needed
}
