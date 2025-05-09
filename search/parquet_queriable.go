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
	"sort"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus-community/parquet-common/file"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/util"
)

type parquetQueryable struct {
	blocks []*ParquetBlock
}

func NewParquetQueryable(blocks ...*ParquetBlock) (storage.Queryable, error) {
	return &parquetQueryable{
		blocks: blocks,
	}, nil
}

func (p parquetQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	return &parquetQuerier{
		mint:   mint,
		maxt:   maxt,
		blocks: p.blocks,
	}, nil
}

type parquetQuerier struct {
	mint, maxt int64

	blocks []*ParquetBlock
}

func (p parquetQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	limit := int64(0)

	if hints != nil {
		limit = int64(hints.Limit)
	}

	resNameValues := [][]string{}

	for _, b := range p.blocks {
		r, err := b.LabelValues(ctx, name, matchers)
		if err != nil {
			return nil, nil, err
		}

		resNameValues = append(resNameValues, r...)
	}

	return util.MergeUnsortedSlices(int(limit), resNameValues...), nil, nil
}

func (p parquetQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	limit := int64(0)

	if hints != nil {
		limit = int64(hints.Limit)
	}

	resNameSets := [][]string{}

	for _, b := range p.blocks {
		r, err := b.LabelNames(ctx, matchers)
		if err != nil {
			return nil, nil, err
		}

		resNameSets = append(resNameSets, r...)
	}

	return util.MergeUnsortedSlices(int(limit), resNameSets...), nil, nil
}

func (p parquetQuerier) Close() error {
	return nil
}

func (p parquetQuerier) Select(ctx context.Context, sorted bool, sp *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	seriesSet := make([]storage.ChunkSeriesSet, len(p.blocks))

	minT, maxT := p.mint, p.maxt
	if sp != nil {
		minT, maxT = sp.Start, sp.End
	}
	skipChunks := sp != nil && sp.Func == "series"

	for i, block := range p.blocks {
		ss, err := block.Query(ctx, sorted, minT, maxT, skipChunks, matchers)
		if err != nil {
			return storage.ErrSeriesSet(err)
		}
		seriesSet[i] = ss
	}
	ss := convert.NewMergeChunkSeriesSet(seriesSet, labels.Compare, storage.NewConcatenatingChunkSeriesMerger())

	return convert.NewSeriesSetFromChunkSeriesSet(ss, skipChunks)
}

type ParquetBlock struct {
	lf, cf *file.ParquetFile
	m      *Materializer
}

func NewParquetBlock(lf, cf *file.ParquetFile, d *schema.PrometheusParquetChunksDecoder) (*ParquetBlock, error) {
	s, err := schema.FromLabelsFile(lf)
	if err != nil {
		return nil, err
	}
	m, err := NewMaterializer(s, d, lf, cf)
	if err != nil {
		return nil, err
	}

	return &ParquetBlock{
		lf: lf,
		cf: cf,
		m:  m,
	}, nil
}

func (b ParquetBlock) Query(ctx context.Context, sorted bool, mint, maxt int64, skipChunks bool, matchers []*labels.Matcher) (storage.ChunkSeriesSet, error) {
	cs, err := MatchersToConstraint(matchers...)
	if err != nil {
		return nil, err
	}
	err = Initialize(b.lf, cs...)
	if err != nil {
		return nil, err
	}

	results := make([]storage.ChunkSeries, 0, 1024)
	for i, group := range b.lf.RowGroups() {
		rr, err := Filter(ctx, group, cs...)
		if err != nil {
			return nil, err
		}
		series, err := b.m.Materialize(ctx, i, mint, maxt, skipChunks, rr)
		if err != nil {
			return nil, err
		}
		results = append(results, series...)
	}

	if sorted {
		sort.Sort(byLabels(results))
	}
	return convert.NewChunksSeriesSet(results), nil
}

func (b ParquetBlock) LabelNames(ctx context.Context, matchers []*labels.Matcher) ([][]string, error) {
	if len(matchers) == 0 {
		return [][]string{b.m.MaterializeAllLabelNames()}, nil
	}
	cs, err := MatchersToConstraint(matchers...)
	if err != nil {
		return nil, err
	}
	err = Initialize(b.lf, cs...)
	if err != nil {
		return nil, err
	}

	results := make([][]string, len(b.lf.RowGroups()))
	for i, group := range b.lf.RowGroups() {
		rr, err := Filter(ctx, group, cs...)
		if err != nil {
			return nil, err
		}
		series, err := b.m.MaterializeLabelNames(ctx, i, rr)
		if err != nil {
			return nil, err
		}
		results[i] = series
	}

	return results, nil
}

func (b ParquetBlock) LabelValues(ctx context.Context, name string, matchers []*labels.Matcher) ([][]string, error) {
	if len(matchers) == 0 {
		return b.allLabelValues(ctx, name)
	}
	cs, err := MatchersToConstraint(matchers...)
	if err != nil {
		return nil, err
	}
	err = Initialize(b.lf, cs...)
	if err != nil {
		return nil, err
	}

	results := make([][]string, len(b.lf.RowGroups()))
	for i, group := range b.lf.RowGroups() {
		rr, err := Filter(ctx, group, cs...)
		if err != nil {
			return nil, err
		}
		series, err := b.m.MaterializeLabelValues(ctx, name, i, rr)
		if err != nil {
			return nil, err
		}
		results[i] = series
	}

	return results, nil
}

func (b ParquetBlock) allLabelValues(ctx context.Context, name string) ([][]string, error) {
	results := make([][]string, len(b.lf.RowGroups()))
	for i := range b.lf.RowGroups() {
		series, err := b.m.MaterializeAllLabelValues(ctx, name, i)
		if err != nil {
			return nil, err
		}
		results[i] = series
	}

	return results, nil
}

type byLabels []storage.ChunkSeries

func (b byLabels) Len() int           { return len(b) }
func (b byLabels) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byLabels) Less(i, j int) bool { return labels.Compare(b[i].Labels(), b[j].Labels()) < 0 }
