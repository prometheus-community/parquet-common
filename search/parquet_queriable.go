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
	prom_storage "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/storage"
	"github.com/prometheus-community/parquet-common/util"
)

type parquetQueryable struct {
	blocks []*queryableBlock
}

func NewParquetQueryable(d *schema.PrometheusParquetChunksDecoder, blocks ...*storage.ParquetBlock) (prom_storage.Queryable, error) {
	qBlocks := make([]*queryableBlock, len(blocks))
	for i, b := range blocks {
		qb, err := newQueryableBlock(b, d)
		if err != nil {
			return nil, err
		}
		qBlocks[i] = qb
	}
	return &parquetQueryable{
		blocks: qBlocks,
	}, nil
}

func (p parquetQueryable) Querier(mint, maxt int64) (prom_storage.Querier, error) {
	return &parquetQuerier{
		mint:   mint,
		maxt:   maxt,
		blocks: p.blocks,
	}, nil
}

type parquetQuerier struct {
	mint, maxt int64

	blocks []*queryableBlock
}

func (p parquetQuerier) LabelValues(ctx context.Context, name string, hints *prom_storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
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

func (p parquetQuerier) LabelNames(ctx context.Context, hints *prom_storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
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

func (p parquetQuerier) Select(ctx context.Context, sorted bool, sp *prom_storage.SelectHints, matchers ...*labels.Matcher) prom_storage.SeriesSet {
	seriesSet := make([]prom_storage.ChunkSeriesSet, len(p.blocks))

	minT, maxT := p.mint, p.maxt
	if sp != nil {
		minT, maxT = sp.Start, sp.End
	}
	skipChunks := sp != nil && sp.Func == "series"

	for i, block := range p.blocks {
		ss, err := block.Query(ctx, sorted, minT, maxT, skipChunks, matchers)
		if err != nil {
			return prom_storage.ErrSeriesSet(err)
		}
		seriesSet[i] = ss
	}
	ss := convert.NewMergeChunkSeriesSet(seriesSet, labels.Compare, prom_storage.NewConcatenatingChunkSeriesMerger())

	return convert.NewSeriesSetFromChunkSeriesSet(ss, skipChunks)
}

type queryableBlock struct {
	block *storage.ParquetBlock
	m     *Materializer
}

func newQueryableBlock(block *storage.ParquetBlock, d *schema.PrometheusParquetChunksDecoder) (*queryableBlock, error) {
	s, err := block.TSDBSchema()
	if err != nil {
		return nil, err
	}
	m, err := NewMaterializer(s, d, block)
	if err != nil {
		return nil, err
	}

	return &queryableBlock{
		block: block,
		m:     m,
	}, nil
}

func (b queryableBlock) Query(ctx context.Context, sorted bool, mint, maxt int64, skipChunks bool, matchers []*labels.Matcher) (prom_storage.ChunkSeriesSet, error) {
	cs, err := MatchersToConstraint(matchers...)
	if err != nil {
		return nil, err
	}
	err = Initialize(b.block.LabelsFile(), cs...)
	if err != nil {
		return nil, err
	}

	results := make([]prom_storage.ChunkSeries, 0, 1024)
	for i, group := range b.block.LabelsFile().RowGroups() {
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

func (b queryableBlock) LabelNames(ctx context.Context, matchers []*labels.Matcher) ([][]string, error) {
	if len(matchers) == 0 {
		return [][]string{b.m.MaterializeAllLabelNames()}, nil
	}
	cs, err := MatchersToConstraint(matchers...)
	if err != nil {
		return nil, err
	}
	err = Initialize(b.block.LabelsFile(), cs...)
	if err != nil {
		return nil, err
	}

	results := make([][]string, len(b.block.LabelsFile().RowGroups()))
	for i, group := range b.block.LabelsFile().RowGroups() {
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

func (b queryableBlock) LabelValues(ctx context.Context, name string, matchers []*labels.Matcher) ([][]string, error) {
	if len(matchers) == 0 {
		return b.allLabelValues(ctx, name)
	}
	cs, err := MatchersToConstraint(matchers...)
	if err != nil {
		return nil, err
	}
	err = Initialize(b.block.LabelsFile(), cs...)
	if err != nil {
		return nil, err
	}

	results := make([][]string, len(b.block.LabelsFile().RowGroups()))
	for i, group := range b.block.LabelsFile().RowGroups() {
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

func (b queryableBlock) allLabelValues(ctx context.Context, name string) ([][]string, error) {
	results := make([][]string, len(b.block.LabelsFile().RowGroups()))
	for i := range b.block.LabelsFile().RowGroups() {
		series, err := b.m.MaterializeAllLabelValues(ctx, name, i)
		if err != nil {
			return nil, err
		}
		results[i] = series
	}

	return results, nil
}

type byLabels []prom_storage.ChunkSeries

func (b byLabels) Len() int           { return len(b) }
func (b byLabels) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byLabels) Less(i, j int) bool { return labels.Compare(b[i].Labels(), b[j].Labels()) < 0 }
