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
	"iter"
	"maps"
	"slices"
	"sync"

	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	prom_storage "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"golang.org/x/sync/errgroup"

	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/storage"
	"github.com/prometheus-community/parquet-common/util"
)

type Materializer struct {
	b           storage.ParquetShard
	s           *schema.TSDBSchema
	d           *schema.PrometheusParquetChunksDecoder
	partitioner util.Partitioner

	colIdx      int
	concurrency int

	dataColToIndex []int

	rowCountQuota   *Quota
	chunkBytesQuota *Quota
	dataBytesQuota  *Quota

	materializedSeriesCallback MaterializedSeriesFunc
}

// MaterializedSeriesFunc is a callback function that can be used to add limiter or statistic logics for
// materialized series.
type MaterializedSeriesFunc func(ctx context.Context, series []prom_storage.ChunkSeries) error

// NoopMaterializedSeriesFunc is a noop callback function that does nothing.
func NoopMaterializedSeriesFunc(_ context.Context, _ []prom_storage.ChunkSeries) error {
	return nil
}

func NewMaterializer(s *schema.TSDBSchema,
	d *schema.PrometheusParquetChunksDecoder,
	block storage.ParquetShard,
	concurrency int,
	rowCountQuota *Quota,
	chunkBytesQuota *Quota,
	dataBytesQuota *Quota,
	materializeSeriesCallback MaterializedSeriesFunc,
) (*Materializer, error) {
	colIdx, ok := block.LabelsFile().Schema().Lookup(schema.ColIndexes)
	if !ok {
		return nil, errors.New(fmt.Sprintf("schema index %s not found", schema.ColIndexes))
	}

	dataColToIndex := make([]int, len(block.ChunksFile().Schema().Columns()))
	for i := 0; i < len(s.DataColsIndexes); i++ {
		c, ok := block.ChunksFile().Schema().Lookup(schema.DataColumn(i))
		if !ok {
			return nil, errors.New(fmt.Sprintf("schema column %s not found", schema.DataColumn(i)))
		}

		dataColToIndex[i] = c.ColumnIndex
	}

	return &Materializer{
		s:                          s,
		d:                          d,
		b:                          block,
		colIdx:                     colIdx.ColumnIndex,
		concurrency:                concurrency,
		partitioner:                util.NewGapBasedPartitioner(block.ChunksFile().Cfg.PagePartitioningMaxGapSize),
		dataColToIndex:             dataColToIndex,
		rowCountQuota:              rowCountQuota,
		chunkBytesQuota:            chunkBytesQuota,
		dataBytesQuota:             dataBytesQuota,
		materializedSeriesCallback: materializeSeriesCallback,
	}, nil
}

// Materialize reconstructs the ChunkSeries that belong to the specified row ranges (rr).
// It uses the row group index (rgi) and time bounds (mint, maxt) to filter and decode the series.
func (m *Materializer) Materialize(ctx context.Context, rgi int, mint, maxt int64, skipChunks bool, rr []RowRange) ([]prom_storage.ChunkSeries, error) {
	if err := m.checkRowCountQuota(rr); err != nil {
		return nil, err
	}
	sLbls, err := m.materializeAllLabels(ctx, rgi, rr)
	if err != nil {
		return nil, errors.Wrapf(err, "error materializing labels")
	}

	results := make([]prom_storage.ChunkSeries, len(sLbls))
	for i, s := range sLbls {
		results[i] = &concreteChunksSeries{
			lbls: labels.New(s...),
		}
	}

	if !skipChunks {
		chks, err := m.materializeChunksSlice(ctx, rgi, mint, maxt, rr)
		if err != nil {
			return nil, errors.Wrap(err, "materializer failed to materialize chunks")
		}

		for i, result := range results {
			result.(*concreteChunksSeries).chks = chks[i]
		}

		// If we are not skipping chunks and there is no chunks for the time range queried, lets remove the series
		results = slices.DeleteFunc(results, func(cs prom_storage.ChunkSeries) bool {
			return len(cs.(*concreteChunksSeries).chks) == 0
		})
	}
	return results, err
}

// MaterializeLazy provides an iterator to lazily reconstruct the ChunkSeries that belong to the specified row ranges (rr).
// It uses the row group index (rgi) and time bounds (mint, maxt) to filter and decode the series.
func (m *Materializer) MaterializeLazy(ctx context.Context, rgi int, mint, maxt int64, skipChunks bool, rr []RowRange) ([]prom_storage.ChunkSeries, error) {
	sLbls, err := m.materializeAllLabels(ctx, rgi, rr)
	if err != nil {
		return nil, errors.Wrapf(err, "error materializing labels")
	}

	results := make([]prom_storage.ChunkSeries, len(sLbls))
	for i, s := range sLbls {
		results[i] = &concreteChunksSeries{
			lbls: labels.New(s...),
		}
	}

	if !skipChunks {
		chks, err := m.materializeChunksSlice(ctx, rgi, mint, maxt, rr)
		if err != nil {
			return nil, errors.Wrap(err, "materializer failed to materialize chunks")
		}

		for i, result := range results {
			result.(*concreteChunksSeries).chks = chks[i]
		}

		// If we are not skipping chunks and there is no chunks for the time range queried, lets remove the series
		results = slices.DeleteFunc(results, func(cs prom_storage.ChunkSeries) bool {
			return len(cs.(*concreteChunksSeries).chks) == 0
		})
	}

	if err := m.materializedSeriesCallback(ctx, results); err != nil {
		return nil, err
	}
	return results, err
}

func (m *Materializer) MaterializeAllLabelNames() []string {
	r := make([]string, 0, len(m.b.LabelsFile().Schema().Columns()))
	for _, c := range m.b.LabelsFile().Schema().Columns() {
		lbl, ok := schema.ExtractLabelFromColumn(c[0])
		if !ok {
			continue
		}

		r = append(r, lbl)
	}
	return r
}

func (m *Materializer) MaterializeLabelNames(ctx context.Context, rgi int, rr []RowRange) ([]string, error) {
	labelsRg := m.b.LabelsFile().RowGroups()[rgi]
	cc := labelsRg.ColumnChunks()[m.colIdx]
	colsIdxs, err := m.materializeColumnToSlice(ctx, m.b.LabelsFile(), rgi, cc, rr, false)
	if err != nil {
		return nil, errors.Wrap(err, "materializer failed to materialize columns")
	}

	seen := make(map[string]struct{})
	colsMap := make(map[string]struct{}, 10)
	for _, colsIdx := range colsIdxs {
		key := util.YoloString(colsIdx.ByteArray())
		if _, ok := seen[key]; !ok {
			idxs, err := schema.DecodeUintSlice(colsIdx.ByteArray())
			if err != nil {
				return nil, errors.Wrap(err, "failed to decode column index")
			}
			for _, idx := range idxs {
				if _, ok := colsMap[m.b.LabelsFile().Schema().Columns()[idx][0]]; !ok {
					colsMap[m.b.LabelsFile().Schema().Columns()[idx][0]] = struct{}{}
				}
			}
		}
	}
	lbls := make([]string, 0, len(colsMap))
	for col := range colsMap {
		l, ok := schema.ExtractLabelFromColumn(col)
		if !ok {
			return nil, errors.New(fmt.Sprintf("error extracting label name from col %v", col))
		}
		lbls = append(lbls, l)
	}
	return lbls, nil
}

func (m *Materializer) MaterializeLabelValues(ctx context.Context, name string, rgi int, rr []RowRange) ([]string, error) {
	labelsRg := m.b.LabelsFile().RowGroups()[rgi]
	cIdx, ok := m.b.LabelsFile().Schema().Lookup(schema.LabelToColumn(name))
	if !ok {
		return []string{}, nil
	}
	cc := labelsRg.ColumnChunks()[cIdx.ColumnIndex]
	values, err := m.materializeColumnToSlice(ctx, m.b.LabelsFile(), rgi, cc, rr, false)
	if err != nil {
		return nil, errors.Wrap(err, "materializer failed to materialize columns")
	}

	r := make([]string, 0, len(values))
	vMap := make(map[string]struct{}, 10)
	for _, v := range values {
		strValue := util.YoloString(v.ByteArray())
		if _, ok := vMap[strValue]; !ok {
			r = append(r, strValue)
			vMap[strValue] = struct{}{}
		}
	}
	return r, nil
}

func (m *Materializer) MaterializeAllLabelValues(ctx context.Context, name string, rgi int) ([]string, error) {
	labelsRg := m.b.LabelsFile().RowGroups()[rgi]
	cIdx, ok := m.b.LabelsFile().Schema().Lookup(schema.LabelToColumn(name))
	if !ok {
		return []string{}, nil
	}
	cc := labelsRg.ColumnChunks()[cIdx.ColumnIndex]
	pages, err := m.b.LabelsFile().GetPages(ctx, cc, 0, 0)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get pages")
	}
	p, err := pages.ReadPage()
	if err != nil {
		return []string{}, errors.Wrap(err, "failed to read page")
	}
	defer parquet.Release(p)

	r := make([]string, 0, p.Dictionary().Len())
	for i := 0; i < p.Dictionary().Len(); i++ {
		r = append(r, p.Dictionary().Index(int32(i)).String())
	}
	return r, nil
}

func (m *Materializer) materializeAllLabels(ctx context.Context, rgi int, rr []RowRange) ([][]labels.Label, error) {
	labelsRg := m.b.LabelsFile().RowGroups()[rgi]
	cc := labelsRg.ColumnChunks()[m.colIdx]
	colsIdxs, err := m.materializeColumnToSlice(ctx, m.b.LabelsFile(), rgi, cc, rr, false)
	if err != nil {
		return nil, errors.Wrap(err, "materializer failed to materialize columns")
	}

	colsMap := make(map[int]*[]parquet.Value, 10)
	results := make([][]labels.Label, len(colsIdxs))

	for _, colsIdx := range colsIdxs {
		idxs, err := schema.DecodeUintSlice(colsIdx.ByteArray())
		if err != nil {
			return nil, errors.Wrap(err, "materializer failed to decode column index")
		}
		for _, idx := range idxs {
			colsMap[idx] = &[]parquet.Value{}
		}
	}

	errGroup, ctx := errgroup.WithContext(ctx)
	errGroup.SetLimit(m.concurrency)

	for cIdx, v := range colsMap {
		errGroup.Go(func() error {
			cc := labelsRg.ColumnChunks()[cIdx]
			values, err := m.materializeColumnToSlice(ctx, m.b.LabelsFile(), rgi, cc, rr, false)
			if err != nil {
				return errors.Wrap(err, "failed to materialize labels values")
			}
			*v = values
			return nil
		})
	}

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	for cIdx, values := range colsMap {
		labelName, ok := schema.ExtractLabelFromColumn(m.b.LabelsFile().Schema().Columns()[cIdx][0])
		if !ok {
			return nil, fmt.Errorf("column %d not found in schema", cIdx)
		}
		for i, value := range *values {
			if value.IsNull() {
				continue
			}
			results[i] = append(results[i], labels.Label{
				Name:  labelName,
				Value: util.YoloString(value.ByteArray()),
			})
		}
	}

	return results, nil
}

func totalRows(rr []RowRange) int64 {
	res := int64(0)
	for _, r := range rr {
		res += r.count
	}
	return res
}

func (m *Materializer) materializeChunksSlice(ctx context.Context, rgi int, mint, maxt int64, rr []RowRange) ([][]chunks.Meta, error) {
	minDataCol := m.s.DataColumIdx(mint)
	maxDataCol := m.s.DataColumIdx(maxt)
	rg := m.b.ChunksFile().RowGroups()[rgi]
	r := make([][]chunks.Meta, totalRows(rr))

	for i := minDataCol; i <= min(maxDataCol, len(m.dataColToIndex)-1); i++ {
		values, err := m.materializeColumnToSlice(ctx, m.b.ChunksFile(), rgi, rg.ColumnChunks()[m.dataColToIndex[i]], rr, true)
		if err != nil {
			return r, err
		}

		for vi, value := range values {
			chks, err := m.d.Decode(value.ByteArray(), mint, maxt)
			if err != nil {
				return r, errors.Wrap(err, "failed to decode chunks")
			}
			r[vi] = append(r[vi], chks...)
		}
	}

	return r, nil
}

//func materializeChunksLazyIter(ctx context.Context, rgi int, mint, maxt int64, rr []RowRange) {
//
//}

//func materializeColumnLazyIter(ctx context.Context, file *storage.ParquetFile, rgi int, cc parquet.ColumnChunk, rr []RowRange) ([]parquet.Value, error) {
//
//}

func (m *Materializer) materializeColumnToSlice(ctx context.Context, file *storage.ParquetFile, rgi int, cc parquet.ColumnChunk, rr []RowRange, chunkColumn bool) ([]parquet.Value, error) {
	pageRanges, err := m.getPageRangesForColummn(cc, file, rgi, rr, chunkColumn)
	if err != nil || len(pageRanges) == 0 {
		return nil, err
	}

	rowValues := make(map[int64]parquet.Value, len(pageRanges))
	rMutex := &sync.Mutex{}
	errGroup := &errgroup.Group{}
	errGroup.SetLimit(m.concurrency)

	dictOff, dictSz := file.DictionaryPageBounds(rgi, cc.Column())
	cc.Type()

	for _, p := range pageRanges {
		errGroup.Go(func() error {
			pageRangeValues, err := m.materializePageRangeToSlice(ctx, file, p, dictOff, dictSz, cc)
			if err != nil {
				return errors.Wrap(err, "failed to materialize page range")
			}
			rMutex.Lock()
			for rowRange, value := range pageRangeValues {
				rowValues[rowRange] = value
			}
			rMutex.Unlock()
			return nil
		})
	}
	err = errGroup.Wait()
	if err != nil {
		return nil, errors.Wrap(err, "failed to materialize columns")
	}

	rows := slices.Collect(maps.Keys(rowValues))
	slices.Sort(rows)

	res := make([]parquet.Value, 0, totalRows(rr))
	for _, v := range rows {
		res = append(res, rowValues[v])
	}
	return res, nil
}

func (m *Materializer) materializePageRangeToSlice(
	ctx context.Context,
	file *storage.ParquetFile,
	p pageToReadWithRow,
	dictOff uint64,
	dictSz uint64,
	cc parquet.ColumnChunk,
) (map[int64]parquet.Value, error) {
	values := make(map[int64]parquet.Value, totalRows(p.rows))

	rowValuesIter, err := newRowRangesValueIterator(
		ctx, file, cc, p, dictOff, dictSz,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create row values iterator for page")
	}
	defer func() { _ = rowValuesIter.Close() }()

	for rowValuesIter.Next() {
		rowIdx, rowRangeValue := rowValuesIter.At()
		values[rowIdx] = rowRangeValue
	}
	if err = rowValuesIter.Err(); err != nil {
		return nil, err
	}

	return values, nil
}

type pageToReadWithRow struct {
	pageToRead
	rows []RowRange
}

func (m *Materializer) getPageRangesForColummn(cc parquet.ColumnChunk, file *storage.ParquetFile, rgi int, rr []RowRange, chunkColumn bool) ([]pageToReadWithRow, error) {
	if len(rr) == 0 {
		return nil, nil
	}

	oidx, err := cc.OffsetIndex()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get offset index")
	}

	cidx, err := cc.ColumnIndex()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get column index")
	}

	group := file.RowGroups()[rgi]
	pagesToRowsMap := make(map[int][]RowRange, len(rr))

	for i := 0; i < cidx.NumPages(); i++ {
		pageRowRange := RowRange{
			from: oidx.FirstRowIndex(i),
		}
		pageRowRange.count = group.NumRows()

		if i < oidx.NumPages()-1 {
			pageRowRange.count = oidx.FirstRowIndex(i+1) - pageRowRange.from
		}

		for _, r := range rr {
			if pageRowRange.Overlaps(r) {
				pagesToRowsMap[i] = append(pagesToRowsMap[i], pageRowRange.Intersection(r))
			}
		}
	}
	if err := m.checkBytesQuota(maps.Keys(pagesToRowsMap), oidx, chunkColumn); err != nil {
		return nil, err
	}

	pageRanges := m.coalescePageRanges(pagesToRowsMap, oidx)
	return pageRanges, nil
}

// Merge nearby pages to enable efficient sequential reads.
// Pages that are not close to each other will be scheduled for concurrent reads.
func (m *Materializer) coalescePageRanges(pagedIdx map[int][]RowRange, offset parquet.OffsetIndex) []pageToReadWithRow {
	if len(pagedIdx) == 0 {
		return []pageToReadWithRow{}
	}
	idxs := make([]int, 0, len(pagedIdx))
	for idx := range pagedIdx {
		idxs = append(idxs, idx)
	}

	slices.Sort(idxs)

	parts := m.partitioner.Partition(len(idxs), func(i int) (int, int) {
		return int(offset.Offset(idxs[i])), int(offset.Offset(idxs[i]) + offset.CompressedPageSize(idxs[i]))
	})

	r := make([]pageToReadWithRow, 0, len(parts))
	for _, part := range parts {
		pagesToRead := pageToReadWithRow{}
		for i := part.ElemRng[0]; i < part.ElemRng[1]; i++ {
			pagesToRead.rows = append(pagesToRead.rows, pagedIdx[idxs[i]]...)
		}
		pagesToRead.pfrom = int64(part.ElemRng[0])
		pagesToRead.pto = int64(part.ElemRng[1])
		pagesToRead.off = part.Start
		pagesToRead.csz = part.End - part.Start
		pagesToRead.rows = simplify(pagesToRead.rows)
		r = append(r, pagesToRead)
	}

	return r
}

func (m *Materializer) checkRowCountQuota(rr []RowRange) error {
	if err := m.rowCountQuota.Reserve(totalRows(rr)); err != nil {
		return fmt.Errorf("would fetch too many rows: %w", err)
	}
	return nil
}

func (m *Materializer) checkBytesQuota(pages iter.Seq[int], oidx parquet.OffsetIndex, chunkColumn bool) error {
	total := totalBytes(pages, oidx)
	if chunkColumn {
		if err := m.chunkBytesQuota.Reserve(total); err != nil {
			return fmt.Errorf("would fetch too many chunk bytes: %w", err)
		}
	}
	if err := m.dataBytesQuota.Reserve(total); err != nil {
		return fmt.Errorf("would fetch too many data bytes: %w", err)
	}
	return nil
}

func totalBytes(pages iter.Seq[int], oidx parquet.OffsetIndex) int64 {
	res := int64(0)
	for i := range pages {
		res += oidx.CompressedPageSize(i)
	}
	return res
}
