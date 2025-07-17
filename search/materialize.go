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

	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	prom_storage "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"golang.org/x/sync/errgroup"

	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/storage"
	"github.com/prometheus-community/parquet-common/util"
)

var tracer = otel.Tracer("parquet-common")

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

	materializedSeriesCallback       MaterializedSeriesFunc
	materializedLabelsFilterCallback MaterializedLabelsFilterCallback
}

// MaterializedSeriesFunc is a callback function that can be used to add limiter or statistic logics for
// materialized series.
type MaterializedSeriesFunc func(ctx context.Context, series []prom_storage.ChunkSeries) error

// NoopMaterializedSeriesFunc is a noop callback function that does nothing.
func NoopMaterializedSeriesFunc(_ context.Context, _ []prom_storage.ChunkSeries) error {
	return nil
}

// MaterializedLabelsFilterCallback returns a filter and a boolean indicating if the filter is enabled or not.
// The filter is used to filter series based on their labels.
// The boolean if set to false then it means that the filter is a noop and we can take shortcut to include all series.
// Otherwise, the filter is used to filter series based on their labels.
type MaterializedLabelsFilterCallback func(ctx context.Context, hints *prom_storage.SelectHints) (MaterializedLabelsFilter, bool)

// MaterializedLabelsFilter is a filter that can be used to filter series based on their labels.
type MaterializedLabelsFilter interface {
	// Filter returns true if the labels should be included in the result.
	Filter(ls labels.Labels) bool
	// Close is used to close the filter and do some cleanup.
	Close()
}

// NoopMaterializedLabelsFilterCallback is a noop MaterializedLabelsFilterCallback function that filters nothing.
func NoopMaterializedLabelsFilterCallback(ctx context.Context, hints *prom_storage.SelectHints) (MaterializedLabelsFilter, bool) {
	return nil, false
}

func NewMaterializer(s *schema.TSDBSchema,
	d *schema.PrometheusParquetChunksDecoder,
	block storage.ParquetShard,
	concurrency int,
	rowCountQuota *Quota,
	chunkBytesQuota *Quota,
	dataBytesQuota *Quota,
	materializeSeriesCallback MaterializedSeriesFunc,
	materializeLabelsFilterCallback MaterializedLabelsFilterCallback,
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
		s:                                s,
		d:                                d,
		b:                                block,
		colIdx:                           colIdx.ColumnIndex,
		concurrency:                      concurrency,
		partitioner:                      util.NewGapBasedPartitioner(block.ChunksFile().Cfg.PagePartitioningMaxGapSize),
		dataColToIndex:                   dataColToIndex,
		rowCountQuota:                    rowCountQuota,
		chunkBytesQuota:                  chunkBytesQuota,
		dataBytesQuota:                   dataBytesQuota,
		materializedSeriesCallback:       materializeSeriesCallback,
		materializedLabelsFilterCallback: materializeLabelsFilterCallback,
	}, nil
}

// Materialize reconstructs the ChunkSeries that belong to the specified row ranges (rr).
// It uses the row group index (rgi) and time bounds (mint, maxt) to filter and decode the series.
func (m *Materializer) Materialize(ctx context.Context, hints *prom_storage.SelectHints, rgi int, mint, maxt int64, skipChunks bool, rr []RowRange) (chunkSeries []prom_storage.ChunkSeries, err error) {
	ctx, span := tracer.Start(ctx, "Materializer.Materialize")
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	span.SetAttributes(
		attribute.Int("row_group_index", rgi),
		attribute.Int64("mint", mint),
		attribute.Int64("maxt", maxt),
		attribute.Bool("skip_chunks", skipChunks),
		attribute.Int("row_ranges_count", len(rr)),
	)

	if err := m.checkRowCountQuota(rr); err != nil {
		return nil, err
	}
	sLbls, err := m.materializeAllLabels(ctx, rgi, rr)
	if err != nil {
		return nil, errors.Wrapf(err, "error materializing labels")
	}

	chunkSeries, rr = m.filterSeries(ctx, hints, sLbls, rr)
	if !skipChunks {
		chksIter, err := m.materializeChunksIter(ctx, rgi, mint, maxt, rr)
		if err != nil {
			return nil, errors.Wrap(err, "materializer failed to create chunks iterator")
		}
		seriesIdx := 0
		for chksIter.Next() {
			chkIter := chksIter.At()
			var iterChks []chunks.Meta
			for chkIter.Next() {
				iterChk := chkIter.At()
				iterChks = append(iterChks, iterChk)
			}
			chunkSeries[seriesIdx].(*concreteChunksSeries).chks = iterChks
			seriesIdx++
		}

		// If we are not skipping chunks and there is no chunks for the time range queried, lets remove the series
		chunkSeries = slices.DeleteFunc(chunkSeries, func(cs prom_storage.ChunkSeries) bool {
			return len(cs.(*concreteChunksSeries).chks) == 0
		})
	}

	if err := m.materializedSeriesCallback(ctx, chunkSeries); err != nil {
		return nil, err
	}

	span.SetAttributes(attribute.Int("materialized_series_count", len(chunkSeries)))
	return chunkSeries, err
}

func (m *Materializer) filterSeries(ctx context.Context, hints *prom_storage.SelectHints, sLbls [][]labels.Label, rr []RowRange) ([]prom_storage.ChunkSeries, []RowRange) {
	results := make([]prom_storage.ChunkSeries, 0, len(sLbls))
	labelsFilter, ok := m.materializedLabelsFilterCallback(ctx, hints)
	if !ok {
		for _, s := range sLbls {
			results = append(results, &concreteChunksSeries{
				lbls: labels.New(s...),
			})
		}
		return results, rr
	}

	defer labelsFilter.Close()

	filteredRR := make([]RowRange, 0, len(rr))
	var currentRange RowRange
	inRange := false
	seriesIdx := 0

	for _, rowRange := range rr {
		for i := int64(0); i < rowRange.Count; i++ {
			actualRowID := rowRange.From + i
			lbls := labels.New(sLbls[seriesIdx]...)

			if labelsFilter.Filter(lbls) {
				results = append(results, &concreteChunksSeries{
					lbls: lbls,
				})

				// Handle row range collection
				if !inRange {
					// Start new range
					currentRange = RowRange{
						From:  actualRowID,
						Count: 1,
					}
					inRange = true
				} else if actualRowID == currentRange.From+currentRange.Count {
					// Extend current range
					currentRange.Count++
				} else {
					// Save current range and start new range (non-contiguous)
					filteredRR = append(filteredRR, currentRange)
					currentRange = RowRange{
						From:  actualRowID,
						Count: 1,
					}
				}
			} else {
				// Save current range and reset when we hit a non-matching series
				if inRange {
					filteredRR = append(filteredRR, currentRange)
					inRange = false
				}
			}
			seriesIdx++
		}
	}

	// Save the final range if we have one
	if inRange {
		filteredRR = append(filteredRR, currentRange)
	}

	return results, filteredRR
}

// MaterializeIter provides an iterator to lazily reconstruct the ChunkSeries that belong to the specified row ranges (rr).
// It uses the row group index (rgi) and time bounds (mint, maxt) to filter and decode the series.

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
	colsIdxs, err := m.materializeColumnSlice(ctx, m.b.LabelsFile(), rgi, rr, cc, false)
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
			return nil, errors.New(fmt.Sprintf("error extracting label name From col %v", col))
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
	values, err := m.materializeColumnSlice(ctx, m.b.LabelsFile(), rgi, rr, cc, false)
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
	colsIdxs, err := m.materializeColumnSlice(ctx, m.b.LabelsFile(), rgi, rr, cc, false)
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
			values, err := m.materializeColumnSlice(ctx, m.b.LabelsFile(), rgi, rr, cc, false)
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
		res += r.Count
	}
	return res
}

func (m *Materializer) materializeChunksIter(ctx context.Context, rgi int, mint, maxt int64, rr []RowRange) (chunkIterableSet, error) {
	minDataCol := m.s.DataColumIdx(mint)
	maxDataCol := m.s.DataColumIdx(maxt)
	rg := m.b.ChunksFile().RowGroups()[rgi]

	var columnValueIterators []*columnValueIterator
	for i := minDataCol; i <= min(maxDataCol, len(m.dataColToIndex)-1); i++ {
		cc := rg.ColumnChunks()[m.dataColToIndex[i]]
		columnValueIter, err := m.materializeColumnIter(ctx, m.b.ChunksFile(), rgi, rr, cc, true)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create column value iterator")
		}
		columnValueIterators = append(columnValueIterators, columnValueIter)
	}
	return &multiColumnChunksDecodingIterator{
		mint:                 mint,
		maxt:                 maxt,
		columnValueIterators: columnValueIterators,
		d:                    m.d,
	}, nil
}

func (m *Materializer) materializeChunksSlice(ctx context.Context, rgi int, mint, maxt int64, rr []RowRange) ([][]chunks.Meta, error) {
	minDataCol := m.s.DataColumIdx(mint)
	maxDataCol := m.s.DataColumIdx(maxt)
	rg := m.b.ChunksFile().RowGroups()[rgi]
	rowChunks := make([][]chunks.Meta, totalRows(rr))

	for dataColIdx := minDataCol; dataColIdx <= min(maxDataCol, len(m.dataColToIndex)-1); dataColIdx++ {
		cc := rg.ColumnChunks()[m.dataColToIndex[dataColIdx]]
		valuesIter, err := m.materializeColumnIter(ctx, m.b.ChunksFile(), rgi, rr, cc, true)
		if err != nil {
			return rowChunks, err
		}

		i := 0
		for valuesIter.Next() {
			iterChks, err := m.d.Decode(valuesIter.At().ByteArray(), mint, maxt)
			if err != nil {
				return rowChunks, errors.Wrap(err, "failed to decode chunks From iterator")
			}
			rowChunks[i] = append(rowChunks[i], iterChks...)
			i++
		}
	}

	return rowChunks, nil
}

func (m *Materializer) materializeColumnIter(
	ctx context.Context,
	file *storage.ParquetFile,
	rgi int,
	rr []RowRange,
	cc parquet.ColumnChunk,
	chunkColumn bool,
) (*columnValueIterator, error) {
	pageRanges, err := m.getPageRangesForColummn(cc, file, rgi, rr, chunkColumn)
	if err != nil {
		return nil, err
	}

	dictOff, dictSz := file.DictionaryPageBounds(rgi, cc.Column())

	rowRangesValueIterators := make([]*rowRangesValueIterator, len(pageRanges))
	for i, pageRange := range pageRanges {
		rowRangesValueIter, err := newRowRangesValueIterator(
			ctx, file, cc, pageRange, dictOff, dictSz,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create row value iterator for page range")
		}
		rowRangesValueIterators[i] = rowRangesValueIter
	}
	return &columnValueIterator{
		rowRangesIterators: rowRangesValueIterators,
	}, nil
}

func (m *Materializer) materializeColumnSlice(
	ctx context.Context,
	file *storage.ParquetFile,
	rgi int,
	rr []RowRange,
	cc parquet.ColumnChunk,
	chunkColumn bool,
) ([]parquet.Value, error) {
	pageRanges, err := m.getPageRangesForColummn(cc, file, rgi, rr, chunkColumn)
	if err != nil || len(pageRanges) == 0 {
		return nil, err
	}

	pageRangesValues := make([][]parquet.Value, len(pageRanges))

	dictOff, dictSz := file.DictionaryPageBounds(rgi, cc.Column())
	errGroup := &errgroup.Group{}
	errGroup.SetLimit(m.concurrency)
	for i, pageRange := range pageRanges {
		errGroup.Go(func() error {
			valuesIter, err := m.materializePageRangeIter(ctx, file, pageRange, dictOff, dictSz, cc)
			if err != nil {
				return errors.Wrap(err, "failed to materialize page range iterator")
			}
			defer func() { _ = valuesIter.Close() }()

			iterValues := make([]parquet.Value, 0, totalRows(pageRange.rows))
			for valuesIter.Next() {
				iterValues = append(iterValues, valuesIter.At())
			}
			if err = valuesIter.Err(); err != nil {
				return err
			}

			pageRangesValues[i] = iterValues
			return nil
		})
	}
	err = errGroup.Wait()
	if err != nil {
		return nil, errors.Wrap(err, "failed to materialize columns")
	}

	type sortablePageRange struct {
		pageRange           pageToReadWithRow
		pageRangesValuesIdx int
	}

	// Do not assume the page ranges are in order; partitions may not prioritize sequential reads.
	pageRangesSorted := make([]sortablePageRange, len(pageRanges))
	for i, pageRange := range pageRanges {
		pageRangesSorted[i] = sortablePageRange{
			pageRange:           pageRange,
			pageRangesValuesIdx: i,
		}
	}
	slices.SortFunc(pageRangesSorted, func(a, b sortablePageRange) int {
		if a.pageRange.pfrom < b.pageRange.pfrom {
			return -1
		} else if a.pageRange.pfrom > b.pageRange.pfrom {
			return 1
		}
		return 0
	})

	res := make([]parquet.Value, 0, totalRows(rr))
	for _, sortedPageRange := range pageRangesSorted {
		// Within a page range, the values are in order
		res = append(res, pageRangesValues[sortedPageRange.pageRangesValuesIdx]...)
	}
	return res, nil
}

func equalValues(expected, actual parquet.Value) bool {
	expectedBytes := expected.ByteArray()
	actualBytes := actual.ByteArray()
	for i := 0; i < len(expectedBytes); i++ {
		if expectedBytes[i] != actualBytes[i] {
			return false
		}
	}
	return true
}

func (m *Materializer) materializePageRangeIter(
	ctx context.Context,
	file *storage.ParquetFile,
	p pageToReadWithRow,
	dictOff uint64,
	dictSz uint64,
	cc parquet.ColumnChunk,
) (*rowRangesValueIterator, error) {
	rowValuesIter, err := newRowRangesValueIterator(
		ctx, file, cc, p, dictOff, dictSz,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create row values iterator for page")
	}
	return rowValuesIter, nil
}

func (m *Materializer) materializePageRangeSlice(
	ctx context.Context,
	file *storage.ParquetFile,
	p pageToReadWithRow,
	dictOff uint64,
	dictSz uint64,
	cc parquet.ColumnChunk,
) ([]parquet.Value, error) {
	values := make([]parquet.Value, totalRows(p.rows))

	rowValuesIter, err := newRowRangesValueIterator(
		ctx, file, cc, p, dictOff, dictSz,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create row values iterator for page")
	}
	defer func() { _ = rowValuesIter.Close() }()

	i := 0
	for rowValuesIter.Next() {
		values[i] = rowValuesIter.At()
		i++
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
			From: oidx.FirstRowIndex(i),
		}
		pageRowRange.Count = group.NumRows()

		if i < oidx.NumPages()-1 {
			pageRowRange.Count = oidx.FirstRowIndex(i+1) - pageRowRange.From
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
