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
	"slices"
	"sort"

	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	prom_storage "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"
	"golang.org/x/sync/errgroup"

	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/storage"
	"github.com/prometheus-community/parquet-common/util"
)

type Materializer struct {
	b           storage.ParquetShard
	s           *schema.TSDBSchema
	d           *schema.PrometheusParquetChunksDecoder
	partitioner util.Partitioner
	ctx         context.Context

	colIdx      int
	concurrency int

	dataColToIndex []int
}

func NewMaterializer(ctx context.Context, s *schema.TSDBSchema,
	d *schema.PrometheusParquetChunksDecoder,
	block storage.ParquetShard,
	concurrency int,
	maxGapPartitioning int,
) (*Materializer, error) {
	colIdx, ok := block.LabelsFile().Schema().Lookup(schema.ColIndexes)
	if !ok {
		return nil, fmt.Errorf("schema index %s not found", schema.ColIndexes)
	}

	dataColToIndex := make([]int, len(block.ChunksFile().Schema().Columns()))
	for i := 0; i < len(s.DataColsIndexes); i++ {
		c, ok := block.ChunksFile().Schema().Lookup(schema.DataColumn(i))
		if !ok {
			return nil, fmt.Errorf("schema column %s not found", schema.DataColumn(i))
		}

		dataColToIndex[i] = c.ColumnIndex
	}

	return &Materializer{
		ctx:            ctx,
		s:              s,
		d:              d,
		b:              block,
		colIdx:         colIdx.ColumnIndex,
		concurrency:    concurrency,
		partitioner:    util.NewGapBasedPartitioner(maxGapPartitioning),
		dataColToIndex: dataColToIndex,
	}, nil
}

// Materialize reconstructs the ChunkSeries that belong to the specified row ranges (rr).
// It uses the row group index (rgi) and time bounds (mint, maxt) to filter and decode the series.
// Labels are loaded upfront, but chunks are loaded on-demand when iterating.
func (m *Materializer) Materialize(ctx context.Context, rgi int, mint, maxt int64, skipChunks bool, rr []RowRange) (prom_storage.ChunkSeriesSet, error) {
	sLbls, err := m.materializeAllLabels(ctx, rgi, rr)
	if err != nil {
		return nil, errors.Wrapf(err, "error materializing labels")
	}

	results := make([]prom_storage.ChunkSeries, 0, len(sLbls))
	for _, s := range sLbls {
		sort.Sort(s)
	}
	if skipChunks {
		for _, s := range sLbls {
			results = append(results, &concreteChunksSeries{
				lbls: s,
			})
		}
		return convert.NewChunksSeriesSet(results), nil
	}

	chunkIters := m.materializeChunks(rgi, mint, maxt, rr)

	return newFilterEmptyChunkSeriesSet(sLbls, chunkIters), nil
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
	colsIdxs, err := m.materializeColumnAsSlice(ctx, m.b.LabelsFile(), labelsRg, cc, rr)
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
			return nil, fmt.Errorf("error extracting label name from col %v", col)
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
	values, err := m.materializeColumnAsSlice(ctx, m.b.LabelsFile(), labelsRg, cc, rr)
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
	pages, err := m.b.LabelsFile().GetPages(ctx, cc)
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

func (m *Materializer) materializeAllLabels(ctx context.Context, rgi int, rr []RowRange) ([]labels.Labels, error) {
	labelsRg := m.b.LabelsFile().RowGroups()[rgi]
	cc := labelsRg.ColumnChunks()[m.colIdx]
	colsIdxs, err := m.materializeColumnAsSlice(ctx, m.b.LabelsFile(), labelsRg, cc, rr)
	if err != nil {
		return nil, errors.Wrap(err, "materializer failed to materialize columns")
	}

	colsMap := make(map[int]*[]parquet.Value, 10)
	results := make([]labels.Labels, len(colsIdxs))

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
			values, err := m.materializeColumnAsSlice(ctx, m.b.LabelsFile(), labelsRg, cc, rr)
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

func (m *Materializer) materializeChunks(rgi int, mint, maxt int64, rr []RowRange) chunkIterableSet {
	minDataCol := m.s.DataColumIdx(mint)
	maxDataCol := m.s.DataColumIdx(maxt)
	rg := m.b.ChunksFile().RowGroups()[rgi]

	dataColCount := min(maxDataCol, len(m.dataColToIndex)-1) - minDataCol + 1

	if dataColCount <= 0 {
		return &repeatEmptyChunkIterableSet{count: totalRows(rr)}
	}

	result := &chunksIteratorIterator{
		d:    m.d,
		mint: mint,
		maxt: maxt,
	}

	for colIdx := range dataColCount {
		iter := m.materializeColumn(m.ctx, m.b.ChunksFile(), rg, rg.ColumnChunks()[m.dataColToIndex[minDataCol+colIdx]], rr)
		result.colIts = append(result.colIts, iter)
	}
	return result
}

func (m *Materializer) materializeColumn(ctx context.Context, file *storage.ParquetFile, group parquet.RowGroup, cc parquet.ColumnChunk, rr []RowRange) *columnIterator {
	if len(rr) == 0 {
		return &columnIterator{}
	}

	pageBatches, err := m.getPageRanges(group, cc, rr)
	if err != nil {
		return &columnIterator{
			err: errors.Wrap(err, "failed to get page ranges"),
		}
	}

	return &columnIterator{
		ctx:         ctx,
		file:        file,
		cc:          cc,
		pageBatches: pageBatches,
		iter: &pagesBatchIterator{
			pi: new(pageIterator),
		},
	}
}

// materializeColumnAsSlice internally calls materializeColumn and collects all values into a slice.
// It is convenient for callers who don't want to stream values.
func (m *Materializer) materializeColumnAsSlice(ctx context.Context, file *storage.ParquetFile, group parquet.RowGroup, cc parquet.ColumnChunk, rr []RowRange) ([]parquet.Value, error) {
	iter := m.materializeColumn(ctx, file, group, cc, rr)
	values := make([]parquet.Value, 0, totalRows(rr))
	for iter.Next() {
		if iter.Err() != nil {
			return nil, iter.Err()
		}
		values = append(values, iter.At())
	}
	if err := iter.Err(); err != nil {
		return nil, errors.Wrap(err, "error iterating column values")
	}
	if len(values) != int(totalRows(rr)) {
		return nil, fmt.Errorf("expected %d values, got %d", totalRows(rr), len(values))
	}
	return values, nil
}

type pageEntryRead struct {
	pages []int
	rows  []RowRange
}

func (m *Materializer) getPageRanges(group parquet.RowGroup, cc parquet.ColumnChunk, rr []RowRange) ([]pageEntryRead, error) {
	oidx, err := cc.OffsetIndex()
	if err != nil {
		return nil, errors.Wrap(err, "could not get offset index")
	}

	cidx, err := cc.ColumnIndex()
	if err != nil {
		return nil, errors.Wrap(err, "could not get column index")
	}

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

	pageRanges := m.coalescePageRanges(pagesToRowsMap, oidx)
	return pageRanges, nil
}

// Merge nearby pages to enable efficient sequential reads.
// Pages that are not close to each other will be scheduled for concurrent reads.
func (m *Materializer) coalescePageRanges(pagedIdx map[int][]RowRange, offset parquet.OffsetIndex) []pageEntryRead {
	if len(pagedIdx) == 0 {
		return []pageEntryRead{}
	}
	idxs := make([]int, 0, len(pagedIdx))
	for idx := range pagedIdx {
		idxs = append(idxs, idx)
	}

	slices.Sort(idxs)

	parts := m.partitioner.Partition(len(idxs), func(i int) (int, int) {
		return int(offset.Offset(idxs[i])), int(offset.Offset(idxs[i]) + offset.CompressedPageSize(idxs[i]))
	})

	r := make([]pageEntryRead, 0, len(parts))
	for _, part := range parts {
		pagesToRead := pageEntryRead{}
		for i := part.ElemRng[0]; i < part.ElemRng[1]; i++ {
			pagesToRead.pages = append(pagesToRead.pages, idxs[i])
			pagesToRead.rows = append(pagesToRead.rows, pagedIdx[idxs[i]]...)
		}
		pagesToRead.rows = simplify(pagesToRead.rows)
		r = append(r, pagesToRead)
	}

	return r
}

// columnIterator iterates through the values of a single column chunk,
// partitioning reads into batches of pages.
type columnIterator struct {
	ctx         context.Context
	file        *storage.ParquetFile
	cc          parquet.ColumnChunk
	pageBatches []pageEntryRead

	iter *pagesBatchIterator
	err  error
}

func (c *columnIterator) Next() bool {
	rv := c.next()
	if !rv && c.iter != nil {
		c.iter.release()
	}
	return rv
}

func (c *columnIterator) next() bool {
	if c.iter == nil || c.err != nil {
		return false
	}
	if c.iter.Next() {
		return true
	}

	if c.iter.Err() != nil {
		return false
	}

	if len(c.pageBatches) == 0 {
		return false
	}
	c.iter.reset(c.ctx, c.file, c.cc, c.pageBatches[0])
	c.pageBatches = c.pageBatches[1:]
	return c.Next()
}

func (c *columnIterator) At() parquet.Value {
	return c.iter.At()
}

func (c *columnIterator) Err() error {
	if c.err != nil {
		return c.err
	}
	if c.iter != nil {
		return c.iter.Err()
	}
	return nil
}

// columnIterator iterates through the values of a batch of pages.
type pagesBatchIterator struct {
	pgs *parquet.FilePages

	pi *pageIterator

	remainingRr []RowRange
	currentRr   RowRange
	next        int64
	remaining   int64
	currentRow  int64

	currentValue parquet.Value
	err          error
	done         bool
}

// release releases any pooled resources held by the iterator, and should be
// called when the iterator is no longer needed. Although helpful for efficient
// memory management, it is not strictly necessary to call this method.
func (c *pagesBatchIterator) release() {
	if c.pgs != nil {
		_ = c.pgs.Close()
		c.pgs = nil
	}
	c.pi.release()
}

func (c *pagesBatchIterator) Next() bool {
	if c.done || c.err != nil {
		return false
	}

	if len(c.remainingRr) == 0 && c.remaining == 0 {
		c.done = true
		return false
	}

	for c.pi.Next() {
		if c.currentRow == c.next {
			c.currentValue = c.pi.At()
			c.remaining--
			if c.remaining > 0 {
				c.next++
			} else if len(c.remainingRr) > 0 {
				c.currentRr = c.remainingRr[0]
				c.next = c.currentRr.from
				c.remaining = c.currentRr.count
				c.remainingRr = c.remainingRr[1:]
			}
			c.currentRow++
			return true
		}
		c.currentRow++
	}
	if c.pi.Error() != nil {
		c.err = c.pi.Error()
		return false
	}
	c.pi.readPage(c.pgs)
	return c.Next()
}

func (c *pagesBatchIterator) At() parquet.Value {
	return c.currentValue
}

func (c *pagesBatchIterator) Err() error {
	return c.err
}

func (c *pagesBatchIterator) reset(ctx context.Context, file *storage.ParquetFile, cc parquet.ColumnChunk, batch pageEntryRead) {
	if c.pgs != nil {
		_ = c.pgs.Close()
		c.pgs = nil
	}

	c.err = nil
	c.done = false
	c.currentValue = parquet.Value{}
	c.remainingRr = batch.rows
	if len(c.remainingRr) == 0 {
		c.remaining = 0
		return
	}
	pgs, err := file.GetPages(ctx, cc, batch.pages...)
	if err != nil {
		c.err = errors.Wrap(err, "failed to get pages")
		return
	}
	c.pgs = pgs
	err = c.pgs.SeekToRow(c.remainingRr[0].from)
	if err != nil {
		_ = c.pgs.Close()
		c.pgs = nil
		c.err = errors.Wrap(err, "could not seek to row")
		return
	}

	c.currentRr = c.remainingRr[0]
	c.next = c.currentRr.from
	c.remaining = c.currentRr.count
	c.currentRow = c.currentRr.from
	c.remainingRr = c.remainingRr[1:]
	c.pi.readPage(c.pgs)
}

// pageIterator iterates through the values of a single page.
type pageIterator struct {
	p parquet.Page

	// TODO: consider using unique.Handle
	cachedSymbols map[int32]parquet.Value
	st            symbolTable

	vr parquet.ValueReader

	current            int
	buffer             []parquet.Value
	currentBufferIndex int
	err                error
	ready              bool
}

// release releases any pooled resources held by the iterator, and should be
// called when the iterator is no longer needed. Although helpful for efficient
// memory management, it is not strictly necessary to call this method.
func (pi *pageIterator) release() {
	parquet.Release(pi.p)
}

func (pi *pageIterator) readPage(pgs parquet.Pages) {
	parquet.Release(pi.p)
	p, err := pgs.ReadPage()
	if err != nil {
		pi.err = errors.Wrap(err, "failed to read page")
		return
	}
	pi.p = p
	pi.vr = nil
	if p.Dictionary() != nil {
		pi.st.Reset(p)
		pi.cachedSymbols = make(map[int32]parquet.Value, p.Dictionary().Len())
	} else {
		pi.vr = p.Values()
		pi.buffer = make([]parquet.Value, 0, 128)
		pi.currentBufferIndex = -1
	}
	pi.current = -1
	pi.ready = true
}

func (pi *pageIterator) Next() bool {
	if pi.err != nil || !pi.ready {
		return false
	}

	pi.current++
	if pi.current >= int(pi.p.NumRows()) {
		return false
	}

	pi.currentBufferIndex++

	if pi.currentBufferIndex == len(pi.buffer) {
		n, err := pi.vr.ReadValues(pi.buffer[:cap(pi.buffer)])
		if err != nil && err != io.EOF {
			pi.err = err
		}
		pi.buffer = pi.buffer[:n]
		pi.currentBufferIndex = 0
	}

	return true
}

func (pi *pageIterator) Error() error {
	return pi.err
}

func (pi *pageIterator) At() parquet.Value {
	if pi.vr == nil {
		dicIndex := pi.st.GetIndex(pi.current)
		// Cache a clone of the current symbol table entry.
		// This allows us to release the original page while avoiding unnecessary future clones.
		if _, ok := pi.cachedSymbols[dicIndex]; !ok {
			pi.cachedSymbols[dicIndex] = pi.st.Get(pi.current).Clone()
		}
		return pi.cachedSymbols[dicIndex]
	}

	return pi.buffer[pi.currentBufferIndex].Clone()
}

type chunkIterableSet interface {
	Next() bool
	At() chunks.Iterator
	Err() error
}

// chunksIteratorIterator iterates multiple columns at once, zipping their
// values together to decode them into chunks. The column iterators are expected
// iterate through the same rows in the same order, and an error is returned if
// they have different lengths.
type chunksIteratorIterator struct {
	colIts []*columnIterator
	mint   int64
	maxt   int64
	d      *schema.PrometheusParquetChunksDecoder

	current *chunksIterator
	err     error
}

func (c *chunksIteratorIterator) Next() bool {
	if c.err != nil || len(c.colIts) == 0 {
		return false
	}
	// TODO: with the current implementation we can't reuse the values buffer
	// across iterations, because each chunks iterator we return has its own
	// lifetime. We could make it explicit that it is unsafe to use old
	// iterators after calling Next(), but that would break the
	// filterEmptyChunkSeriesSet. If we find a way to filter empty series in
	// advance, we could drop that. On the other hand, Prometheus'
	// ChunkSeriesSet explicitly says: "Returned series should be iterable even
	// after Next is called.".
	values := make([]parquet.Value, 0, len(c.colIts))
	for _, it := range c.colIts {
		if !it.Next() {
			c.err = it.Err()
			if c.err != nil {
				return false
			}
			continue
		}
		values = append(values, it.At())
	}
	if len(values) == 0 {
		return false
	}
	if len(values) != len(c.colIts) {
		c.err = fmt.Errorf("unexpected mismatch in column iterators: expected %d values, got %d", len(c.colIts), len(values))
		return false
	}
	c.current = &chunksIterator{
		mint:   c.mint,
		maxt:   c.maxt,
		values: values,
		d:      c.d,
	}
	return true
}

func (c *chunksIteratorIterator) At() chunks.Iterator {
	return c.current
}

func (c *chunksIteratorIterator) Err() error {
	return c.err
}

type repeatEmptyChunkIterableSet struct {
	count int64
}

func (r *repeatEmptyChunkIterableSet) Next() bool {
	if r.count == 0 {
		return false
	}
	r.count--
	return true
}

func (r *repeatEmptyChunkIterableSet) At() chunks.Iterator {
	return prom_storage.NewListChunkSeriesIterator()
}

func (r *repeatEmptyChunkIterableSet) Err() error {
	return nil
}

// chunksIterator iterates through a values slice and lazily decodes values into
// chunks.
type chunksIterator struct {
	// TODO: why do we need these for decoding? can't we discard out of range chunks in advance?
	mint   int64
	maxt   int64
	values []parquet.Value
	d      *schema.PrometheusParquetChunksDecoder

	decoded []chunks.Meta
	current chunks.Meta
	err     error
}

func (c *chunksIterator) Next() bool {
	if c.err != nil {
		return false
	}
	if len(c.values) == 0 && len(c.decoded) == 0 {
		return false
	}
	if len(c.decoded) > 0 {
		c.current = c.decoded[0]
		c.decoded = c.decoded[1:]
		return true
	}
	value := c.values[0]
	c.values = c.values[1:]

	// TODO: we can do better at pooling here.
	c.decoded, c.err = c.d.Decode(value.ByteArray(), c.mint, c.maxt)
	return c.Next()
}

func (c *chunksIterator) At() chunks.Meta {
	return c.current
}

func (c *chunksIterator) Err() error {
	return c.err
}

var _ prom_storage.ChunkSeries = &concreteChunksSeries{}

type concreteChunksSeries struct {
	lbls labels.Labels
	chks []chunks.Meta
}

func (c concreteChunksSeries) Labels() labels.Labels {
	return c.lbls
}

func (c concreteChunksSeries) Iterator(_ chunks.Iterator) chunks.Iterator {
	return prom_storage.NewListChunkSeriesIterator(c.chks...)
}

type iteratorChunksSeries struct {
	lbls   labels.Labels
	chunks chunks.Iterator
}

func (i *iteratorChunksSeries) Labels() labels.Labels {
	return i.lbls
}

func (i *iteratorChunksSeries) Iterator(_ chunks.Iterator) chunks.Iterator {
	return i.chunks
}

// filterEmptyChunkSeriesSet is a ChunkSeriesSet that lazily filters out series with no chunks.
type filterEmptyChunkSeriesSet struct {
	lblsSet []labels.Labels
	chnkSet chunkIterableSet

	currentSeries *iteratorChunksSeries
	err           error
}

func newFilterEmptyChunkSeriesSet(lblsSet []labels.Labels, chnkSet chunkIterableSet) *filterEmptyChunkSeriesSet {
	return &filterEmptyChunkSeriesSet{
		lblsSet: lblsSet,
		chnkSet: chnkSet,
	}
}

func (f *filterEmptyChunkSeriesSet) Next() bool {
	for f.chnkSet.Next() {
		if len(f.lblsSet) == 0 {
			f.err = errors.New("less labels than chunks, this should not happen")
			return false
		}
		lbls := f.lblsSet[0]
		f.lblsSet = f.lblsSet[1:]
		iter := f.chnkSet.At()
		if iter.Next() {
			// The series has chunks, keep it
			meta := iter.At()
			f.currentSeries = &iteratorChunksSeries{
				lbls: lbls,
				chunks: &peekedChunksIterator{
					inner:       iter,
					peekedValue: &meta,
				},
			}
			return true
		}

		if iter.Err() != nil {
			f.err = iter.Err()
			return false
		}
		// This series has no chunks, skip it and continue to the next
	}
	if f.chnkSet.Err() != nil {
		f.err = f.chnkSet.Err()
	}
	if len(f.lblsSet) > 0 {
		f.err = errors.New("more labels than chunks, this should not happen")
	}
	return false
}

func (f *filterEmptyChunkSeriesSet) At() prom_storage.ChunkSeries {
	return f.currentSeries
}

func (f *filterEmptyChunkSeriesSet) Err() error {
	if f.err != nil {
		return f.err
	}
	return f.chnkSet.Err()
}

func (f *filterEmptyChunkSeriesSet) Warnings() annotations.Annotations {
	return nil
}

// peekedChunksIterator wraps a chunks.Iterator with an extra value to return on
// the first iteration. It is useful to allow peeking the first chunk without
// consuming it.
type peekedChunksIterator struct {
	inner       chunks.Iterator
	peekedValue *chunks.Meta
	nextCalled  bool
}

func (p *peekedChunksIterator) Next() bool {
	if !p.nextCalled {
		p.nextCalled = true
		return true
	}
	if p.peekedValue != nil {
		// This is the second call to Next, discard the peeked value
		p.peekedValue = nil
	}
	return p.inner.Next()
}

func (p *peekedChunksIterator) At() chunks.Meta {
	if p.peekedValue != nil {
		return *p.peekedValue
	}
	return p.inner.At()
}

func (p *peekedChunksIterator) Err() error {
	return p.inner.Err()
}
