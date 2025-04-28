package query

import (
	"errors"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// Taken from https://github.com/thanos-io/thanos/blob/main/pkg/query/iter.go
type chunkSeries struct {
	lset       labels.Labels
	chunks     []chunkenc.Chunk
	mint, maxt int64
}

func (s *chunkSeries) Labels() labels.Labels {
	return s.lset
}

func (s *chunkSeries) Iterator(_ chunkenc.Iterator) chunkenc.Iterator {
	its := make([]chunkenc.Iterator, 0, len(s.chunks))
	for _, chk := range s.chunks {
		its = append(its, chk.Iterator(nil))
	}
	// We might have collect series where we trimmed all chunks because they had
	// no timeseries in the interval
	if len(its) == 0 {
		return chunkenc.NewNopIterator()
	}
	return newBoundedSeriesIterator(newChunkSeriesIterator(its), s.mint, s.maxt)
}

type errSeriesIterator struct {
	err error
}

func (errSeriesIterator) Seek(int64) chunkenc.ValueType { return chunkenc.ValNone }
func (errSeriesIterator) Next() chunkenc.ValueType      { return chunkenc.ValNone }
func (errSeriesIterator) At() (int64, float64)          { return 0, 0 }
func (errSeriesIterator) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	return 0, nil
}
func (errSeriesIterator) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return 0, nil
}
func (errSeriesIterator) AtT() int64    { return 0 }
func (it errSeriesIterator) Err() error { return it.err }

type chunkSeriesIterator struct {
	chunks  []chunkenc.Iterator
	i       int
	lastVal chunkenc.ValueType

	cur chunkenc.Iterator
}

func newChunkSeriesIterator(cs []chunkenc.Iterator) chunkenc.Iterator {
	if len(cs) == 0 {
		return errSeriesIterator{err: errors.New("got empty chunks")}
	}
	return &chunkSeriesIterator{chunks: cs, cur: cs[0]}
}

func (it *chunkSeriesIterator) Seek(t int64) chunkenc.ValueType {
	// We generally expect the chunks already to be cut down
	// to the range we are interested in. There's not much to be gained from
	// hopping across chunks so we just call next until we reach t.
	for {
		ct := it.AtT()
		if ct >= t {
			return it.lastVal
		}
		it.lastVal = it.Next()
		if it.lastVal == chunkenc.ValNone {
			return chunkenc.ValNone
		}
	}
}

func (it *chunkSeriesIterator) At() (t int64, v float64) {
	return it.cur.At()
}

func (it *chunkSeriesIterator) AtHistogram(h *histogram.Histogram) (int64, *histogram.Histogram) {
	return it.cur.AtHistogram(h)
}

func (it *chunkSeriesIterator) AtFloatHistogram(fh *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return it.cur.AtFloatHistogram(fh)
}

func (it *chunkSeriesIterator) AtT() int64 {
	return it.cur.AtT()
}

func (it *chunkSeriesIterator) Next() chunkenc.ValueType {
	lastT := it.AtT()

	if valueType := it.chunks[it.i].Next(); valueType != chunkenc.ValNone {
		it.lastVal = valueType
		return valueType
	}
	if it.Err() != nil {
		return chunkenc.ValNone
	}
	if it.i >= len(it.chunks)-1 {
		return chunkenc.ValNone
	}
	// Chunks are guaranteed to be ordered but not generally guaranteed to not overlap.
	// We must ensure to skip any overlapping range between adjacent chunks.
	it.i++
	it.cur = it.chunks[it.i]
	return it.Seek(lastT + 1)
}

func (it *chunkSeriesIterator) Err() error {
	return it.chunks[it.i].Err()
}

// Taken from https://github.com/thanos-io/thanos/blob/main/pkg/dedup/iter.go
type boundedSeriesIterator struct {
	it         chunkenc.Iterator
	mint, maxt int64
}

func newBoundedSeriesIterator(it chunkenc.Iterator, mint, maxt int64) *boundedSeriesIterator {
	return &boundedSeriesIterator{it: it, mint: mint, maxt: maxt}
}

func (it *boundedSeriesIterator) Seek(t int64) chunkenc.ValueType {
	if t > it.maxt {
		return chunkenc.ValNone
	}
	if t < it.mint {
		t = it.mint
	}
	return it.it.Seek(t)
}

func (it *boundedSeriesIterator) At() (t int64, v float64) {
	return it.it.At()
}

func (it *boundedSeriesIterator) AtHistogram(h *histogram.Histogram) (int64, *histogram.Histogram) {
	return it.it.AtHistogram(h)
}

func (it *boundedSeriesIterator) AtFloatHistogram(fh *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return it.it.AtFloatHistogram(fh)
}

func (it *boundedSeriesIterator) AtT() int64 {
	return it.it.AtT()
}

func (it *boundedSeriesIterator) Next() chunkenc.ValueType {
	valueType := it.it.Next()
	if valueType == chunkenc.ValNone {
		return chunkenc.ValNone
	}
	t := it.it.AtT()

	// Advance the iterator if we are before the valid interval.
	if t < it.mint {
		if it.Seek(it.mint) == chunkenc.ValNone {
			return chunkenc.ValNone
		}
		t = it.it.AtT()
	}
	// Once we passed the valid interval, there is no going back.
	if t <= it.maxt {
		return valueType
	}

	return chunkenc.ValNone
}

func (it *boundedSeriesIterator) Err() error {
	return it.it.Err()
}
