package search

import (
	"io"

	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	prom_storage "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

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

func (c concreteChunksSeries) ChunkCount() (int, error) {
	return len(c.chks), nil
}

// pagesValuesIterator yields a slice of parquet Values from each parquet Page in its FilePages.
type pagesValuesIterator struct {
	pgs          *parquet.FilePages
	pageIterator *pageValueIterator

	remainingRr []RowRange
	currentRr   RowRange
	next        int64
	remaining   int64
	currentRow  int64

	currentPageValues []parquet.Value
	err               error
}

func (c *pagesValuesIterator) At() (RowRange, []parquet.Value) {
	return c.currentRr, c.currentPageValues
}

func (c *pagesValuesIterator) Next() bool {
	if c.err != nil {
		return false
	}

	if len(c.remainingRr) == 0 && c.remaining == 0 {
		return false
	}

	// prepare inner iterator
	page, err := c.pgs.ReadPage()
	if err != nil {
		c.err = errors.Wrap(err, "could not read page")
		return false
	}
	c.pageIterator.Reset(page)

	c.currentPageValues = []parquet.Value{}
	for c.pageIterator.Next() {
		if c.currentRow == c.next {
			c.currentPageValues = append(c.currentPageValues, c.pageIterator.At())
			c.remaining--
			if c.remaining > 0 {
				c.next = c.next + 1
			} else if len(c.remainingRr) > 0 {
				c.currentRr = c.remainingRr[0]
				c.next = c.currentRr.from
				c.remaining = c.currentRr.count
				c.remainingRr = c.remainingRr[1:]
			}
		}
		c.currentRow++
	}
	parquet.Release(page)
	if c.pageIterator.Err() != nil {
		c.err = errors.Wrap(c.pageIterator.Err(), "could not read page values")
		return false
	}

	return true
}

func (c *pagesValuesIterator) Err() error {
	return c.err
}

// pageValueIterator yields individual parquet Values from its Page.
type pageValueIterator struct {
	p parquet.Page

	// TODO: consider using unique.Handle
	cachedSymbols map[int32]parquet.Value
	st            symbolTable

	vr parquet.ValueReader

	current            int
	buffer             []parquet.Value
	currentBufferIndex int
	err                error
}

func (vi *pageValueIterator) At() parquet.Value {
	if vi.vr == nil {
		dicIndex := vi.st.GetIndex(vi.current)
		// Cache a clone of the current symbol table entry.
		// This allows us to release the original page while avoiding unnecessary future clones.
		if _, ok := vi.cachedSymbols[dicIndex]; !ok {
			vi.cachedSymbols[dicIndex] = vi.st.Get(vi.current).Clone()
		}
		return vi.cachedSymbols[dicIndex]
	}

	return vi.buffer[vi.currentBufferIndex].Clone()
}

func (vi *pageValueIterator) Next() bool {
	if vi.err != nil {
		return false
	}

	vi.current++
	if vi.current >= int(vi.p.NumRows()) {
		return false
	}

	vi.currentBufferIndex++

	if vi.currentBufferIndex == len(vi.buffer) {
		n, err := vi.vr.ReadValues(vi.buffer[:cap(vi.buffer)])
		if err != nil && err != io.EOF {
			vi.err = err
		}
		vi.buffer = vi.buffer[:n]
		vi.currentBufferIndex = 0
	}

	return true
}

func (vi *pageValueIterator) Reset(p parquet.Page) {
	vi.p = p
	vi.vr = nil
	if p.Dictionary() != nil {
		vi.st.Reset(p)
		vi.cachedSymbols = make(map[int32]parquet.Value, p.Dictionary().Len())
	} else {
		vi.vr = p.Values()
		vi.buffer = make([]parquet.Value, 0, 128)
		vi.currentBufferIndex = -1
	}
	vi.current = -1
}

func (vi *pageValueIterator) Err() error {
	return vi.err
}
