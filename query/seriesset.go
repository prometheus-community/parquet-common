package query

import (
	"context"
	"sync"

	"github.com/hashicorp/go-multierror"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

type hints struct {
	By       bool
	Func     string
	Grouping []string
}

func fromStorageHints(h *storage.SelectHints) hints {
	clone := make([]string, len(h.Grouping))
	copy(clone, h.Grouping)
	return hints{
		Func:     h.Func,
		By:       h.By,
		Grouping: clone,
	}
}

func toStorageHints(h hints) *storage.SelectHints {
	return &storage.SelectHints{Func: h.Func, By: h.By, Grouping: h.Grouping}
}

type selectFn func(context.Context, bool, *storage.SelectHints, ...*labels.Matcher) storage.SeriesSet

func newLazySeriesSet(ctx context.Context, selectFn selectFn, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	res := &lazySeriesSet{
		selectfn: selectFn,
		ctx:      ctx,
		sorted:   sorted,
		// SelectHints is reused in the subsequent parallel Select call
		hints:    fromStorageHints(hints),
		matchers: matchers,
		done:     make(chan struct{}),
	}
	go res.init()

	return res
}

type lazySeriesSet struct {
	selectfn selectFn
	ctx      context.Context
	sorted   bool
	hints    hints
	matchers []*labels.Matcher

	set storage.SeriesSet

	once sync.Once
	done chan struct{}
}

func (c *lazySeriesSet) init() {
	c.once.Do(func() {
		c.set = c.selectfn(c.ctx, c.sorted, toStorageHints(c.hints), c.matchers...)
		close(c.done)
	})
}

func (c *lazySeriesSet) Next() bool {
	<-c.done
	return c.set.Next()
}

func (c *lazySeriesSet) Err() error {
	<-c.done
	return c.set.Err()
}

func (c *lazySeriesSet) At() storage.Series {
	<-c.done
	return c.set.At()
}

func (c *lazySeriesSet) Warnings() annotations.Annotations {
	<-c.done
	return c.set.Warnings()
}

type verticalSeriesSet struct {
	i    int
	sets []storage.SeriesSet
}

func newVerticalSeriesSet(sets ...storage.SeriesSet) storage.SeriesSet {
	if len(sets) == 0 {
		return storage.EmptySeriesSet()
	}
	return &verticalSeriesSet{sets: sets, i: 0}
}

func (ss *verticalSeriesSet) Next() bool {
	if ss.sets[ss.i].Next() {
		return true
	}
	for ss.i < len(ss.sets)-1 {
		ss.i++
		if ss.sets[ss.i].Next() {
			return true
		}
	}
	return false
}

func (ss *verticalSeriesSet) At() storage.Series { return ss.sets[ss.i].At() }
func (ss *verticalSeriesSet) Err() error {
	var err *multierror.Error
	for i := range ss.sets {
		err = multierror.Append(err, ss.sets[i].Err())
	}
	return err.ErrorOrNil()
}

func (ss *verticalSeriesSet) Warnings() annotations.Annotations {
	res := annotations.New()
	for i := range ss.sets {
		res.Merge(ss.sets[i].Warnings())
	}
	return *res
}

type concatSeriesSet struct {
	i      int
	series []storage.Series
}

func newConcatSeriesSet(series ...storage.Series) storage.SeriesSet {
	if len(series) == 0 {
		return storage.EmptySeriesSet()
	}
	return &concatSeriesSet{series: series, i: -1}
}

func (ss *concatSeriesSet) Next() bool {
	if ss.i < len(ss.series)-1 {
		ss.i++
		return true
	}
	return false
}

func (ss *concatSeriesSet) At() storage.Series                { return ss.series[ss.i] }
func (ss *concatSeriesSet) Err() error                        { return nil }
func (ss *concatSeriesSet) Warnings() annotations.Annotations { return nil }
