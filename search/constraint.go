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
	"bytes"
	"context"
	"fmt"
	"slices"
	"sort"
	"sync"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/sync/errgroup"

	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/storage"
	"github.com/prometheus-community/parquet-common/util"
)

type Constraint interface {
	fmt.Stringer

	// init initializes the constraint with respect to the file schema and projections.
	init(f storage.ParquetFileView) error

	// path is the path for the column that is constrained
	path() string

	// prefilter returns a set of non-overlapping increasing row indexes that may satisfy the constraint.
	// This MUST be a superset of the real set of matching rows.
	prefilter(rgi int, rr []RowRange) ([]RowRange, error)

	// filter returns a set of non-overlapping increasing row indexes that do satisfy the constraint.
	// This MUST be the precise set of matching rows.
	filter(ctx context.Context, rgi int, primary bool, rr []RowRange) ([]RowRange, error)
}

// MatchersToConstraints converts Prometheus label matchers into parquet search constraints.
// It supports MatchEqual, MatchNotEqual, MatchRegexp, and MatchNotRegexp matcher types.
// Returns a slice of constraints that can be used to filter parquet data based on the
// provided label matchers, or an error if an unsupported matcher type is encountered.
func MatchersToConstraints(matchers ...*labels.Matcher) ([]Constraint, error) {
	r := make([]Constraint, 0, len(matchers))
	for _, matcher := range matchers {
		var c Constraint
	S:
		switch matcher.Type {
		case labels.MatchEqual:
			c = Equal(schema.LabelToColumn(matcher.Name), parquet.ValueOf(matcher.Value))
		case labels.MatchNotEqual:
			c = Not(Equal(schema.LabelToColumn(matcher.Name), parquet.ValueOf(matcher.Value)))
		case labels.MatchRegexp:
			if matcher.GetRegexString() == ".*" {
				continue
			}
			if matcher.GetRegexString() == ".+" {
				c = Not(Equal(schema.LabelToColumn(matcher.Name), parquet.ValueOf("")))
				break S
			}
			if set := matcher.SetMatches(); len(set) == 1 {
				c = Equal(schema.LabelToColumn(matcher.Name), parquet.ValueOf(set[0]))
				break S
			}
			rc, err := Regex(schema.LabelToColumn(matcher.Name), matcher)
			if err != nil {
				return nil, fmt.Errorf("unable to construct regex matcher: %w", err)
			}
			c = rc
		case labels.MatchNotRegexp:
			inverted, err := matcher.Inverse()
			if err != nil {
				return nil, fmt.Errorf("unable to invert matcher: %w", err)
			}
			if set := inverted.SetMatches(); len(set) == 1 {
				c = Not(Equal(schema.LabelToColumn(matcher.Name), parquet.ValueOf(set[0])))
				break S
			}
			rc, err := Regex(schema.LabelToColumn(matcher.Name), inverted)
			if err != nil {
				return nil, fmt.Errorf("unable to construct regex matcher: %w", err)
			}
			c = Not(rc)
		default:
			return nil, fmt.Errorf("unsupported matcher type %s", matcher.Type)
		}
		r = append(r, c)
	}
	return r, nil
}

// Initialize prepares the given constraints for use with the specified parquet file.
// It calls the init method on each constraint to validate compatibility with the
// file schema and set up any necessary internal state.
//
// Parameters:
//   - f: The ParquetFile that the constraints will be applied to
//   - cs: Variable number of constraints to initialize
//
// Returns an error if any constraint fails to initialize, wrapping the original
// error with context about which constraint failed.
func Initialize(f storage.ParquetFileView, cs ...Constraint) error {
	for i := range cs {
		if err := cs[i].init(f); err != nil {
			return fmt.Errorf("unable to initialize constraint %d: %w", i, err)
		}
	}
	return nil
}

// sortConstraintsBySortingColumns reorders constraints to prioritize those that match sorting columns.
// Constraints matching sorting columns are moved to the front, ordered by the sorting column priority.
// Other constraints maintain their original relative order.
func sortConstraintsBySortingColumns(cs []Constraint, sc []parquet.SortingColumn) {
	if len(sc) == 0 {
		return // No sorting columns, nothing to do
	}

	sortingPaths := make(map[string]int, len(sc))
	for i, col := range sc {
		sortingPaths[col.Path()[0]] = i
	}

	// Sort constraints: sorting column constraints first (by their order in sc), then others
	slices.SortStableFunc(cs, func(a, b Constraint) int {
		aIdx, aIsSorting := sortingPaths[a.path()]
		bIdx, bIsSorting := sortingPaths[b.path()]

		if aIsSorting && bIsSorting {
			return aIdx - bIdx // Sort by sorting column order
		}
		if aIsSorting {
			return -1 // a comes first
		}
		if bIsSorting {
			return 1 // b comes first
		}
		return 0 // preserve original order for non-sorting constraints
	})
}

// Filter applies the given constraints to a parquet row group and returns the row ranges
// that satisfy all constraints. It optimizes performance by prioritizing constraints on
// sorting columns, which are cheaper to evaluate.
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - s: ParquetShard containing the parquet file to filter
//   - rgIdx: Index of the row group to filter within the parquet file
//   - cs: Variable number of constraints to apply for filtering
//
// Returns a slice of RowRange that represent the rows satisfying all constraints,
// or an error if any constraint fails to filter.
func Filter(ctx context.Context, f storage.ParquetShard, rgi int, cs ...Constraint) ([]RowRange, error) {
	rg := f.LabelsFile().RowGroups()[rgi]

	// Constraints for sorting columns are cheaper to evaluate, so we sort them first.
	sc := rg.SortingColumns()

	sortConstraintsBySortingColumns(cs, sc)

	var (
		err error
		mu  sync.Mutex
		g   errgroup.Group
	)

	// First pass prefilter with a quick index scan to find a superset of matching rows
	rr := []RowRange{{From: int64(0), Count: rg.NumRows()}}
	for i := range cs {
		rr, err = cs[i].prefilter(rgi, rr)
		if err != nil {
			return nil, fmt.Errorf("unable to prefilter with constraint %d: %w", i, err)
		}
	}
	res := slices.Clone(rr)

	if len(res) == 0 {
		return nil, nil
	}

	// Second pass page filter find the real set of matching rows, done concurrently because it involves IO
	for i := range cs {
		g.Go(func() error {
			isPrimary := len(sc) > 0 && cs[i].path() == sc[0].Path()[0]

			srr, err := cs[i].filter(ctx, rgi, isPrimary, rr)
			if err != nil {
				return fmt.Errorf("unable to filter with constraint %d: %w", i, err)
			}
			mu.Lock()
			res = intersectRowRanges(res, srr)
			mu.Unlock()

			return nil
		})
	}
	if err = g.Wait(); err != nil {
		return nil, fmt.Errorf("unable to do second pass filter: %w", err)
	}

	return res, nil
}

type PageToRead struct {
	idx int

	// for data pages
	pfrom int64
	pto   int64

	// for data and dictionary pages
	off int64
	csz int64 // compressed size
}

func NewPageToRead(idx int, pfrom, pto, off, csz int64) PageToRead {
	return PageToRead{
		idx:   idx,
		pfrom: pfrom,
		pto:   pto,
		off:   off,
		csz:   csz,
	}
}

func (p *PageToRead) From() int64 {
	return p.pfrom
}

func (p *PageToRead) To() int64 {
	return p.pto
}

func (p *PageToRead) Offset() int64 {
	return p.off
}

func (p *PageToRead) CompressedSize() int64 {
	return p.csz
}

// SymbolTable is a helper that can decode the i-th value of a page.
// Using it we only need to allocate an int32 slice and not a slice of
// string values.
// It only works for optional dictionary encoded columns. All of our label
// columns are that though.
type SymbolTable struct {
	dict parquet.Dictionary
	syms []int32
	defs []byte
}

func (s *SymbolTable) Get(r int) parquet.Value {
	i := s.GetIndex(r)
	switch i {
	case -1:
		return parquet.NullValue()
	default:
		return s.dict.Index(i)
	}
}

func (s *SymbolTable) GetIndex(i int) int32 {
	switch s.defs[i] {
	case 1:
		return s.syms[i]
	default:
		return -1
	}
}

func (s *SymbolTable) Reset(pg parquet.Page) {
	dict := pg.Dictionary()
	data := pg.Data()
	syms := data.Int32()
	s.defs = pg.DefinitionLevels()

	if s.syms == nil {
		s.syms = make([]int32, len(s.defs))
	} else {
		s.syms = slices.Grow(s.syms, len(s.defs))[:len(s.defs)]
	}

	sidx := 0
	for i := range s.defs {
		if s.defs[i] == 1 {
			s.syms[i] = syms[sidx]
			sidx++
		}
	}
	s.dict = dict
}

func (s *SymbolTable) ResetWithRange(pg parquet.Page, l, r int) {
	dict := pg.Dictionary()
	data := pg.Data()
	syms := data.Int32()
	s.defs = pg.DefinitionLevels()

	if s.syms == nil {
		s.syms = make([]int32, len(s.defs))
	} else {
		s.syms = slices.Grow(s.syms, len(s.defs))[:len(s.defs)]
	}

	sidx := 0
	for i := range l {
		if s.defs[i] == 1 {
			sidx++
		}
	}
	for i := l; i < r; i++ {
		if s.defs[i] == 1 {
			s.syms[i] = syms[sidx]
			sidx++
		}
	}
	s.dict = dict
}

type equalConstraint struct {
	pth string

	f storage.ParquetFileView

	val parquet.Value

	comp func(l, r parquet.Value) int
}

func (ec *equalConstraint) String() string {
	return fmt.Sprintf("equal(%q,%q)", ec.pth, ec.val)
}

func Equal(path string, value parquet.Value) Constraint {
	return &equalConstraint{pth: path, val: value}
}

func (ec *equalConstraint) prefilter(rgi int, rr []RowRange) ([]RowRange, error) {
	if len(rr) == 0 {
		return nil, nil
	}
	rg := ec.f.RowGroups()[rgi]

	from, to := rr[0].From, rr[len(rr)-1].From+rr[len(rr)-1].Count

	col, ok := rg.Schema().Lookup(ec.path())
	if !ok {
		// If match empty, return rr (filter nothing)
		// otherwise return empty
		if ec.matches(parquet.ValueOf("")) {
			return slices.Clone(rr), nil
		}
		return []RowRange{}, nil
	}
	cci := col.ColumnIndex

	cc := rg.ColumnChunks()[cci].(*parquet.FileColumnChunk)

	if skip, err := ec.skipByBloomfilter(cc); err != nil {
		return nil, fmt.Errorf("unable to skip by bloomfilter: %w", err)
	} else if skip {
		return nil, nil
	}

	oidx, err := cc.OffsetIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read offset index: %w", err)
	}
	cidx, err := cc.ColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read column index: %w", err)
	}
	res := make([]RowRange, 0)
	for i := range cidx.NumPages() {
		// If page does not intersect from, to; we can immediately discard it
		pfrom := oidx.FirstRowIndex(i)
		pcount := rg.NumRows() - pfrom
		if i < oidx.NumPages()-1 {
			pcount = oidx.FirstRowIndex(i+1) - pfrom
		}
		pto := pfrom + pcount
		if pfrom > to {
			break
		}
		if pto < from {
			continue
		}
		// Page intersects [from, to] but we might be able to discard it with statistics
		if cidx.NullPage(i) {
			if ec.matches(parquet.ValueOf("")) {
				res = append(res, RowRange{pfrom, pcount})
			}
			continue
		}

		// If we are not matching the empty string ( which would be satisfied by Null too ), we can
		// use page statistics to skip rows
		minv, maxv := cidx.MinValue(i), cidx.MaxValue(i)
		if !ec.matches(parquet.ValueOf("")) && !maxv.IsNull() && ec.comp(ec.val, maxv) > 0 {
			if cidx.IsDescending() {
				break
			}
			continue
		}
		if !ec.matches(parquet.ValueOf("")) && !minv.IsNull() && ec.comp(ec.val, minv) < 0 {
			if cidx.IsAscending() {
				break
			}
			continue
		}
		// We cannot discard the page through statistics but we might need to read it to see if it has the value
		res = append(res, RowRange{From: pfrom, Count: pto - pfrom})
	}
	if len(res) == 0 {
		return nil, nil
	}
	return intersectRowRanges(simplify(res), rr), nil
}

func (ec *equalConstraint) filter(ctx context.Context, rgi int, primary bool, rr []RowRange) ([]RowRange, error) {
	if len(rr) == 0 {
		return nil, nil
	}
	rg := ec.f.RowGroups()[rgi]

	from, to := rr[0].From, rr[len(rr)-1].From+rr[len(rr)-1].Count

	col, ok := rg.Schema().Lookup(ec.path())
	if !ok {
		// If match empty, return rr (filter nothing)
		// otherwise return empty
		if ec.matches(parquet.ValueOf("")) {
			return slices.Clone(rr), nil
		}
		return []RowRange{}, nil
	}
	cci := col.ColumnIndex

	cc := rg.ColumnChunks()[cci].(*parquet.FileColumnChunk)
	if skip, err := ec.skipByBloomfilter(cc); err != nil {
		return nil, fmt.Errorf("unable to skip by bloomfilter: %w", err)
	} else if skip {
		return nil, nil
	}

	oidx, err := cc.OffsetIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read offset index: %w", err)
	}
	cidx, err := cc.ColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read column index: %w", err)
	}
	var (
		res     = make([]RowRange, 0)
		readPgs = make([]PageToRead, 0)
	)
	for i := range cidx.NumPages() {
		// If page does not intersect from, to; we can immediately discard it
		pfrom := oidx.FirstRowIndex(i)
		pcount := rg.NumRows() - pfrom
		if i < oidx.NumPages()-1 {
			pcount = oidx.FirstRowIndex(i+1) - pfrom
		}
		pto := pfrom + pcount
		if pfrom > to {
			break
		}
		if pto < from {
			continue
		}
		// Page intersects [from, to] but we might be able to discard it with statistics
		if cidx.NullPage(i) {
			if ec.matches(parquet.ValueOf("")) {
				res = append(res, RowRange{pfrom, pcount})
			}
			continue
		}

		// If we are not matching the empty string ( which would be satisfied by Null too ), we can
		// use page statistics to skip rows
		minv, maxv := cidx.MinValue(i), cidx.MaxValue(i)
		if !ec.matches(parquet.ValueOf("")) && !maxv.IsNull() && ec.comp(ec.val, maxv) > 0 {
			if cidx.IsDescending() {
				break
			}
			continue
		}
		if !ec.matches(parquet.ValueOf("")) && !minv.IsNull() && ec.comp(ec.val, minv) < 0 {
			if cidx.IsAscending() {
				break
			}
			continue
		}
		// We cannot discard the page through statistics but we might need to read it to see if it has the value
		readPgs = append(readPgs, PageToRead{idx: i, pfrom: pfrom, pto: pto})
	}

	if len(readPgs) == 0 {
		return intersectRowRanges(simplify(res), rr), nil
	}

	dictOffset, dictSize := ec.f.DictionaryPageBounds(rgi, col.ColumnIndex)
	minOffset := uint64(oidx.Offset(readPgs[0].idx))
	maxOffset := uint64(oidx.Offset(readPgs[len(readPgs)-1].idx)) + uint64(oidx.CompressedPageSize(readPgs[len(readPgs)-1].idx))

	// If the gap between the first page and the dic page is less than PagePartitioningMaxGapSize,
	// we include the dic to be read in the single read
	if int(minOffset-(dictOffset+dictSize)) < ec.f.PagePartitioningMaxGapSize() {
		minOffset = dictOffset
	}

	pgs, err := ec.f.GetPages(ctx, cc, int64(minOffset), int64(maxOffset))
	if err != nil {
		return nil, err
	}
	defer func() { _ = pgs.Close() }()

	symbols := new(SymbolTable)
	for _, p := range readPgs {
		pfrom := p.pfrom
		pto := p.pto

		if err := pgs.SeekToRow(pfrom); err != nil {
			return nil, fmt.Errorf("unable to seek to row: %w", err)
		}
		pg, err := pgs.ReadPage()
		if err != nil {
			return nil, fmt.Errorf("unable to read page: %w", err)
		}
		symbols.Reset(pg)

		// The page has the value, we need to find the matching row ranges
		n := int(pg.NumRows())
		bl := int(max(pfrom, from) - pfrom)
		br := n - int(pto-min(pto, to))
		var l, r int
		switch {
		case cidx.IsAscending() && primary:
			l = sort.Search(n, func(i int) bool { return ec.comp(ec.val, symbols.Get(i)) <= 0 })
			r = sort.Search(n, func(i int) bool { return ec.comp(ec.val, symbols.Get(i)) < 0 })

			if lv, rv := max(bl, l), min(br, r); rv > lv {
				res = append(res, RowRange{pfrom + int64(lv), int64(rv - lv)})
			}
		default:
			off, count := bl, 0
			for j := bl; j < br; j++ {
				if !ec.matches(symbols.Get(j)) {
					if count != 0 {
						res = append(res, RowRange{pfrom + int64(off), int64(count)})
					}
					off, count = j, 0
				} else {
					if count == 0 {
						off = j
					}
					count++
				}
			}
			if count != 0 {
				res = append(res, RowRange{pfrom + int64(off), int64(count)})
			}
			parquet.Release(pg)
		}
	}

	if len(res) == 0 {
		return nil, nil
	}
	return intersectRowRanges(simplify(res), rr), nil
}

func (ec *equalConstraint) init(f storage.ParquetFileView) error {
	ec.f = f

	c, ok := f.Schema().Lookup(ec.path())
	if !ok {
		return nil
	}
	stringKind := parquet.String().Type().Kind()
	if ec.val.Kind() != stringKind {
		return fmt.Errorf("schema: can only search string kind, got: %s", ec.val.Kind())
	}
	if c.Node.Type().Kind() != stringKind {
		return fmt.Errorf("schema: cannot search value of kind %s in column of kind %s", stringKind, c.Node.Type().Kind())
	}
	ec.comp = c.Node.Type().Compare
	return nil
}

func (ec *equalConstraint) path() string {
	return ec.pth
}

func (ec *equalConstraint) matches(v parquet.Value) bool {
	return bytes.Equal(v.ByteArray(), ec.val.ByteArray())
}

func (ec *equalConstraint) skipByBloomfilter(cc parquet.ColumnChunk) (bool, error) {
	if ec.f.SkipBloomFilters() {
		return false, nil
	}

	bf := cc.BloomFilter()
	if bf == nil {
		return false, nil
	}
	ok, err := bf.Check(ec.val)
	if err != nil {
		return false, fmt.Errorf("unable to check bloomfilter: %w", err)
	}
	return !ok, nil
}

type regexConstraint struct {
	pth   string
	cache map[parquet.Value]bool

	f storage.ParquetFileView

	// if its a "set" or "prefix" regex
	// for set, those are minv and maxv of the set, for prefix minv is the prefix, maxv is prefix+max(charset)*16
	minv parquet.Value
	maxv parquet.Value

	r *labels.Matcher

	comp func(l, r parquet.Value) int
}

// r MUST be a matcher of type Regex
func Regex(path string, r *labels.Matcher) (Constraint, error) {
	if r.Type != labels.MatchRegexp {
		return nil, fmt.Errorf("unsupported matcher type: %s", r.Type)
	}
	return &regexConstraint{pth: path, cache: make(map[parquet.Value]bool), r: r}, nil
}

func (rc *regexConstraint) String() string {
	return fmt.Sprintf("regex(%v,%v)", rc.pth, rc.r.GetRegexString())
}

func (rc *regexConstraint) prefilter(rgi int, rr []RowRange) ([]RowRange, error) {
	if len(rr) == 0 {
		return nil, nil
	}
	rg := rc.f.RowGroups()[rgi]

	from, to := rr[0].From, rr[len(rr)-1].From+rr[len(rr)-1].Count

	col, ok := rg.Schema().Lookup(rc.path())
	if !ok {
		// If match empty, return rr (filter nothing)
		// otherwise return empty
		if rc.matches(parquet.ValueOf("")) {
			return slices.Clone(rr), nil
		}
		return []RowRange{}, nil
	}
	cc := rg.ColumnChunks()[col.ColumnIndex].(*parquet.FileColumnChunk)

	oidx, err := cc.OffsetIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read offset index: %w", err)
	}
	cidx, err := cc.ColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read column index: %w", err)
	}
	res := make([]RowRange, 0)
	for i := range cidx.NumPages() {
		// If page does not intersect from, to; we can immediately discard it
		pfrom := oidx.FirstRowIndex(i)
		pcount := rg.NumRows() - pfrom
		if i < oidx.NumPages()-1 {
			pcount = oidx.FirstRowIndex(i+1) - pfrom
		}
		pto := pfrom + pcount
		if pfrom > to {
			break
		}
		if pto < from {
			continue
		}
		// Page intersects [from, to] but we might be able to discard it with statistics
		if cidx.NullPage(i) {
			if rc.matches(parquet.ValueOf("")) {
				res = append(res, RowRange{pfrom, pcount})
			}
			continue
		}
		// If we have a special regular expression that works with statistics, we can use them to skip.
		// This works for i.e.: 'pod_name=~"thanos-.*"' or 'status_code=~"403|404"'
		minv, maxv := cidx.MinValue(i), cidx.MaxValue(i)
		if !rc.minv.IsNull() && !rc.maxv.IsNull() {
			if !rc.matches(parquet.ValueOf("")) && !maxv.IsNull() && rc.comp(rc.minv, maxv) > 0 {
				if cidx.IsDescending() {
					break
				}
				continue
			}
			if !rc.matches(parquet.ValueOf("")) && !minv.IsNull() && rc.comp(rc.maxv, minv) < 0 {
				if cidx.IsAscending() {
					break
				}
				continue
			}
		}
		res = append(res, RowRange{From: pfrom, Count: pto - pfrom})
	}
	if len(res) == 0 {
		return nil, nil
	}
	return intersectRowRanges(simplify(res), rr), nil
}

func (rc *regexConstraint) filter(ctx context.Context, rgi int, isPrimary bool, rr []RowRange) ([]RowRange, error) {
	if len(rr) == 0 {
		return nil, nil
	}
	rg := rc.f.RowGroups()[rgi]

	from, to := rr[0].From, rr[len(rr)-1].From+rr[len(rr)-1].Count

	col, ok := rg.Schema().Lookup(rc.path())
	if !ok {
		// If match empty, return rr (filter nothing)
		// otherwise return empty
		if rc.matches(parquet.ValueOf("")) {
			return slices.Clone(rr), nil
		}
		return []RowRange{}, nil
	}
	cc := rg.ColumnChunks()[col.ColumnIndex].(*parquet.FileColumnChunk)

	oidx, err := cc.OffsetIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read offset index: %w", err)
	}
	cidx, err := cc.ColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read column index: %w", err)
	}
	var (
		res     = make([]RowRange, 0)
		readPgs = make([]PageToRead, 0)
	)
	for i := range cidx.NumPages() {
		// If page does not intersect from, to; we can immediately discard it
		pfrom := oidx.FirstRowIndex(i)
		pcount := rg.NumRows() - pfrom
		if i < oidx.NumPages()-1 {
			pcount = oidx.FirstRowIndex(i+1) - pfrom
		}
		pto := pfrom + pcount
		if pfrom > to {
			break
		}
		if pto < from {
			continue
		}
		// Page intersects [from, to] but we might be able to discard it with statistics
		if cidx.NullPage(i) {
			if rc.matches(parquet.ValueOf("")) {
				res = append(res, RowRange{pfrom, pcount})
			}
			continue
		}
		// If we have a special regular expression that works with statistics, we can use them to skip.
		// This works for i.e.: 'pod_name=~"thanos-.*"' or 'status_code=~"403|404"'
		minv, maxv := cidx.MinValue(i), cidx.MaxValue(i)
		if !rc.minv.IsNull() && !rc.maxv.IsNull() {
			if !rc.matches(parquet.ValueOf("")) && !maxv.IsNull() && rc.comp(rc.minv, maxv) > 0 {
				if cidx.IsDescending() {
					break
				}
				continue
			}
			if !rc.matches(parquet.ValueOf("")) && !minv.IsNull() && rc.comp(rc.maxv, minv) < 0 {
				if cidx.IsAscending() {
					break
				}
				continue
			}
		}
		readPgs = append(readPgs, PageToRead{pfrom: pfrom, pto: pto, idx: i})
	}
	if len(readPgs) == 0 {
		return intersectRowRanges(simplify(res), rr), nil
	}

	dictOffset, dictSize := rc.f.DictionaryPageBounds(rgi, col.ColumnIndex)
	minOffset := uint64(oidx.Offset(readPgs[0].idx))
	maxOffset := uint64(oidx.Offset(readPgs[len(readPgs)-1].idx)) + uint64(oidx.CompressedPageSize(readPgs[len(readPgs)-1].idx))

	// If the gap between the first page and the dic page is less than PagePartitioningMaxGapSize,
	// we include the dic to be read in the single read
	if int(minOffset-(dictOffset+dictSize)) < rc.f.PagePartitioningMaxGapSize() {
		minOffset = dictOffset
	}

	pgs, err := rc.f.GetPages(ctx, cc, int64(minOffset), int64(maxOffset))
	if err != nil {
		return nil, err
	}
	defer func() { _ = pgs.Close() }()

	symbols := new(SymbolTable)
	for _, p := range readPgs {
		pfrom := p.pfrom
		pto := p.pto

		if err := pgs.SeekToRow(pfrom); err != nil {
			return nil, fmt.Errorf("unable to seek to row: %w", err)
		}
		pg, err := pgs.ReadPage()
		if err != nil {
			return nil, fmt.Errorf("unable to read page: %w", err)
		}
		symbols.Reset(pg)

		// The page has the value, we need to find the matching row ranges
		n := int(pg.NumRows())
		bl := int(max(pfrom, from) - pfrom)
		br := n - int(pto-min(pto, to))
		off, count := bl, 0
		for j := bl; j < br; j++ {
			if !rc.matches(symbols.Get(j)) {
				if count != 0 {
					res = append(res, RowRange{pfrom + int64(off), int64(count)})
				}
				off, count = j, 0
			} else {
				if count == 0 {
					off = j
				}
				count++
			}
		}
		if count != 0 {
			res = append(res, RowRange{pfrom + int64(off), int64(count)})
		}
		parquet.Release(pg)
	}

	if len(res) == 0 {
		return nil, nil
	}
	return intersectRowRanges(simplify(res), rr), nil
}

func (rc *regexConstraint) init(f storage.ParquetFileView) error {
	rc.f = f

	c, ok := f.Schema().Lookup(rc.path())
	if !ok {
		return nil
	}
	if stringKind := parquet.String().Type().Kind(); c.Node.Type().Kind() != stringKind {
		return fmt.Errorf("schema: cannot search value of kind %s in column of kind %s", stringKind, c.Node.Type().Kind())
	}
	rc.cache = make(map[parquet.Value]bool)
	rc.comp = c.Node.Type().Compare

	// if applicable compute the minv and maxv of the implied set of matches
	rc.minv = parquet.NullValue()
	rc.maxv = parquet.NullValue()
	if len(rc.r.SetMatches()) > 0 {
		sm := make([]parquet.Value, len(rc.r.SetMatches()))
		for i, m := range rc.r.SetMatches() {
			sm[i] = parquet.ValueOf(m)
		}
		rc.minv = slices.MinFunc(sm, rc.comp)
		rc.maxv = slices.MaxFunc(sm, rc.comp)
	} else if len(rc.r.Prefix()) > 0 {
		rc.minv = parquet.ValueOf(rc.r.Prefix())
		rc.maxv = parquet.ValueOf(append([]byte(rc.r.Prefix()), bytes.Repeat([]byte{0xff}, 16)...))
	}

	return nil
}

func (rc *regexConstraint) path() string {
	return rc.pth
}

func (rc *regexConstraint) matches(v parquet.Value) bool {
	accept, seen := rc.cache[v]
	if !seen {
		accept = rc.r.Matches(util.YoloString(v.ByteArray()))
		rc.cache[v] = accept
	}
	return accept
}

func Not(c Constraint) Constraint {
	return &notConstraint{c: c}
}

func (nc *notConstraint) String() string {
	return fmt.Sprintf("not(%v)", nc.c.String())
}

type notConstraint struct {
	c Constraint
}

func (nc *notConstraint) prefilter(_ int, rr []RowRange) ([]RowRange, error) {
	// NOT constraints cannot be prefiltered since the child constraint returns a superset of the matching row range,
	// if we were to complement this row range the result here would be a subset and this would violate our interface.
	return slices.Clone(rr), nil
}

func (nc *notConstraint) filter(ctx context.Context, rgi int, isPrimary bool, rr []RowRange) ([]RowRange, error) {
	base, err := nc.c.filter(ctx, rgi, isPrimary, rr)
	if err != nil {
		return nil, fmt.Errorf("unable to filter child constraint: %w", err)
	}
	// no need to intersect since its already subset of rr
	return complementRowRanges(base, rr), nil
}

func (nc *notConstraint) init(f storage.ParquetFileView) error {
	return nc.c.init(f)
}

func (nc *notConstraint) path() string {
	return nc.c.path()
}
