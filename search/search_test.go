package search

import (
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestSearch(t *testing.T) {
	t.Run("", func(t *testing.T) {
		type S struct {
			A int64  `parquet:",optional,dict"`
			B int64  `parquet:",optional,dict"`
			C string `parquet:",optional,dict"`
		}
		srows := []S{
			{
				A: 1,
				B: 2,
				C: "a",
			},
			{
				A: 3,
				B: 4,
				C: "b",
			},
			{
				A: 7,
				B: 12,
				C: "c",
			},
			{
				A: 9,
				B: 22,
				C: "d",
			},
			{
				A: 0,
				B: 1,
				C: "e",
			},
			{
				A: 0,
				B: 1,
				C: "f",
			},
			{
				A: 0,
				B: 1,
				C: "g",
			},
			{
				A: 0,
				B: 1,
				C: "h",
			},
		}
		type T struct {
			D string `parquet:",optional,dict"`
		}

		trows := []T{
			{
				D: "h",
			},
			{
				D: "g",
			},
			{
				D: "f",
			},
			{
				D: "e",
			},
			{
				D: "d",
			},
			{
				D: "c",
			},
			{
				D: "b",
			},
			{
				D: "a",
			},
		}
		sfile := buildFile(t, srows)
		tfile := buildFile(t, trows)

		t.Run("", func(t *testing.T) {
			constraint := Equal("B", parquet.ValueOf(4))

			rr, err := Match(
				constraint,
				sfile, sfile.Schema(),
				tfile, tfile.Schema(),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer rr.Close()

			got := readAll(t, rr)
			expect := []parquet.Row{
				{parquet.ValueOf(3), parquet.ValueOf(4), parquet.ValueOf("b"), parquet.ValueOf("g")},
			}

			if !equalRows(got, expect) {
				t.Fatalf("expected %q to equal %q", got, expect)
			}
		})
	})
}

func equalRows(l, r []parquet.Row) bool {
	return slices.EqualFunc(l, r, func(ll, rr parquet.Row) bool {
		return equalRow(ll, rr)
	})
}

func equalRow(l, r parquet.Row) bool {
	return slices.EqualFunc(l, r, func(lv, rv parquet.Value) bool {
		return lv.String() == rv.String()
	})
}

func readAll(t *testing.T, rr parquet.RowReader) []parquet.Row {
	res := make([]parquet.Row, 0)

	rw := parquet.RowWriterFunc(func(rs []parquet.Row) (int, error) {
		res = slices.Grow(res, len(res))
		for _, r := range rs {
			res = append(res, r.Clone())
		}
		return len(res), nil
	})
	if _, err := parquet.CopyRows(rw, rr); err != nil {
		t.Fatal(err)
	}

	return res
}
