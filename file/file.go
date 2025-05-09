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

package file

import (
	"context"

	"github.com/thanos-io/objstore"

	"github.com/parquet-go/parquet-go"
)

type ParquetFile struct {
	*parquet.File
	ReadAtWithContext
}

func (f *ParquetFile) GetPage(ctx context.Context, cc parquet.ColumnChunk) *parquet.FilePages {
	colChunk := cc.(*parquet.FileColumnChunk)
	pages := colChunk.PagesFrom(f.WithContext(ctx))
	return pages
}

func OpenParquetFile(ctx context.Context, bkt objstore.Bucket, filename string, options ...parquet.FileOption) (*ParquetFile, error) {
	labelsAttr, err := bkt.Attributes(ctx, filename)
	if err != nil {
		return nil, err
	}
	reader := NewBucketReadAt(ctx, filename, bkt)
	file, err := parquet.OpenFile(reader, labelsAttr.Size, options...)
	if err != nil {
		return nil, err
	}
	return &ParquetFile{
		File:              file,
		ReadAtWithContext: reader,
	}, nil
}
