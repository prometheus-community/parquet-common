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

package storage

import (
	"context"
	"os"
	"sync"

	"github.com/hashicorp/go-multierror"
	"github.com/parquet-go/parquet-go"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"

	"github.com/prometheus-community/parquet-common/schema"
)

type ParquetFileConfigView interface {
	SkipMagicBytes() bool
	SkipPageIndex() bool
	SkipBloomFilters() bool
	OptimisticRead() bool
	ReadBufferSize() int
	ReadMode() parquet.ReadMode
}

type ParquetFileConfig struct {
	config *parquet.FileConfig
}

func (p ParquetFileConfig) SkipMagicBytes() bool {
	return p.config.SkipMagicBytes
}

func (p ParquetFileConfig) SkipPageIndex() bool {
	return p.config.SkipPageIndex
}

func (p ParquetFileConfig) SkipBloomFilters() bool {
	return p.config.SkipBloomFilters
}

func (p ParquetFileConfig) OptimisticRead() bool {
	return p.config.OptimisticRead
}

func (p ParquetFileConfig) ReadBufferSize() int {
	return p.config.ReadBufferSize
}

func (p ParquetFileConfig) ReadMode() parquet.ReadMode {
	return p.config.ReadMode
}

type ParquetFileView interface {
	parquet.FileView
	ReadAtWithContextCloser

	ParquetFileConfigView
}

var DefaultFileOptions = []parquet.FileOption{
	parquet.OptimisticRead(true),
}

type ParquetFile struct {
	*parquet.File
	ReadAtWithContextCloser

	ParquetFileConfigView
}

//func (f *ParquetFile) Size() int64 {
//	return f.ReadAtWithContextCloser.Size()
//}

func GetPages(ctx context.Context, fileView ParquetFileView, cc parquet.ColumnChunk, optimisticReadPages ...int) (*parquet.FilePages, error) {
	colChunk := cc.(*parquet.FileColumnChunk)
	reader := fileView.WithContext(ctx)

	if len(optimisticReadPages) > 0 && fileView.OptimisticRead() {
		offset, err := cc.OffsetIndex()
		if err != nil {
			return nil, err
		}
		minOffset := offset.Offset(optimisticReadPages[0])
		maxOffset := offset.Offset(optimisticReadPages[len(optimisticReadPages)-1]) + offset.CompressedPageSize(optimisticReadPages[len(optimisticReadPages)-1])
		reader = newOptimisticReaderAt(reader, minOffset, maxOffset)
	}

	pages := colChunk.PagesFrom(reader)
	return pages, nil
}

func Open(ctx context.Context, r ReadAtWithContextCloser, size int64, opts ...parquet.FileOption) (*ParquetFile, error) {
	// allow overriding defaults by applying opts args after defaults
	opts = append(DefaultFileOptions, opts...)
	cfg, err := parquet.NewFileConfig(opts...)
	if err != nil {
		return nil, err
	}

	file, err := parquet.OpenFile(r.WithContext(ctx), size, opts...)
	if err != nil {
		return nil, err
	}

	return &ParquetFile{
		File:                    file,
		ReadAtWithContextCloser: r,
		ParquetFileConfigView:   ParquetFileConfig{config: cfg},
	}, nil
}

func OpenFromBucket(ctx context.Context, bkt objstore.BucketReader, name string, opts ...parquet.FileOption) (*ParquetFile, error) {
	attr, err := bkt.Attributes(ctx, name)
	if err != nil {
		return nil, err
	}

	r := NewBucketReadAt(name, bkt)
	return Open(ctx, r, attr.Size, opts...)
}

func OpenFromFile(ctx context.Context, path string, opts ...parquet.FileOption) (*ParquetFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	r := NewFileReadAt(f)
	pf, err := Open(ctx, r, stat.Size(), opts...)
	if err != nil {
		_ = r.Close()
		return nil, err
	}
	// At this point, the file's lifecycle is managed by the ParquetFile
	return pf, nil
}

type ParquetShard interface {
	LabelsFile() ParquetFileView
	ChunksFile() ParquetFileView
	TSDBSchema() (*schema.TSDBSchema, error)
}

type ParquetOpener interface {
	Open(ctx context.Context, path string, opts ...parquet.FileOption) (ParquetFileView, error)
}

type ParquetBucketOpener struct {
	bkt objstore.BucketReader
}

func NewParquetBucketOpener(bkt objstore.BucketReader) *ParquetBucketOpener {
	return &ParquetBucketOpener{
		bkt: bkt,
	}
}

func (o *ParquetBucketOpener) Open(ctx context.Context, name string, opts ...parquet.FileOption) (ParquetFileView, error) {
	return OpenFromBucket(ctx, o.bkt, name, opts...)
}

type ParquetLocalFileOpener struct{}

func NewParquetLocalFileOpener() *ParquetLocalFileOpener {
	return &ParquetLocalFileOpener{}
}

func (o *ParquetLocalFileOpener) Open(ctx context.Context, name string, opts ...parquet.FileOption) (ParquetFileView, error) {
	return OpenFromFile(ctx, name, opts...)
}

type ParquetShardOpener struct {
	labelsFile, chunksFile ParquetFileView
	schema                 *schema.TSDBSchema
	o                      sync.Once
}

func NewParquetShardOpener(
	ctx context.Context,
	name string,
	labelsFileOpener ParquetOpener,
	chunksFileOpener ParquetOpener,
	shard int,
	opts ...parquet.FileOption,
) (*ParquetShardOpener, error) {
	labelsFileName := schema.LabelsPfileNameForShard(name, shard)
	chunksFileName := schema.ChunksPfileNameForShard(name, shard)

	errGroup := errgroup.Group{}

	var labelsFile, chunksFile ParquetFileView

	errGroup.Go(func() (err error) {
		labelsFile, err = labelsFileOpener.Open(ctx, labelsFileName, opts...)
		return err
	})

	errGroup.Go(func() (err error) {
		chunksFile, err = chunksFileOpener.Open(ctx, chunksFileName, opts...)
		return err
	})

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	return &ParquetShardOpener{
		labelsFile: labelsFile,
		chunksFile: chunksFile,
	}, nil
}

func (s *ParquetShardOpener) LabelsFile() ParquetFileView {
	return s.labelsFile
}

func (s *ParquetShardOpener) ChunksFile() ParquetFileView {
	return s.chunksFile
}

func (s *ParquetShardOpener) TSDBSchema() (*schema.TSDBSchema, error) {
	var err error
	s.o.Do(func() {
		s.schema, err = schema.FromLabelsFile(s.labelsFile)
	})
	return s.schema, err
}

func (s *ParquetShardOpener) Close() error {
	err := &multierror.Error{}
	err = multierror.Append(err, s.labelsFile.Close())
	err = multierror.Append(err, s.chunksFile.Close())
	return err.ErrorOrNil()
}
