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

package schema

import (
	"fmt"
	"strings"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/compress/snappy"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/parquet-go/parquet-go/format"
)

const (
	// LabelColumnPrefix is the prefix used for all Prometheus label columns in the Parquet schema.
	// Each label becomes a column named "l_{label_name}" (e.g., "l___name__", "l_job", "l_instance").
	// These columns store label values as optional strings with dictionary encoding for compression.
	LabelColumnPrefix = "l_"

	// DataColumnPrefix is the prefix used for time-partitioned data columns that store encoded chunks.
	// Data columns are named "s_data_{index}" (e.g., "s_data_0", "s_data_1") where each column
	// represents a time range based on the configured column duration (default 8 hours).
	DataColumnPrefix = "s_data_"

	// ColIndexes is the column name that stores which label columns contain values for each row.
	// This column contains a compressed list of column indexes, allowing efficient reconstruction
	// of the labelset without scanning all label columns. Uses delta byte array encoding.
	ColIndexes = "s_col_indexes"

	// SeriesHash is the column name that stores the hash of the complete Prometheus labelset.
	// Contains the result of labels.Hash() encoded as an 8-byte array in big-endian format.
	// Used for fast series identification, deduplication, and joins across Parquet files.
	SeriesHash = "s_series_hash"

	DataColSizeMd = "data_col_duration_ms"
	MinTMd        = "minT"
	MaxTMd        = "maxT"
)

type CompressionCodec int

const (
	CompressionZstd CompressionCodec = iota
	CompressionSnappy
)

type compressionOpts struct {
	enabled bool
	codec   CompressionCodec
	level   zstd.Level
}

var DefaultCompressionOpts = compressionOpts{
	enabled: true,
	codec:   CompressionZstd,
	level:   zstd.SpeedBetterCompression,
}

type CompressionOpts func(*compressionOpts)

func WithCompressionEnabled(enabled bool) CompressionOpts {
	return func(opts *compressionOpts) {
		opts.enabled = enabled
	}
}

func WithCompressionCodec(codec CompressionCodec) CompressionOpts {
	return func(opts *compressionOpts) {
		opts.codec = codec
	}
}

func WithCompressionLevel(level zstd.Level) CompressionOpts {
	return func(opts *compressionOpts) {
		opts.level = level
	}
}

func LabelToColumn(lbl string) string {
	return fmt.Sprintf("%s%s", LabelColumnPrefix, lbl)
}

func ExtractLabelFromColumn(col string) (string, bool) {
	if !strings.HasPrefix(col, LabelColumnPrefix) {
		return "", false
	}
	return col[len(LabelColumnPrefix):], true
}

func IsDataColumn(col string) bool {
	return strings.HasPrefix(col, DataColumnPrefix)
}

func DataColumn(i int) string {
	return fmt.Sprintf("%s%v", DataColumnPrefix, i)
}

func LabelsPfileNameForShard(name string, shard int) string {
	return fmt.Sprintf("%s/%d.%s", name, shard, "labels.parquet")
}

func ChunksPfileNameForShard(name string, shard int) string {
	return fmt.Sprintf("%s/%d.%s", name, shard, "chunks.parquet")
}

// WithCompression applies compression configuration to a parquet schema.
//
// This function takes a parquet schema and applies compression settings based on the provided options.
// By default, it enables zstd compression with SpeedBetterCompression level, maintaining backward compatibility.
// The compression can be disabled, or the codec and level can be customized using the configuration options.
func WithCompression(s *parquet.Schema, opts ...CompressionOpts) *parquet.Schema {
	cfg := DefaultCompressionOpts

	for _, opt := range opts {
		opt(&cfg)
	}

	if !cfg.enabled {
		return s
	}

	g := make(parquet.Group)

	for _, c := range s.Columns() {
		lc, _ := s.Lookup(c...)

		var codec compress.Codec
		switch cfg.codec {
		case CompressionZstd:
			codec = &zstd.Codec{Level: cfg.level}
		case CompressionSnappy:
			codec = &snappy.Codec{}
		default:
			codec = &zstd.Codec{Level: zstd.SpeedBetterCompression}
		}

		g[lc.Path[0]] = parquet.Compressed(lc.Node, codec)
	}

	return parquet.NewSchema("compressed", g)
}

func MetadataToMap(md []format.KeyValue) map[string]string {
	r := make(map[string]string, len(md))
	for _, kv := range md {
		r[kv.Key] = kv.Value
	}
	return r
}