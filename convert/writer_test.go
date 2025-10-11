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

package convert

import (
	"context"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/prometheus-community/parquet-common/schema"
)

func Test_ShouldRespectContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	bkt, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = bkt.Close() })
	s := &schema.TSDBProjection{
		Schema: parquet.NewSchema("testRow", parquet.Group{
			"testField": parquet.Leaf(parquet.FixedLenByteArrayType(32)),
		}),
	}

	pipeReaderWriter := NewPipeReaderBucketWriter(bkt)
	sw, err := newSplitFileWriter(ctx, s.Schema, map[string]*schema.TSDBProjection{"test": s}, pipeReaderWriter)
	require.NoError(t, err)
	require.ErrorIs(t, sw.Close(), context.Canceled)
}
