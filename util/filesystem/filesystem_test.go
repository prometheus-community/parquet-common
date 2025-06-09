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
// Provenance-includes-location: https://github.com/thanos-io/objstore/blob/71fdd5acb3633b26f88c75dd6768fdc2f9ec246b/providers/filesystem/filesystem_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package filesystem

import (
	"bytes"
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"

	"github.com/thanos-io/objstore"
)

func TestDelete_EmptyDirDeletionRaceCondition(t *testing.T) {
	const runs = 1000

	ctx := context.Background()

	for r := 0; r < runs; r++ {
		b, err := NewBucket(t.TempDir())
		testutil.Ok(t, err)

		// Upload 2 objects in a subfolder.
		testutil.Ok(t, b.Upload(ctx, "subfolder/first", strings.NewReader("first")))
		testutil.Ok(t, b.Upload(ctx, "subfolder/second", strings.NewReader("second")))

		// Prepare goroutines to concurrently delete the 2 objects (each one deletes a different object)
		start := make(chan struct{})
		group := sync.WaitGroup{}
		group.Add(2)

		for _, object := range []string{"first", "second"} {
			go func(object string) {
				defer group.Done()

				<-start
				testutil.Ok(t, b.Delete(ctx, "subfolder/"+object))
			}(object)
		}

		// Go!
		close(start)
		group.Wait()
	}
}

func TestIter_CancelledContext(t *testing.T) {
	b, err := NewBucket(t.TempDir())
	testutil.Ok(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = b.Iter(ctx, "", func(s string) error {
		return nil
	})

	testutil.NotOk(t, err)
	testutil.Equals(t, context.Canceled, err)
}

func TestIterWithAttributes(t *testing.T) {
	dir := t.TempDir()
	f, err := os.CreateTemp(dir, "test")
	testutil.Ok(t, err)
	t.Cleanup(func() { _ = f.Close() })

	stat, err := f.Stat()
	testutil.Ok(t, err)

	cases := []struct {
		name              string
		opts              []objstore.IterOption
		expectedUpdatedAt time.Time
	}{
		{
			name: "no options",
			opts: nil,
		},
		{
			name: "with updated at",
			opts: []objstore.IterOption{
				objstore.WithUpdatedAt(),
			},
			expectedUpdatedAt: stat.ModTime(),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := NewBucket(dir)
			testutil.Ok(t, err)

			var attrs objstore.IterObjectAttributes

			ctx := context.Background()
			err = b.IterWithAttributes(ctx, "", func(objectAttrs objstore.IterObjectAttributes) error {
				attrs = objectAttrs
				return nil
			}, tc.opts...)

			testutil.Ok(t, err)

			lastModified, ok := attrs.LastModified()
			if zero := tc.expectedUpdatedAt.IsZero(); zero {
				testutil.Equals(t, false, ok)
			} else {
				testutil.Equals(t, true, ok)
				testutil.Equals(t, tc.expectedUpdatedAt, lastModified)
			}
		})
	}
}

func TestGet_CancelledContext(t *testing.T) {
	b, err := NewBucket(t.TempDir())
	testutil.Ok(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = b.Get(ctx, "some-file")
	testutil.NotOk(t, err)
	testutil.Equals(t, context.Canceled, err)
}

func TestAttributes_CancelledContext(t *testing.T) {
	b, err := NewBucket(t.TempDir())
	testutil.Ok(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = b.Attributes(ctx, "some-file")
	testutil.NotOk(t, err)
	testutil.Equals(t, context.Canceled, err)
}

func TestGetRange_CancelledContext(t *testing.T) {
	b, err := NewBucket(t.TempDir())
	testutil.Ok(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = b.GetRange(ctx, "some-file", 0, 100)
	testutil.NotOk(t, err)
	testutil.Equals(t, context.Canceled, err)
}

func TestExists_CancelledContext(t *testing.T) {
	b, err := NewBucket(t.TempDir())
	testutil.Ok(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = b.Exists(ctx, "some-file")
	testutil.NotOk(t, err)
	testutil.Equals(t, context.Canceled, err)
}

func TestUpload_CancelledContext(t *testing.T) {
	b, err := NewBucket(t.TempDir())
	testutil.Ok(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = b.Upload(ctx, "some-file", bytes.NewReader([]byte("file content")))
	testutil.NotOk(t, err)
	testutil.Equals(t, context.Canceled, err)
}

func TestDelete_CancelledContext(t *testing.T) {
	b, err := NewBucket(t.TempDir())
	testutil.Ok(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = b.Delete(ctx, "some-file")
	testutil.NotOk(t, err)
	testutil.Equals(t, context.Canceled, err)
}
