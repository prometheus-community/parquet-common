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

package queryable

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/efficientgo/core/errcapture"
	"github.com/efficientgo/core/testutil"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"
)

// Config stores the configuration for storing and accessing blobs in filesystem.
type Config struct {
	Directory string `yaml:"directory"`
}

// cachedFileInfo holds both the file descriptor and metadata to avoid repeated Stat() calls
type cachedFileInfo struct {
	file *os.File
	size int64
}

// bucket implements the objstore.Bucket interfaces against filesystem that binary runs on.
// Methods from bucket interface are thread-safe. Objects are assumed to be immutable.
// NOTE: It does not follow symbolic links.
type bucket struct {
	rootDir   string
	fileCache map[string]*cachedFileInfo
	mutex     sync.RWMutex
}

// newBucket returns a new filesystem.Bucket.
func newBucket(rootDir string) (*bucket, error) {
	absDir, err := filepath.Abs(rootDir)
	if err != nil {
		return nil, err
	}
	return &bucket{
		rootDir:   absDir,
		fileCache: make(map[string]*cachedFileInfo),
	}, nil
}

func (b *bucket) Provider() objstore.ObjProvider { return objstore.FILESYSTEM }

func (b *bucket) SupportedIterOptions() []objstore.IterOptionType {
	return []objstore.IterOptionType{objstore.Recursive, objstore.UpdatedAt}
}

func (b *bucket) IterWithAttributes(ctx context.Context, dir string, f func(attrs objstore.IterObjectAttributes) error, options ...objstore.IterOption) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	if err := objstore.ValidateIterOptions(b.SupportedIterOptions(), options...); err != nil {
		return err
	}

	params := objstore.ApplyIterOptions(options...)
	absDir := filepath.Join(b.rootDir, dir)
	info, err := os.Stat(absDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return errors.Wrapf(err, "stat %s", absDir)
	}
	if !info.IsDir() {
		return nil
	}

	files, err := os.ReadDir(absDir)
	if err != nil {
		return err
	}
	for _, file := range files {
		name := filepath.Join(dir, file.Name())

		if file.IsDir() {
			empty, err := isDirEmpty(filepath.Join(absDir, file.Name()))
			if err != nil {
				return err
			}

			if empty {
				// Skip empty directories.
				continue
			}

			name += objstore.DirDelim

			if params.Recursive {
				// Recursively list files in the subdirectory.
				if err := b.IterWithAttributes(ctx, name, f, options...); err != nil {
					return err
				}

				// The callback f() has already been called for the subdirectory
				// files so we should skip to next filesystem entry.
				continue
			}
		}

		attrs := objstore.IterObjectAttributes{
			Name: name,
		}
		if params.LastModified {
			absPath := filepath.Join(absDir, file.Name())
			stat, err := os.Stat(absPath)
			if err != nil {
				return errors.Wrapf(err, "stat %s", name)
			}
			attrs.SetLastModified(stat.ModTime())
		}
		if err := f(attrs); err != nil {
			return err
		}
	}
	return nil
}

// Iter calls f for each entry in the given directory. The argument to f is the full
// object name including the prefix of the inspected directory.
func (b *bucket) Iter(ctx context.Context, dir string, f func(string) error, opts ...objstore.IterOption) error {
	// Only include recursive option since attributes are not used in this method.
	var filteredOpts []objstore.IterOption
	for _, opt := range opts {
		if opt.Type == objstore.Recursive {
			filteredOpts = append(filteredOpts, opt)
			break
		}
	}

	return b.IterWithAttributes(ctx, dir, func(attrs objstore.IterObjectAttributes) error {
		return f(attrs.Name)
	}, filteredOpts...)
}

// Get returns a reader for the given object name.
func (b *bucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return b.GetRange(ctx, name, 0, -1)
}

// readAtReader implements io.ReadCloser using ReadAt with a cached file descriptor
type readAtReader struct {
	f      *os.File
	offset int64
	size   int64
	pos    int64
}

func (r *readAtReader) Read(p []byte) (n int, err error) {
	if r.pos >= r.size {
		return 0, io.EOF
	}

	remaining := r.size - r.pos
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}

	n, err = r.f.ReadAt(p, r.offset+r.pos)
	r.pos += int64(n)

	if r.pos >= r.size && err == nil {
		err = io.EOF
	}

	return n, err
}

func (r *readAtReader) Close() error {
	// Don't close the cached file descriptor
	return nil
}

// Attributes returns information about the specified object.
func (b *bucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	if ctx.Err() != nil {
		return objstore.ObjectAttributes{}, ctx.Err()
	}

	file := filepath.Join(b.rootDir, name)

	// Check cache first
	b.mutex.RLock()
	cachedInfo, exists := b.fileCache[file]
	b.mutex.RUnlock()

	if exists {
		// Use cached file size but we still need to stat for LastModified
		// since that can change and we don't cache it
		stat, err := os.Stat(file)
		if err != nil {
			return objstore.ObjectAttributes{}, errors.Wrapf(err, "stat %s", file)
		}
		return objstore.ObjectAttributes{
			Size:         cachedInfo.size,
			LastModified: stat.ModTime(),
		}, nil
	}

	// Not in cache, do full stat
	stat, err := os.Stat(file)
	if err != nil {
		return objstore.ObjectAttributes{}, errors.Wrapf(err, "stat %s", file)
	}

	return objstore.ObjectAttributes{
		Size:         stat.Size(),
		LastModified: stat.ModTime(),
	}, nil
}

// GetRange returns a new range reader for the given object name and range.
func (b *bucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if name == "" {
		return nil, errors.New("object name is empty")
	}

	file := filepath.Join(b.rootDir, name)

	// Check cache first
	b.mutex.RLock()
	cachedInfo, exists := b.fileCache[file]
	b.mutex.RUnlock()

	if !exists {
		// File not in cache, stat it, open it, and add to cache
		stat, err := os.Stat(file)
		if err != nil {
			return nil, errors.Wrapf(err, "stat %s", file)
		}

		f, err := os.OpenFile(filepath.Clean(file), os.O_RDONLY, 0o600)
		if err != nil {
			return nil, err
		}

		info := &cachedFileInfo{
			file: f,
			size: stat.Size(),
		}

		b.mutex.Lock()
		// Check again in case another goroutine added it while we were waiting for the lock
		if existingInfo, alreadyExists := b.fileCache[file]; alreadyExists {
			// Another goroutine already added it, close our copy and use the existing one
			_ = f.Close()
			cachedInfo = existingInfo
		} else {
			// We're the first to add it to the cache
			b.fileCache[file] = info
			cachedInfo = info
		}
		b.mutex.Unlock()
	}

	// Calculate the size to read using cached file size
	fileSize := cachedInfo.size
	if off > fileSize {
		off = fileSize
	}

	var readSize int64
	if length == -1 {
		readSize = fileSize - off
	} else {
		readSize = length
		if off+readSize > fileSize {
			readSize = fileSize - off
		}
	}

	reader := &readAtReader{
		f:      cachedInfo.file,
		offset: off,
		size:   readSize,
		pos:    0,
	}

	return objstore.ObjectSizerReadCloser{
		ReadCloser: reader,
		Size: func() (int64, error) {
			return readSize, nil
		},
	}, nil
}

// Exists checks if the given directory exists in memory.
func (b *bucket) Exists(ctx context.Context, name string) (bool, error) {
	if ctx.Err() != nil {
		return false, ctx.Err()
	}

	info, err := os.Stat(filepath.Join(b.rootDir, name))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, errors.Wrapf(err, "stat %s", filepath.Join(b.rootDir, name))
	}
	return !info.IsDir(), nil
}

// Upload writes the file specified in src to into the memory.
func (b *bucket) Upload(ctx context.Context, name string, r io.Reader, _ ...objstore.ObjectUploadOption) (err error) {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	file := filepath.Join(b.rootDir, name)
	if err := os.MkdirAll(filepath.Dir(file), os.ModePerm); err != nil {
		return err
	}

	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer errcapture.Do(&err, f.Close, "close")

	if _, err := io.Copy(f, r); err != nil {
		return errors.Wrapf(err, "copy to %s", file)
	}
	return nil
}

func isDirEmpty(name string) (ok bool, err error) {
	f, err := os.Open(filepath.Clean(name))
	if os.IsNotExist(err) {
		// The directory doesn't exist. We don't consider it an error and we treat it like empty.
		return true, nil
	}
	if err != nil {
		return false, err
	}
	defer errcapture.Do(&err, f.Close, "close dir")

	if _, err = f.Readdir(1); err == io.EOF || os.IsNotExist(err) {
		return true, nil
	}
	return false, err
}

// Delete removes all data prefixed with the dir.
func (b *bucket) Delete(ctx context.Context, name string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	file := filepath.Join(b.rootDir, name)
	for file != b.rootDir {
		if err := os.RemoveAll(file); err != nil {
			return errors.Wrapf(err, "rm %s", file)
		}
		file = filepath.Dir(file)
		empty, err := isDirEmpty(file)
		if err != nil {
			return err
		}
		if !empty {
			break
		}
	}
	return nil
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (b *bucket) IsObjNotFoundErr(err error) bool {
	return os.IsNotExist(errors.Cause(err))
}

// IsAccessDeniedErr returns true if access to object is denied.
func (b *bucket) IsAccessDeniedErr(_ error) bool {
	return false
}

func (b *bucket) Close() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	var errs []error
	for path, info := range b.fileCache {
		if err := info.file.Close(); err != nil {
			errs = append(errs, errors.Wrapf(err, "close cached file %s", path))
		}
	}

	// Clear the cache
	b.fileCache = make(map[string]*cachedFileInfo)

	if len(errs) > 0 {
		return fmt.Errorf("errors closing cached files: %v", errs)
	}
	return nil
}

// Name returns the bucket name.
func (b *bucket) Name() string {
	return fmt.Sprintf("fs: %s", b.rootDir)
}

func TestDelete_EmptyDirDeletionRaceCondition(t *testing.T) {
	const runs = 1000

	ctx := context.Background()

	for r := 0; r < runs; r++ {
		b, err := newBucket(t.TempDir())
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
	b, err := newBucket(t.TempDir())
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
			b, err := newBucket(dir)
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
	b, err := newBucket(t.TempDir())
	testutil.Ok(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = b.Get(ctx, "some-file")
	testutil.NotOk(t, err)
	testutil.Equals(t, context.Canceled, err)
}

func TestAttributes_CancelledContext(t *testing.T) {
	b, err := newBucket(t.TempDir())
	testutil.Ok(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = b.Attributes(ctx, "some-file")
	testutil.NotOk(t, err)
	testutil.Equals(t, context.Canceled, err)
}

func TestGetRange_CancelledContext(t *testing.T) {
	b, err := newBucket(t.TempDir())
	testutil.Ok(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = b.GetRange(ctx, "some-file", 0, 100)
	testutil.NotOk(t, err)
	testutil.Equals(t, context.Canceled, err)
}

func TestExists_CancelledContext(t *testing.T) {
	b, err := newBucket(t.TempDir())
	testutil.Ok(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = b.Exists(ctx, "some-file")
	testutil.NotOk(t, err)
	testutil.Equals(t, context.Canceled, err)
}

func TestUpload_CancelledContext(t *testing.T) {
	b, err := newBucket(t.TempDir())
	testutil.Ok(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = b.Upload(ctx, "some-file", bytes.NewReader([]byte("file content")))
	testutil.NotOk(t, err)
	testutil.Equals(t, context.Canceled, err)
}

func TestDelete_CancelledContext(t *testing.T) {
	b, err := newBucket(t.TempDir())
	testutil.Ok(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = b.Delete(ctx, "some-file")
	testutil.NotOk(t, err)
	testutil.Equals(t, context.Canceled, err)
}
