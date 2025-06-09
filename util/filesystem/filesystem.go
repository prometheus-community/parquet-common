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
// Provenance-includes-location: https://github.com/thanos-io/objstore/blob/71fdd5acb3633b26f88c75dd6768fdc2f9ec246b/providers/filesystem/filesystem.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package filesystem

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/efficientgo/core/errcapture"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"

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

// Bucket implements the objstore.Bucket interfaces against filesystem that binary runs on.
// Methods from Bucket interface are thread-safe. Objects are assumed to be immutable.
// NOTE: It does not follow symbolic links.
type Bucket struct {
	rootDir   string
	fileCache map[string]*cachedFileInfo
	mutex     sync.RWMutex
}

// NewBucketFromConfig returns a new filesystem.Bucket from config.
func NewBucketFromConfig(conf []byte) (*Bucket, error) {
	var c Config
	if err := yaml.Unmarshal(conf, &c); err != nil {
		return nil, err
	}
	if c.Directory == "" {
		return nil, errors.New("missing directory for filesystem bucket")
	}
	return NewBucket(c.Directory)
}

// NewBucket returns a new filesystem.Bucket.
func NewBucket(rootDir string) (*Bucket, error) {
	absDir, err := filepath.Abs(rootDir)
	if err != nil {
		return nil, err
	}
	return &Bucket{
		rootDir:   absDir,
		fileCache: make(map[string]*cachedFileInfo),
	}, nil
}

func (b *Bucket) Provider() objstore.ObjProvider { return objstore.FILESYSTEM }

func (b *Bucket) SupportedIterOptions() []objstore.IterOptionType {
	return []objstore.IterOptionType{objstore.Recursive, objstore.UpdatedAt}
}

func (b *Bucket) IterWithAttributes(ctx context.Context, dir string, f func(attrs objstore.IterObjectAttributes) error, options ...objstore.IterOption) error {
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
func (b *Bucket) Iter(ctx context.Context, dir string, f func(string) error, opts ...objstore.IterOption) error {
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
func (b *Bucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
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
func (b *Bucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
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
func (b *Bucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
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

		f, err := os.OpenFile(filepath.Clean(file), os.O_RDONLY, 0600)
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
			f.Close()
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
func (b *Bucket) Exists(ctx context.Context, name string) (bool, error) {
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
func (b *Bucket) Upload(ctx context.Context, name string, r io.Reader, _ ...objstore.ObjectUploadOption) (err error) {
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
func (b *Bucket) Delete(ctx context.Context, name string) error {
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
func (b *Bucket) IsObjNotFoundErr(err error) bool {
	return os.IsNotExist(errors.Cause(err))
}

// IsAccessDeniedErr returns true if access to object is denied.
func (b *Bucket) IsAccessDeniedErr(_ error) bool {
	return false
}

func (b *Bucket) Close() error {
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
func (b *Bucket) Name() string {
	return fmt.Sprintf("fs: %s", b.rootDir)
}
