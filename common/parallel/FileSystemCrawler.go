// Copyright © Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package parallel

import (
	"context"
	"os"
	"path/filepath"
)

type FileSystemEntry struct {
	FullPath string
	Info     os.FileInfo
}

// represents a file info that we may have failed to obtain
type FailableFileInfo interface {
	os.FileInfo
	Error() error
}

type DirReader interface {
	Readdir(dir *os.File, n int) ([]os.FileInfo, error)
	Close()
}

// CrawlLocalDirectory specializes parallel.Crawl to work specifically on a local directory.
// It does not follow symlinks.
// The items in the CrawResult output channel are FileSystemEntry s.
// For a wrapper that makes this look more like filepath.Walk, see parallel.Walk.
func CrawlLocalDirectory(ctx context.Context, root Directory, parallelism int, reader DirReader, enumerateOneFileSystemDirectory EnumerateOneDir,
	getIndexerSize func() int64, tqueue chan interface{}, isSource bool, maxIndexerObjectSizeInGB uint) <-chan CrawlResult {
	return Crawl(ctx,
		root,
		func(dir Directory, enqueueDir func(Directory), enqueueOutput func(DirectoryEntry, error)) error {
			return enumerateOneFileSystemDirectory(dir, enqueueDir, enqueueOutput, reader)
		},
		parallelism, getIndexerSize, tqueue, isSource, maxIndexerObjectSizeInGB)
}

type EnumerateOneDir func(dir Directory, enqueueDir func(Directory), enqueueOutput func(DirectoryEntry, error), r DirReader) error

type WalkFunc func(filePath string, fileInfo os.FileInfo, fileError error) error

type StoredObjectFunc func(filePath string, fileInfo os.FileInfo) interface{}

// Walk is similar to filepath.Walk.
// But note the following difference is how WalkFunc is used:
// 1. If fileError passed to walkFunc is not nil, then here the filePath passed to that function will usually be ""
//    (whereas with filepath.Walk it will usually (always?) have a value).
// 2. If the return value of walkFunc function is not nil, enumeration will always stop, not matter what the type of the error.
//    (Unlike filepath.WalkFunc, where returning filePath.SkipDir is handled as a special case).
func Walk(appCtx context.Context, root string, parallelism int, parallelStat bool, walkFn WalkFunc, enumerateOneFileSystemDirectory EnumerateOneDir,
	constructStoredObject StoredObjectFunc, getIndexerMapSize func() int64, tqueue chan interface{}, isSource bool, maxObjectIndexerSizeInGB uint) {
	var ctx context.Context
	var cancel context.CancelFunc
	signalRootError := func(e error) {
		_ = walkFn(root, nil, e)
	}

	root, err := filepath.Abs(root)
	if err != nil {
		signalRootError(err)
		return
	}

	// Call walkfunc on the root.  This is necessary for compatibility with filePath.Walk
	// TODO: add at a test that CrawlLocalDirectory does NOT include the root (i.e. add test to define that behaviour)
	r, err := os.Open(root) // for directories, we don't need a special open with FILE_FLAG_BACKUP_SEMANTICS, because directory opening uses FindFirst which doesn't need that flag. https://blog.differentpla.net/blog/2007/05/25/findfirstfile-and-se_backup_name
	if err != nil {
		signalRootError(err)
		return
	}
	rs, err := r.Stat()
	if err != nil {
		signalRootError(err)
		return
	}

	err = walkFn(root, rs, nil)
	if err != nil {
		signalRootError(err)
		return
	}

	so := constructStoredObject(root, rs)
	_ = r.Close()

	// walk the stuff inside the root
	reader, remainingParallelism := NewDirReader(parallelism, parallelStat)
	defer reader.Close()
	if appCtx != nil {
		ctx, cancel = context.WithCancel(appCtx)
	} else {
		ctx, cancel = context.WithCancel(context.Background())
	}
	ch := CrawlLocalDirectory(ctx, so, remainingParallelism, reader, enumerateOneFileSystemDirectory, getIndexerMapSize, tqueue, isSource, maxObjectIndexerSizeInGB)
	for crawlResult := range ch {
		entry, err := crawlResult.Item()
		if err == nil {
			fsEntry := entry.(FileSystemEntry)
			err = walkFn(fsEntry.FullPath, fsEntry.Info, nil)
		} else {
			// Our directory scanners can enqueue FileSystemEntry items with potentially full path and fileInfo for failures encountered during enumeration.
			// If the entry is valid we pass those to caller.
			if fsEntry, ok := entry.(FileSystemEntry); ok {
				err = walkFn(fsEntry.FullPath, fsEntry.Info, err)
			} else {
				err = walkFn("", nil, err) // cannot supply path here, because crawlResult probably doesn't have one, due to the error
			}
		}
		if err != nil {
			cancel()
			return
		}
	}
}
