// Copyright © 2017 Microsoft <wastore@microsoft.com>
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

package cmd

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/pkg/errors"
)

//
// Hierarchical object indexer for quickly searching children of a given directory.
// Used by the streaming sync processor.
//
type folderIndexer struct {
	// FolderMap is a map for a folder, which stores map of files in respective folder.
	// Key here is parent folder f.e dir1/dir2/file.txt the key is dir1/dir2. It has objectIndexer map for this path.
	// ObjectIndexer is again map with key file.txt and it stores storedObject of this file.
	folderMap map[string]*objectIndexer

	// lock should be held when reading/modifying folderMap.
	lock sync.RWMutex

	// counter stores number of files/folders in FolderMap
	counter int64

	//
	totalSize int64
	// isDestinationCaseInsensitive is true when the destination is case-insensitive
	// In Windows, both paths D:\path\to\dir and D:\Path\TO\DiR point to the same resource.
	// Apple File System (APFS) can be configured to be case-sensitive or case-insensitive.
	// So for such locations, the key in the indexMap will be lowercase to avoid infinite syncing.
	isDestinationCaseInsensitive bool
}

func newfolderIndexer() *folderIndexer {
	return &folderIndexer{folderMap: make(map[string]*objectIndexer)}
}

func storedObjectSize(so StoredObject) int64 {
	return int64(unsafe.Sizeof(StoredObject{}) + unsafe.Sizeof(so.name) + unsafe.Sizeof(so.relativePath))
}

// process the given stored object by indexing it using its relative path
func (i *folderIndexer) store(storedObject StoredObject) (err error) {
	// It is safe to index all StoredObjects just by relative path, regardless of their entity type, because
	// no filesystem allows a file and a folder to have the exact same full path.  This is true of
	// Linux file systems, Windows, Azure Files and ADLS Gen 2 (and logically should be true of all file systems).
	var lcFileName, lcFolderName, lcRelativePath string

	size := storedObjectSize(storedObject)

	if i.isDestinationCaseInsensitive {
		lcRelativePath = strings.ToLower(storedObject.relativePath)
	} else {
		lcRelativePath = storedObject.relativePath
	}

	lcFolderName = filepath.Dir(lcRelativePath)
	lcFileName = filepath.Base(lcRelativePath)
	
	i.lock.Lock()
	if _, ok := i.folderMap[lcFolderName]; !ok {
		i.folderMap[lcFolderName] = newObjectIndexer()
	}
	if lcFileName != "" {
		if _, ok := i.folderMap[lcFolderName].indexMap[lcFileName]; !ok {
			i.folderMap[lcFolderName].indexMap[lcFileName] = storedObject
		} else {
			fmt.Printf("FileName [%s] under Folder [%s] already present in map", lcFileName, lcFolderName)
			return errors.Errorf("FileName [%s] and FolderName [%s] already present in map", lcFileName, lcFolderName)
		}
	}

	i.counter += 1
	i.folderMap[lcFolderName].counter += 1
	atomic.AddInt64(&i.totalSize, size)

	i.lock.Unlock()
	return
}

func (i *folderIndexer) getIndexerMapSize() int64 {
	return atomic.LoadInt64(&i.totalSize)
}

func (i *folderIndexer) getStoredObject(lcRelativePath string) StoredObject {
	lcFolderName := filepath.Dir(lcRelativePath)
	lcFileName := filepath.Base(lcRelativePath)
	i.lock.RLock()
	if folderMap, ok := i.folderMap[lcFolderName]; ok {
		if so, ok := folderMap.indexMap[lcFileName]; ok {
			return so
		}
	}
	i.lock.RUnlock()
	panic(fmt.Sprintf("Stored Object for relative path[%s] not found", lcRelativePath))
}

// go through the remaining stored objects in the map to process them
func (i *folderIndexer) traverse(processor objectProcessor, filters []ObjectFilter) (err error) {
	for _, folder := range i.folderMap {
		for _, value := range folder.indexMap {
			fmt.Printf("\n File with relative path[%s] still in map", value.relativePath)
		}
		// TODO: Need to panic/assert here as folderMap should be empty.
		// As of now added the print statements so that we can debug.
		panic("Map should be empty but still it has some entries.")
	}
	return
}

// the objectIndexer is essential for the generic sync enumerator to work
// it can serve as a:
// 		1. objectProcessor: accumulate a lookup map with given StoredObjects
//		2. resourceTraverser: go through the entities in the map like a traverser
type objectIndexer struct {
	indexMap     map[string]StoredObject
	counter      int
	folderObject StoredObject
	// isDestinationCaseInsensitive is true when the destination is case-insensitive
	// In Windows, both paths D:\path\to\dir and D:\Path\TO\DiR point to the same resource.
	// Apple File System (APFS) can be configured to be case-sensitive or case-insensitive.
	// So for such locations, the key in the indexMap will be lowercase to avoid infinite syncing.
	isDestinationCaseInsensitive bool
}

func newObjectIndexer() *objectIndexer {
	return &objectIndexer{indexMap: make(map[string]StoredObject)}
}

// process the given stored object by indexing it using its relative path
func (i *objectIndexer) store(storedObject StoredObject) (err error) {
	// TODO we might buffer too much data in memory, figure out whether we should limit the max number of files
	// TODO previously we used 10M as the max, but it was proven to be too small for some users

	// It is safe to index all StoredObjects just by relative path, regardless of their entity type, because
	// no filesystem allows a file and a folder to have the exact same full path.  This is true of
	// Linux file systems, Windows, Azure Files and ADLS Gen 2 (and logically should be true of all file systems).
	if i.isDestinationCaseInsensitive {
		lcRelativePath := strings.ToLower(storedObject.relativePath)
		i.indexMap[lcRelativePath] = storedObject
	} else {
		i.indexMap[storedObject.relativePath] = storedObject
	}
	i.counter += 1
	return
}

// go through the remaining stored objects in the map to process them
func (i *objectIndexer) traverse(processor objectProcessor, filters []ObjectFilter) (err error) {
	for _, value := range i.indexMap {
		err = processIfPassedFilters(filters, value, processor)
		_, err = getProcessingError(err)
		if err != nil {
			return
		}
	}
	return
}
