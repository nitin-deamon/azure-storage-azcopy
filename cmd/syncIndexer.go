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
	"path/filepath"
	"strings"
	"sync"
	"unsafe"

	"github.com/nitin-deamon/azure-storage-azcopy/v10/common"
)

type folderIndexer struct {
	folderMap map[string]*objectIndexer
	lock      sync.Mutex
	counter   int64

	// isDestinationCaseInsensitive is true when the destination is case-insensitive
	// In Windows, both paths D:\path\to\dir and D:\Path\TO\DiR point to the same resource.
	// Apple File System (APFS) can be configured to be case-sensitive or case-insensitive.
	// So for such locations, the key in the indexMap will be lowercase to avoid infinite syncing.
	isDestinationCaseInsensitive bool
}

func newfolderIndexer() *folderIndexer {
	return &folderIndexer{folderMap: make(map[string]*objectIndexer)}
}

// process the given stored object by indexing it using its relative path
func (i *folderIndexer) store(storedObject StoredObject) (err error) {
	// It is safe to index all StoredObjects just by relative path, regardless of their entity type, because
	// no filesystem allows a file and a folder to have the exact same full path.  This is true of
	// Linux file systems, Windows, Azure Files and ADLS Gen 2 (and logically should be true of all file systems).
	var lcFileName, lcFolderName, lcRelativePath string

	i.lock.Lock()
	if i.isDestinationCaseInsensitive {
		lcRelativePath = strings.ToLower(storedObject.relativePath)
	} else {
		lcRelativePath = storedObject.relativePath
	}

	if storedObject.isVirtualFolder || storedObject.entityType == common.EEntityType.Folder() {
		lcFolderName = lcRelativePath
	} else {
		lcFolderName = filepath.Dir(lcRelativePath)
		lcFileName = filepath.Base(lcRelativePath)
	}

	if lcFolderName == "" {
		lcFolderName = "."
	}

	if _, ok := i.folderMap[lcFolderName]; !ok {
		i.folderMap[lcFolderName] = newObjectIndexer()
	}
	if lcFileName != "" {
		i.folderMap[lcFolderName].indexMap[lcFileName] = storedObject
	} else {
		i.folderMap[lcFolderName].folderObject = storedObject
	}

	i.counter += 1
	i.folderMap[lcFolderName].counter += 1

	i.lock.Unlock()
	return
}

func (i *folderIndexer) getIndexerMapSize() int64 {
	i.lock.Lock()
	counter := i.counter
	i.lock.Unlock()
	return counter * int64((unsafe.Sizeof(StoredObject{})))
}

// go through the remaining stored objects in the map to process them
func (i *folderIndexer) traverse(processor objectProcessor, filters []ObjectFilter) (err error) {
	for _, folder := range i.folderMap {
		for _, value := range folder.indexMap {
			err = processIfPassedFilters(filters, value, processor)
			_, err = getProcessingError(err)
			if err != nil {
				return
			}
		}
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
