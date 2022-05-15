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
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/nitin-deamon/azure-storage-azcopy/v10/common"
	"github.com/pkg/errors"
)

//
// Hierarchical object indexer for quickly searching children of a given directory.
// We keep parent and child relationship only not ancestor. That's said it's flat and hierarchical namespace where all folder at flat level,
// each folder contains its children. So it helps us take decision on folder. Used by the streaming sync processor.
//
type folderIndexer struct {
	// FolderMap is a map for a folder, which stores map of files in respective folder.
	// Key here is parent folder f.e dir1/dir2/file.txt the key is dir1/dir2. It has objectIndexer map for this path.
	// ObjectIndexer is again map with key file.txt and it stores storedObject of this file.
	folderMap map[string]*objectIndexer

	// lock should be held when reading/modifying folderMap.
	lock sync.RWMutex

	// Memory consumed in bytes by this folderIndexer, including all the objectIndexer data it stores.
	// It's signed as it help to do sanity check.
	totalSize int64

	// isDestinationCaseInsensitive is true when the destination is case-insensitive
	// In Windows, both paths D:\path\to\dir and D:\Path\TO\DiR point to the same resource.
	// Apple File System (APFS) can be configured to be case-sensitive or case-insensitive.
	// So for such locations, the key in the indexMap will be lowercase to avoid infinite syncing.
	isDestinationCaseInsensitive bool
}

// Constructor for folderIndexer.
func newfolderIndexer() *folderIndexer {
	return &folderIndexer{folderMap: make(map[string]*objectIndexer)}
}

// storedObjectSize function calculates the size of given stored object.
func storedObjectSize(so StoredObject) int64 {

	return int64(unsafe.Sizeof(StoredObject{})) + int64(len(so.name)+len(so.relativePath)+len(so.contentDisposition)+
		len(so.cacheControl)+len(so.contentLanguage)+len(so.contentEncoding)+len(so.contentType)+len(so.ContainerName)+len(so.DstContainerName))
}

// folderIndexer.store() is called by Source Traverser for all scanned objects.
// It stores the storedObject in folderIndexer.folderMap["parent directory path"] where Target Traverser can
// look it up for finding if the object needs to be sync'ed.
//
// Note: Source Traverser blindly saves all the scanned objects for Target Traverser to look up.
//       All the intelligence regarding which object needs to be sync'ed, whether it's a full sync or
//       just metadata sync, is in the Target Traverser.
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
	defer i.lock.Unlock()

	// Very first object scanned in the folder, create the objectIndexer for this folder.
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

	/*
	 * Why we need to this because folder storedObject get deleted as parent enumeration only exception to this root folder.
	 * We need storedObject of this folder at time of this folder enumeration.
	 *
	 * Note: We can make it same for root folder too, without any use. It will make code same for all cases.
	 */
	if storedObject.isVirtualFolder || storedObject.entityType == common.EEntityType.Folder() {
		lcFolderName = path.Join(lcFolderName, lcFileName)
		if _, ok := i.folderMap[lcFolderName]; !ok {
			i.folderMap[lcFolderName] = newObjectIndexer()
		}
		i.folderMap[lcFolderName].folderObject = storedObject
		size += size
	}

	/*
	 * We can do this atomic operation outside of lock too. It reduce the locking time.
	 * It can lead to some discrepencies where accounting of storedObject delayed.
	 */
	atomic.AddInt64(&i.totalSize, size)

	return
}

// getIndexerMapSize return the size of map.
func (i *folderIndexer) getIndexerMapSize() int64 {
	return atomic.LoadInt64(&i.totalSize)
}

// traverse called in last as sanity.
func (i *folderIndexer) traverse(processor objectProcessor, filters []ObjectFilter) (err error) {
	found := false

	if atomic.LoadInt64(&i.totalSize) != 0 {
		panic(fmt.Sprintf("Total Size should be zero. Size: %v", atomic.LoadInt64(&i.totalSize)))
	}

	for _, folder := range i.folderMap {
		found = true
		fmt.Printf("\n Folder with relative path[%s] still in map", folder.folderObject.relativePath)
		for _, value := range folder.indexMap {
			fmt.Printf("\n File with relative path[%s] still in map", value.relativePath)
		}
	}

	// Let's panic in case there are entries in objectIndexerMap.
	if found {
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
