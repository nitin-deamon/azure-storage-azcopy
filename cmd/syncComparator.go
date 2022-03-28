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
)

// with the help of an objectIndexer containing the source objects
// find out the destination objects that should be transferred
// in other words, this should be used when destination is being enumerated secondly
type syncDestinationComparator struct {
	// the rejected objects would be passed to the destinationCleaner
	destinationCleaner objectProcessor

	// the processor responsible for scheduling copy transfers
	copyTransferScheduler objectProcessor

	// storing the source objects
	sourceIndex *folderObjectIndexer

	disableComparison bool

	sourceDone bool

	destinationDone bool
}

func newSyncDestinationComparator(i *folderObjectIndexer, copyScheduler, cleaner objectProcessor, disableComparison bool) *syncDestinationComparator {
	return &syncDestinationComparator{sourceIndex: i, copyTransferScheduler: copyScheduler, destinationCleaner: cleaner, disableComparison: disableComparison}
}

// it will only schedule transfers for destination objects that are present in the indexer but stale compared to the entry in the map
// if the destinationObject is not at the source, it will be passed to the destinationCleaner
// ex: we already know what the source contains, now we are looking at objects at the destination
// if file x from the destination exists at the source, then we'd only transfer it if it is considered stale compared to its counterpart at the source
// if file x does not exist at the source, then it is considered extra, and will be deleted
func (f *syncDestinationComparator) processIfNecessary(destinationObject StoredObject, isSource bool) error {
	var present bool
	var sourceObjectInMap StoredObject
	var lcFolderPath string
	var lcRelativePath string

	if f.sourceIndex.isDestinationCaseInsensitive {
		lcRelativePath = strings.ToLower(destinationObject.relativePath)
		lcFolderPath = filepath.Dir(lcRelativePath)
		if _, ok := f.sourceIndex.srcFolderMap[lcFolderPath]; ok {
			sourceObjectInMap, present = f.sourceIndex.srcFolderMap[lcFolderPath].indexMap[filepath.Base(lcRelativePath)]
		}
	} else {
		lcRelativePath = destinationObject.relativePath
		lcFolderPath = filepath.Dir(destinationObject.relativePath)
		if _, ok := f.sourceIndex.srcFolderMap[lcFolderPath]; ok {
			sourceObjectInMap, present = f.sourceIndex.srcFolderMap[lcFolderPath].indexMap[filepath.Base(destinationObject.relativePath)]
		}
	}

	// if the destinationObject is present at source and stale, we transfer the up-to-date version from source
	if present {
		defer delete(f.sourceIndex.srcFolderMap[lcFolderPath].indexMap, filepath.Base(destinationObject.relativePath))
		if !isSource {
			if f.disableComparison || sourceObjectInMap.isMoreRecentThan(destinationObject) {
				fmt.Printf("destinaObject: %+v", destinationObject)
				err := f.copyTransferScheduler(sourceObjectInMap)
				if err != nil {
					return err
				}
			}
		} else {
			if f.disableComparison || destinationObject.isMoreRecentThan(sourceObjectInMap) {
				fmt.Printf("destinaObject: %+v", sourceObjectInMap)
				err := f.copyTransferScheduler(sourceObjectInMap)
				if err != nil {
					return err
				}
			}
		}
	}

	if !present && !f.destinationDone {
		if _, ok := f.sourceIndex.srcFolderMap[lcFolderPath]; !ok {
			f.sourceIndex.srcFolderMap[lcFolderPath] = *newObjectIndexer()
		}
		destinationObject.isSource = isSource
		f.sourceIndex.srcFolderMap[lcFolderPath].indexMap[filepath.Base(lcRelativePath)] = destinationObject
	}

	if f.destinationDone {
		err := f.copyTransferScheduler(sourceObjectInMap)
		if err != nil {
			return err
		}
	}
	return nil
}

// with the help of an objectIndexer containing the destination objects
// filter out the source objects that should be transferred
// in other words, this should be used when source is being enumerated secondly
type syncSourceComparator struct {
	// the processor responsible for scheduling copy transfers
	copyTransferScheduler objectProcessor

	// storing the destination objects
	destinationIndex *folderObjectIndexer

	disableComparison bool
}

func newSyncSourceComparator(i *folderObjectIndexer, copyScheduler objectProcessor, disableComparison bool) *syncSourceComparator {
	return &syncSourceComparator{destinationIndex: i, copyTransferScheduler: copyScheduler, disableComparison: disableComparison}
}

// it will only transfer source items that are:
//	1. not present in the map
//  2. present but is more recent than the entry in the map
// note: we remove the StoredObject if it is present so that when we have finished
// the index will contain all objects which exist at the destination but were NOT seen at the source
func (f *syncSourceComparator) processIfNecessary(sourceObject StoredObject, isSource bool) error {
	relPath := sourceObject.relativePath

	if f.destinationIndex.isDestinationCaseInsensitive {
		relPath = strings.ToLower(relPath)
	}

	destinationObjectInMap, present := f.destinationIndex.srcFolderMap[filepath.Dir(relPath)].indexMap[filepath.Base(relPath)]

	if present {
		defer delete(f.destinationIndex.srcFolderMap[filepath.Dir(relPath)].indexMap, filepath.Base(relPath))

		// if destination is stale, schedule source for transfer
		if f.disableComparison || sourceObject.isMoreRecentThan(destinationObjectInMap) {
			return f.copyTransferScheduler(sourceObject)
		}
		// skip if source is more recent
		return nil
	}

	// if source does not exist at the destination, then schedule it for transfer
	return f.copyTransferScheduler(sourceObject)
}
