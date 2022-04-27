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
	"time"

	"github.com/nitin-deamon/azure-storage-azcopy/v10/common"
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
	sourceIndex *objectIndexer

	sourceFolderIndex *folderIndexer

	disableComparison bool
}

func newSyncDestinationComparator(i *folderIndexer, copyScheduler, cleaner objectProcessor, disableComparison bool) *syncDestinationComparator {
	return &syncDestinationComparator{sourceFolderIndex: i, copyTransferScheduler: copyScheduler, destinationCleaner: cleaner, disableComparison: disableComparison}
}

// it will only schedule transfers for destination objects that are present in the indexer but stale compared to the entry in the map
// if the destinationObject is not at the source, it will be passed to the destinationCleaner
// ex: we already know what the source contains, now we are looking at objects at the destination
// if file x from the destination exists at the source, then we'd only transfer it if it is considered stale compared to its counterpart at the source
// if file x does not exist at the source, then it is considered extra, and will be deleted
func (f *syncDestinationComparator) processIfNecessary(destinationObject StoredObject) error {
	var lcFolderName, lcFileName, lcRelativePath string
	var present bool
	var sourceObjectInMap StoredObject

	if f.sourceFolderIndex.isDestinationCaseInsensitive {
		lcRelativePath = strings.ToLower(destinationObject.relativePath)
	} else {
		lcRelativePath = destinationObject.relativePath
	}

	lcFolderName = filepath.Dir(destinationObject.relativePath)
	lcFileName = filepath.Base(destinationObject.relativePath)
	if destinationObject.isVirtualFolder || destinationObject.entityType == common.EEntityType.Folder() {
		lcFolderName = lcRelativePath
		lcFileName = ""
	}

	f.sourceFolderIndex.lock.Lock()

	if lcFolderName == "" {
		lcFolderName = "."
	}

	foldermap, folderPresent := f.sourceFolderIndex.folderMap[lcFolderName]

	// Do the auditing of map.
	defer func() {
		if present {
			delete(f.sourceFolderIndex.folderMap[lcFolderName].indexMap, lcFileName)
			f.sourceFolderIndex.counter--
		}
		if folderPresent {
			if len(f.sourceFolderIndex.folderMap[lcFolderName].indexMap) == 0 {
				delete(f.sourceFolderIndex.folderMap, lcFolderName)
			}
			f.sourceFolderIndex.counter--
		}
		f.sourceFolderIndex.lock.Unlock()
	}()

	// Folder Case
	if destinationObject.isVirtualFolder || destinationObject.entityType == common.EEntityType.Folder() {
		// Folder not present on target side.
		if (destinationObject.lastModifiedTime == time.Time{}) {
			// As source enumerate the folder and add files to map. After enumeration done, source queue folder information to target.
			// So in any case folder should be present in map.
			if !folderPresent {
				fmt.Printf("lcFolderName: %s, lcFileName: %s", lcFolderName, lcFileName)
				panic("Target Enumeration has directory which is not present in source.")
			}
			// We know this folder not exist on target, lets create job order for it.
			for ch := range foldermap.indexMap {
				f.copyTransferScheduler(foldermap.indexMap[ch])
				delete(foldermap.indexMap, ch)
			}

			f.sourceFolderIndex.counter = f.sourceFolderIndex.counter - int64(f.sourceFolderIndex.folderMap[lcFolderName].counter)

			return nil
		} else if !destinationObject.hasEntityUpdated {
			if !folderPresent {
				panic("Target Enumeration has directory which is not present in source.")
			}
			for ch := range foldermap.indexMap {
				fmt.Println("File: ", ch)
				if foldermap.indexMap[ch].hasEntityUpdated && foldermap.indexMap[ch].entityType == common.EEntityType.File() {
					f.copyTransferScheduler(foldermap.indexMap[ch])
				}
				delete(foldermap.indexMap, ch)
			}

			f.sourceFolderIndex.counter = f.sourceFolderIndex.counter - int64(f.sourceFolderIndex.folderMap[lcFolderName].counter)
			return nil
		} else {
			// Lets create folder on target side.
			if folderPresent {
				sourceObjectInMap = foldermap.folderObject
				if f.disableComparison || sourceObjectInMap.isMoreRecentThan(destinationObject) {
					err := f.copyTransferScheduler(sourceObjectInMap)
					if err != nil {
						return err
					}
				}
			} else {
				// Folder Deletion not enabled on azcopy. So its no-op only.
				_ = f.destinationCleaner(destinationObject)
			}
			return nil
		}
	}

	// File case.
	if folderPresent {
		sourceObjectInMap, present = foldermap.indexMap[lcFileName]

		// if the destinationObject is present at source and stale, we transfer the up-to-date version from source
		if present {
			if f.disableComparison || sourceObjectInMap.isMoreRecentThan(destinationObject) {
				err := f.copyTransferScheduler(sourceObjectInMap)
				if err != nil {
					return err
				}
			}
		} else {
			_ = f.destinationCleaner(destinationObject)
		}
	} else {
		_ = f.destinationCleaner(destinationObject)
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
	destinationIndex *folderIndexer

	disableComparison bool
}

func newSyncSourceComparator(i *folderIndexer, copyScheduler objectProcessor, disableComparison bool) *syncSourceComparator {
	return &syncSourceComparator{destinationIndex: i, copyTransferScheduler: copyScheduler, disableComparison: disableComparison}
}

// it will only transfer source items that are:
//	1. not present in the map
//  2. present but is more recent than the entry in the map
// note: we remove the StoredObject if it is present so that when we have finished
// the index will contain all objects which exist at the destination but were NOT seen at the source
func (f *syncSourceComparator) processIfNecessary(sourceObject StoredObject) error {
	relPath := sourceObject.relativePath

	if f.destinationIndex.isDestinationCaseInsensitive {
		relPath = strings.ToLower(relPath)
	}

	destinationObjectInMap, present := f.destinationIndex.folderMap[filepath.Dir(relPath)].indexMap[filepath.Base(relPath)]

	if present {
		defer delete(f.destinationIndex.folderMap[filepath.Dir(relPath)].indexMap, relPath)

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
