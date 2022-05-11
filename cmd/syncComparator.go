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
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"
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

	CFDMode CFDModeFlags

	lastSync time.Time
}

func newSyncDestinationComparator(i *folderIndexer, copyScheduler, cleaner objectProcessor, disableComparison bool, cfdMode CFDModeFlags, lastSyncTime time.Time) *syncDestinationComparator {
	return &syncDestinationComparator{sourceFolderIndex: i, copyTransferScheduler: copyScheduler, destinationCleaner: cleaner, disableComparison: disableComparison,
		CFDMode:  cfdMode,
		lastSync: lastSyncTime}
}

// HasFileChangedSinceLastSyncUsingLocalChecks depending on mode returns dataChanged and metadataChanged.
// Given a file and the corresponding scanned source object, find out if we need to copy data, metadata, both, none.
// This is called by TargetTraverser. It honours various sync qualifiers to make the decision, f.e., if sync
// qualifiers allow ctime/mtime to be used for CFD it may not need to query file attributes from target.
//
// Note: Caller will use the returned information to decide whether to copy the storedObject to target and whether to
// 		 copy only data, only metadata or both.
//
// Note: This SHOULD NOT be called for children of "changed" directories, since for changed directories we cannot safely
// 	     check for changed files purely by doing time based comparison. Use HasFileChangedSinceLastSyncUsingTargetCompare()
// 		 for children of changed directories.
func (f *syncDestinationComparator) HasFileChangedSinceLastSyncUsingLocalChecks(so StoredObject, filePath string) (bool, bool) {
	// Changed file detection using Ctime and Mtime.
	if f.CFDMode.CtimeMtime {
		// File Mtime changed, which means data changed and it cause metadata change.
		if so.lastModifiedTime.After(f.lastSync) {
			return true, true
		} else if so.lastChangeTime.After(f.lastSync) {
			// File Ctime changed only, only meta data changed.
			return false, true
		}
		// File not changed at all.
		return false, false
	} else if f.CFDMode.Ctime {
		// Changed file detection using Ctime only.

		// File changed since lastSync time. CFDMode is Ctime, so we can't rely on mtime as it can be modified by any other tool.
		if so.lastChangeTime.After(f.lastSync) {
			// If MetaDataSync Flag is false we don't need to check for data or metadata change. We can return true in that case.
			if !f.CFDMode.MetaDataOnlySync {
				return true, true
			} else {
				// TODO: If MetaDataSync Flag is true, we need to check for data or metadata change.
				// 		 We need to get file properties to know what changed.
				return true, true
			}
		} else {
			// File Ctime not changed, means no data or metadata changed.
			return false, false
		}
	} else if f.CFDMode.archiveBit {
		if so.archiveBit {
			return false, false
		} else {
			// File modified case.

			// If MetaDataSync Flag is false we don't need to check for data or metadata change. We can return true in that case.
			if !f.CFDMode.MetaDataOnlySync {
				return true, true
			} else {
				// TODO: If MetaDataSync Flag is true, we need to check for data or metadata change.
				// 		 We need to get file properties to know what changed.
				return true, true
			}
		}
	} else {

	}
	return true, true
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

	lcFolderName = filepath.Dir(lcRelativePath)
	lcFileName = filepath.Base(lcRelativePath)

	f.sourceFolderIndex.lock.Lock()

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

	// Folder Case.
	if destinationObject.isVirtualFolder || destinationObject.entityType == common.EEntityType.Folder() {

		if destinationObject.isFolderEndMarker {
			var size int64

			// Each folder storedObject stored in its parent directory, one exception to this is root folder.
			// Which is stored in root folder only as "." filename. End marker come for folder present on source.
			// NOTE: Following are the scenarios for end marker.
			//       1. End Marker came for folder which is not present on target.
			//       2. End Marker came for folder present on target.

			// This will happen if folder not present on target. Lets create folder first and then files.
			// In case folder present then it will taken in else case.
			if folderPresent {
				storedObject := foldermap.indexMap[lcFileName]
				size += storedObjectSize(storedObject)
				delete(foldermap.indexMap, lcFileName)
				metaChange, dataChange := f.HasFileChangedSinceLastSyncUsingLocalChecks(storedObject, storedObject.relativePath)
				if dataChange {
					f.copyTransferScheduler(storedObject)
				}
				if metaChange {
					// TODO: Add calls to just update meta data of file.
				}

				// Delete the parent map of folder if it's empty.
				if len(f.sourceFolderIndex.folderMap[lcFolderName].indexMap) == 0 {
					delete(f.sourceFolderIndex.folderMap, lcFolderName)
				}
			}

			lcFolderName = path.Join(lcFolderName, lcFileName)
			foldermap, folderPresent = f.sourceFolderIndex.folderMap[lcFolderName]
			// lets copy all the files underneath in this folder which are left out.
			// It may happen all the files present on target then there is nothing left in map, otherwise left files will be taken care.
			if folderPresent {
				for file := range foldermap.indexMap {
					storedObject := foldermap.indexMap[file]
					size += storedObjectSize(storedObject)
					delete(foldermap.indexMap, file)

					metaChange, dataChange := f.HasFileChangedSinceLastSyncUsingLocalChecks(storedObject, storedObject.relativePath)
					if dataChange {
						f.copyTransferScheduler(storedObject)
					}
					if metaChange {
						// TODO: Add calls to just update meta data of file.
					}
				}
			}
			size = -size
			atomic.AddInt64(&f.sourceFolderIndex.totalSize, size)

			if atomic.LoadInt64(&f.sourceFolderIndex.totalSize) < 0 {
				panic("Total Size is negative.")
			}
			return nil
		} else {
			// Folder present on source and its present on target too.
			if folderPresent {
				sourceObjectInMap = foldermap.indexMap[lcFileName]
				delete(foldermap.indexMap, lcFileName)
				size := storedObjectSize(sourceObjectInMap)
				size = -size
				atomic.AddInt64(&f.sourceFolderIndex.totalSize, size)
				if sourceObjectInMap.relativePath != destinationObject.relativePath {
					panic("Relative Path not matched")
				}
				if f.disableComparison || sourceObjectInMap.isMoreRecentThan(destinationObject) {
					err := f.copyTransferScheduler(sourceObjectInMap)
					if err != nil {
						return err
					}
				}
			} else {
				// We detect folder not present on source, now we need to delete the folder and files underneath.
				// TODO: Need to add call to delete the folder.
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
			size := storedObjectSize(sourceObjectInMap)
			size = -size
			atomic.AddInt64(&f.sourceFolderIndex.totalSize, size)
			if sourceObjectInMap.relativePath != destinationObject.relativePath {
				panic("Relative Path not matched")
			}
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
		// TODO: Need to add the delete code which distinquish between blob and file.
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
