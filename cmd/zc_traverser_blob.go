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
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/nitin-deamon/azure-storage-azcopy/v10/common/parallel"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/pkg/errors"

	"github.com/nitin-deamon/azure-storage-azcopy/v10/common"
)

// allow us to iterate through a path pointing to the blob endpoint
type blobTraverser struct {
	rawURL    *url.URL
	p         pipeline.Pipeline
	ctx       context.Context
	recursive bool

	// parallel listing employs the hierarchical listing API which is more expensive
	// cx should have the option to disable this optimization in the name of saving costs
	parallelListing bool

	// whether to include blobs that have metadata 'hdi_isfolder = true'
	includeDirectoryStubs bool

	// a generic function to notify that a new stored object has been enumerated
	incrementEnumerationCounter enumerationCounterFunc

	s2sPreserveSourceTags bool

	cpkOptions common.CpkOptions

	// Fields applicable only to sync operation.
	isSync                   bool
	indexerMap               *folderIndexer
	tqueue                   chan interface{}
	isSource                 bool
	maxObjectIndexerSizeInGB uint
	lastSync                 time.Time
	cfdMode                  CFDModeFlags
}

func (t *blobTraverser) IsDirectory(isSource bool) bool {
	isDirDirect := copyHandlerUtil{}.urlIsContainerOrVirtualDirectory(t.rawURL)

	// Skip the single blob check if we're checking a destination.
	// This is an individual exception for blob because blob supports virtual directories and blobs sharing the same name.
	if isDirDirect || !isSource {
		return isDirDirect
	}

	_, isSingleBlob, _, err := t.getPropertiesIfSingleBlob()

	if stgErr, ok := err.(azblob.StorageError); ok {
		// We know for sure this is a single blob still, let it walk on through to the traverser.
		if stgErr.ServiceCode() == common.CPK_ERROR_SERVICE_CODE {
			return false
		}
	}

	return !isSingleBlob
}

func (t *blobTraverser) getPropertiesIfSingleBlob() (props *azblob.BlobGetPropertiesResponse, isBlob bool, isDirStub bool, err error) {
	// trim away the trailing slash before we check whether it's a single blob
	// so that we can detect the directory stub in case there is one
	blobUrlParts := azblob.NewBlobURLParts(*t.rawURL)
	blobUrlParts.BlobName = strings.TrimSuffix(blobUrlParts.BlobName, common.AZCOPY_PATH_SEPARATOR_STRING)

	// perform the check
	blobURL := azblob.NewBlobURL(blobUrlParts.URL(), t.p)
	clientProvidedKey := azblob.ClientProvidedKeyOptions{}
	if t.cpkOptions.IsSourceEncrypted {
		clientProvidedKey = common.GetClientProvidedKey(t.cpkOptions)
	}
	props, err = blobURL.GetProperties(t.ctx, azblob.BlobAccessConditions{}, clientProvidedKey)

	// if there was no problem getting the properties, it means that we are looking at a single blob
	if err == nil {
		if gCopyUtil.doesBlobRepresentAFolder(props.NewMetadata()) {
			return props, false, true, nil
		}

		return props, true, false, err
	}

	return nil, false, false, err
}

func (t *blobTraverser) getBlobTags() (common.BlobTags, error) {
	blobUrlParts := azblob.NewBlobURLParts(*t.rawURL)
	blobUrlParts.BlobName = strings.TrimSuffix(blobUrlParts.BlobName, common.AZCOPY_PATH_SEPARATOR_STRING)

	// perform the check
	blobURL := azblob.NewBlobURL(blobUrlParts.URL(), t.p)
	blobTagsMap := make(common.BlobTags)
	blobGetTagsResp, err := blobURL.GetTags(t.ctx, nil)
	if err != nil {
		return blobTagsMap, err
	}

	for _, blobTag := range blobGetTagsResp.BlobTagSet {
		blobTagsMap[url.QueryEscape(blobTag.Key)] = url.QueryEscape(blobTag.Value)
	}
	return blobTagsMap, nil
}

func (t *blobTraverser) Traverse(preprocessor objectMorpher, processor objectProcessor, filters []ObjectFilter) (err error) {
	blobUrlParts := azblob.NewBlobURLParts(*t.rawURL)

	// check if the url points to a single blob
	blobProperties, isBlob, isDirStub, propErr := t.getPropertiesIfSingleBlob()

	if stgErr, ok := propErr.(azblob.StorageError); ok {
		// Don't error out unless it's a CPK error just yet
		// If it's a CPK error, we know it's a single blob and that we can't get the properties on it anyway.
		if stgErr.ServiceCode() == common.CPK_ERROR_SERVICE_CODE {
			return errors.New("this blob uses customer provided encryption keys (CPK). At the moment, AzCopy does not support CPK-encrypted blobs. " +
				"If you wish to make use of this blob, we recommend using one of the Azure Storage SDKs")
		}
	}

	// schedule the blob in two cases:
	// 	1. either we are targeting a single blob and the URL wasn't explicitly pointed to a virtual dir
	//	2. either we are scanning recursively with includeDirectoryStubs set to true,
	//	   then we add the stub blob that represents the directory
	if (isBlob && !strings.HasSuffix(blobUrlParts.BlobName, common.AZCOPY_PATH_SEPARATOR_STRING)) ||
		(t.includeDirectoryStubs && isDirStub && t.recursive) {
		// sanity checking so highlighting doesn't highlight things we're not worried about.
		if blobProperties == nil {
			panic("isBlob should never be set if getting properties is an error")
		}

		if azcopyScanningLogger != nil {
			azcopyScanningLogger.Log(pipeline.LogDebug, "Detected the root as a blob.")
		}

		storedObject := newStoredObject(
			preprocessor,
			getObjectNameOnly(strings.TrimSuffix(blobUrlParts.BlobName, common.AZCOPY_PATH_SEPARATOR_STRING)),
			"",
			common.EEntityType.File(),
			blobProperties.LastModified(),
			blobProperties.ContentLength(),
			blobProperties,
			blobPropertiesResponseAdapter{blobProperties},
			common.FromAzBlobMetadataToCommonMetadata(blobProperties.NewMetadata()), // .NewMetadata() seems odd to call, but it does actually retrieve the metadata from the blob properties.
			blobUrlParts.ContainerName,
		)

		if t.s2sPreserveSourceTags {
			blobTagsMap, err := t.getBlobTags()
			if err != nil {
				panic("Couldn't fetch blob tags due to error: " + err.Error())
			}
			if len(blobTagsMap) > 0 {
				storedObject.blobTags = blobTagsMap
			}
		}

		if t.incrementEnumerationCounter != nil {
			t.incrementEnumerationCounter(common.EEntityType.File())
		}

		err := processIfPassedFilters(filters, storedObject, processor)
		_, err = getProcessingError(err)

		// short-circuit if we don't have anything else to scan
		if isBlob || err != nil {
			fmt.Println("Returned error")
			return err
		}
	}

	// get the container URL so that we can list the blobs
	containerRawURL := copyHandlerUtil{}.getContainerUrl(blobUrlParts)
	containerURL := azblob.NewContainerURL(containerRawURL, t.p)

	// get the search prefix to aid in the listing
	// example: for a url like https://test.blob.core.windows.net/test/foo/bar/bla
	// the search prefix would be foo/bar/bla
	searchPrefix := blobUrlParts.BlobName

	// append a slash if it is not already present
	// example: foo/bar/bla becomes foo/bar/bla/ so that we only list children of the virtual directory
	if searchPrefix != "" && !strings.HasSuffix(searchPrefix, common.AZCOPY_PATH_SEPARATOR_STRING) {
		searchPrefix += common.AZCOPY_PATH_SEPARATOR_STRING
	}

	// as a performance optimization, get an extra prefix to do pre-filtering. It's typically the start portion of a blob name.
	extraSearchPrefix := FilterSet(filters).GetEnumerationPreFilter(t.recursive)

	if t.parallelListing {
		return t.parallelList(containerURL, blobUrlParts.ContainerName, searchPrefix, extraSearchPrefix, preprocessor, processor, filters)
	}

	return t.serialList(containerURL, blobUrlParts.ContainerName, searchPrefix, extraSearchPrefix, preprocessor, processor, filters)
}

// HasDirectoryChangedSinceLastSync checks directory changed since lastSyncTime.
// It honours CFDMode to make the decision, f.e., if CFDMode allows ctime/mtime to be used for CFD it may not need to query
// attributes from target. If it returns True, TargetTraverser will enumerate the directory and compare each enumerated object
// with the source scanned objects in ObjectIndexer[] to find out if the object needs to be sync'ed.
//
// Note: For CFDModes that allow ctime for CFD we can avoid enumerating target dir if we know directory has not changed.
//       This increases sync efficiency.
//
// Note: If we discover that certain sources cannot be safely trusted for ctime update we can change this to return True for them,
//       thus falling back on the more rigorous target<->source comparison.
func (t *blobTraverser) HasDirectoryChangedSinceLastSync(so StoredObject) bool {
	// CFDMode is to enumerate Target and compare. So we return true in that case.
	// This will lead to enumeration of target.
	if t.cfdMode.TargetCompare {
		return true
	}

	if t.cfdMode.Ctime || t.cfdMode.CtimeMtime {
		if so.lastChangeTime.After(t.lastSync) {
			return true
		}
		return false
	} else if t.cfdMode.archiveBit {
		// TODO: Add the code to get the attributes of dir and check with the last sync time.
		if !so.archiveBit {
			return true
		}
		return false
	} else {
		// FallBack to default case to enumerate whole target.
		return true
	}
}

func (t *blobTraverser) parallelList(containerURL azblob.ContainerURL, containerName string, searchPrefix string,
	extraSearchPrefix string, preprocessor objectMorpher, processor objectProcessor, filters []ObjectFilter) error {
	// Define how to enumerate its contents
	// This func must be thread safe/goroutine safe
	enumerateOneDir := func(dir parallel.Directory, enqueueDir func(parallel.Directory), enqueueOutput func(parallel.DirectoryEntry, error)) error {
		if _, ok := dir.(string); !ok {
			panic("Entry in queue is not string")
		}

		so := t.indexerMap.getStoredObject(dir.(string))
		currentDirPath := so.relativePath

		// This is applicable for only sync mode as source local give path without forward slash.
		// TODO: Need to modify the source(Local) traverser to add forward slash.
		if t.tqueue != nil && !t.isSource && currentDirPath != "" {
			currentDirPath = currentDirPath + "/"
		}

		// Check the directory if we need enumeration at target side or not.
		if t.tqueue != nil && !t.isSource && !t.HasDirectoryChangedSinceLastSync(so) {
			goto FinalizeDirectory
		}

		for marker := (azblob.Marker{}); marker.NotDone(); {
			lResp, err := containerURL.ListBlobsHierarchySegment(t.ctx, marker, "/", azblob.ListBlobsSegmentOptions{Prefix: currentDirPath,
				Details: azblob.BlobListingDetails{Metadata: true, Tags: t.s2sPreserveSourceTags}})
			if err != nil {
				return fmt.Errorf("cannot list files due to reason %s", err)
			}

			// queue up the sub virtual directories if recursive is true
			if t.recursive {
				for _, virtualDir := range lResp.Segment.BlobPrefixes {
					enqueueDir(virtualDir.Name)

					if t.includeDirectoryStubs {
						// try to get properties on the directory itself, since it's not listed in BlobItems
						// TODO: This logic need to be changed/revisit for deletion of folder on target which not present on source. As of now GetProperties failed everytime.
						//       Which mean in case of blob target directory not enqueue to comparator.
						fblobURL := containerURL.NewBlobURL(strings.TrimSuffix(currentDirPath+virtualDir.Name, common.AZCOPY_PATH_SEPARATOR_STRING))
						resp, err := fblobURL.GetProperties(t.ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
						if err == nil {
							storedObject := newStoredObject(
								preprocessor,
								getObjectNameOnly(strings.TrimSuffix(currentDirPath+virtualDir.Name, common.AZCOPY_PATH_SEPARATOR_STRING)),
								strings.TrimSuffix(currentDirPath+virtualDir.Name, common.AZCOPY_PATH_SEPARATOR_STRING),
								common.EEntityType.File(), // folder stubs are treated like files in in the serial lister as well
								resp.LastModified(),
								resp.ContentLength(),
								resp,
								blobPropertiesResponseAdapter{resp},
								common.FromAzBlobMetadataToCommonMetadata(resp.NewMetadata()),
								containerName,
							)
							storedObject.isVirtualFolder = true
							if t.s2sPreserveSourceTags {
								var BlobTags *azblob.BlobTags
								BlobTags, err = fblobURL.GetTags(t.ctx, nil)

								if err == nil {
									blobTagsMap := common.BlobTags{}
									for _, blobTag := range BlobTags.BlobTagSet {
										blobTagsMap[url.QueryEscape(blobTag.Key)] = url.QueryEscape(blobTag.Value)
									}
									storedObject.blobTags = blobTagsMap
								}
							}

							enqueueOutput(storedObject, err)
						}
					}
				}
			}

			// process the blobs returned in this result segment
			for _, blobInfo := range lResp.Segment.BlobItems {

				// if the blob represents a hdi folder, then skip it
				if t.doesBlobRepresentAFolder(blobInfo.Metadata) {
					continue
				}

				storedObject := t.createStoredObjectForBlob(preprocessor, blobInfo, strings.TrimPrefix(blobInfo.Name, searchPrefix), containerName)

				if t.s2sPreserveSourceTags && blobInfo.BlobTags != nil {
					blobTagsMap := common.BlobTags{}
					for _, blobTag := range blobInfo.BlobTags.BlobTagSet {
						blobTagsMap[url.QueryEscape(blobTag.Key)] = url.QueryEscape(blobTag.Value)
					}
					storedObject.blobTags = blobTagsMap
				}

				enqueueOutput(storedObject, nil)
			}

			// if debug mode is on, note down the result, this is not going to be fast
			if azcopyScanningLogger != nil && azcopyScanningLogger.ShouldLog(pipeline.LogDebug) {
				tokenValue := "NONE"
				if marker.Val != nil {
					tokenValue = *marker.Val
				}

				var vdirListBuilder strings.Builder
				for _, virtualDir := range lResp.Segment.BlobPrefixes {
					fmt.Fprintf(&vdirListBuilder, " %s,", virtualDir.Name)
				}
				var fileListBuilder strings.Builder
				for _, blobInfo := range lResp.Segment.BlobItems {
					fmt.Fprintf(&fileListBuilder, " %s,", blobInfo.Name)
				}
				msg := fmt.Sprintf("Enumerating %s with token %s. Sub-dirs:%s Files:%s", currentDirPath,
					tokenValue, vdirListBuilder.String(), fileListBuilder.String())
				azcopyScanningLogger.Log(pipeline.LogDebug, msg)
			}

			marker = lResp.NextMarker
		}

	FinalizeDirectory:
		// This storedObject marks the end of folder enumeration.
		storedObject := StoredObject{
			name:              getObjectNameOnly(strings.TrimSuffix(currentDirPath, common.AZCOPY_PATH_SEPARATOR_STRING)),
			relativePath:      strings.TrimSuffix(currentDirPath, common.AZCOPY_PATH_SEPARATOR_STRING),
			entityType:        common.EEntityType.File(), // folder stubs are treated like files in in the serial lister as well
			lastModifiedTime:  time.Time{},
			size:              0,
			ContainerName:     containerName,
			isVirtualFolder:   true,
			isFolderEndMarker: true,
		}
		enqueueOutput(storedObject, nil)
		return nil
	}

	//
	// This is used by the source traverser to find the indexer map size.
	// It uses it to induce a wait before starting enumeration of a new directory, to keep indexer map size in check.
	//
	getIndexerMapSize := func() int64 {
		if t.isSource && t.tqueue != nil {
			if t.indexerMap == nil {
				panic("Source traverser must have indexerMap set!")

			}
			return t.indexerMap.getIndexerMapSize()
		} else {
			return -1
		}
	}

	// initiate parallel scanning, starting at the root path
	workerContext, cancelWorkers := context.WithCancel(t.ctx)
	cCrawled := parallel.Crawl(workerContext, searchPrefix+extraSearchPrefix, enumerateOneDir, EnumerationParallelism, getIndexerMapSize, t.tqueue, t.isSource, t.isSync, t.maxObjectIndexerSizeInGB)

	for x := range cCrawled {
		item, workerError := x.Item()
		if workerError != nil {
			cancelWorkers()
			return workerError
		}

		if t.incrementEnumerationCounter != nil {
			t.incrementEnumerationCounter(common.EEntityType.File())
		}

		object := item.(StoredObject)

		processErr := processIfPassedFilters(filters, object, processor)
		_, processErr = getProcessingError(processErr)
		if processErr != nil {
			cancelWorkers()
			return processErr
		}
	}

	return nil
}

func (t *blobTraverser) createStoredObjectForBlob(preprocessor objectMorpher, blobInfo azblob.BlobItemInternal, relativePath string, containerName string) StoredObject {
	adapter := blobPropertiesAdapter{blobInfo.Properties}
	return newStoredObject(
		preprocessor,
		getObjectNameOnly(blobInfo.Name),
		relativePath,
		common.EEntityType.File(),
		blobInfo.Properties.LastModified,
		*blobInfo.Properties.ContentLength,
		adapter,
		adapter, // adapter satisfies both interfaces
		common.FromAzBlobMetadataToCommonMetadata(blobInfo.Metadata),
		containerName,
	)
}

func (t *blobTraverser) doesBlobRepresentAFolder(metadata azblob.Metadata) bool {
	util := copyHandlerUtil{}
	return util.doesBlobRepresentAFolder(metadata) && !(t.includeDirectoryStubs && t.recursive)
}

func (t *blobTraverser) serialList(containerURL azblob.ContainerURL, containerName string, searchPrefix string,
	extraSearchPrefix string, preprocessor objectMorpher, processor objectProcessor, filters []ObjectFilter) error {

	for marker := (azblob.Marker{}); marker.NotDone(); {
		// see the TO DO in GetEnumerationPreFilter if/when we make this more directory-aware

		// look for all blobs that start with the prefix
		// Passing tags = true in the list call will save additional GetTags call
		// TODO optimize for the case where recursive is off
		listBlob, err := containerURL.ListBlobsFlatSegment(t.ctx, marker,
			azblob.ListBlobsSegmentOptions{Prefix: searchPrefix + extraSearchPrefix, Details: azblob.BlobListingDetails{Metadata: true, Tags: t.s2sPreserveSourceTags}})
		if err != nil {
			return fmt.Errorf("cannot list blobs. Failed with error %s", err.Error())
		}

		// process the blobs returned in this result segment
		for _, blobInfo := range listBlob.Segment.BlobItems {
			// if the blob represents a hdi folder, then skip it
			if t.doesBlobRepresentAFolder(blobInfo.Metadata) {
				continue
			}

			relativePath := strings.TrimPrefix(blobInfo.Name, searchPrefix)
			// if recursive
			if !t.recursive && strings.Contains(relativePath, common.AZCOPY_PATH_SEPARATOR_STRING) {
				continue
			}

			storedObject := t.createStoredObjectForBlob(preprocessor, blobInfo, relativePath, containerName)

			// Setting blob tags
			if t.s2sPreserveSourceTags && blobInfo.BlobTags != nil {
				blobTagsMap := common.BlobTags{}
				for _, blobTag := range blobInfo.BlobTags.BlobTagSet {
					blobTagsMap[url.QueryEscape(blobTag.Key)] = url.QueryEscape(blobTag.Value)
				}
				storedObject.blobTags = blobTagsMap
			}

			if t.incrementEnumerationCounter != nil {
				t.incrementEnumerationCounter(common.EEntityType.File())
			}

			processErr := processIfPassedFilters(filters, storedObject, processor)
			_, processErr = getProcessingError(processErr)
			if processErr != nil {
				return processErr
			}
		}

		marker = listBlob.NextMarker
	}

	return nil
}

func newBlobTraverser(rawURL *url.URL, p pipeline.Pipeline, ctx context.Context, recursive, includeDirectoryStubs bool, incrementEnumerationCounter enumerationCounterFunc,
	s2sPreserveSourceTags bool, cpkOptions common.CpkOptions, indexerMap *folderIndexer, tqueue chan interface{}, isSource bool, isSync bool, maxObjectIndexerSizeInGB uint, lastSync time.Time,
	cfdMode CFDModeFlags) (t *blobTraverser) {
	t = &blobTraverser{
		rawURL:                      rawURL,
		p:                           p,
		ctx:                         ctx,
		recursive:                   recursive,
		includeDirectoryStubs:       includeDirectoryStubs,
		incrementEnumerationCounter: incrementEnumerationCounter,
		parallelListing:             true,
		s2sPreserveSourceTags:       s2sPreserveSourceTags,
		cpkOptions:                  cpkOptions,
		indexerMap:                  indexerMap,
		isSync:                      isSync,
		tqueue:                      tqueue,
		isSource:                    isSource,
		maxObjectIndexerSizeInGB:    maxObjectIndexerSizeInGB,
		lastSync:                    lastSync,
		cfdMode:                     cfdMode,
	}

	if strings.ToLower(glcm.GetEnvironmentVariable(common.EEnvironmentVariable.DisableHierarchicalScanning())) == "true" {
		// TODO log to frontend log that parallel listing was disabled, once the frontend log PR is merged
		t.parallelListing = false
	}
	return
}
