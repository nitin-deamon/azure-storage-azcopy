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

package ste

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/nitin-deamon/azure-storage-azcopy/v10/azbfs"
	"github.com/Azure/azure-storage-blob-go/azblob"

	"github.com/nitin-deamon/azure-storage-azcopy/v10/common"
)

var lowMemoryLimitAdvice sync.Once

type blockBlobSenderBase struct {
	jptm             IJobPartTransferMgr
	sip              ISourceInfoProvider
	destBlockBlobURL azblob.BlockBlobURL
	chunkSize        int64
	numChunks        uint32
	pacer            pacer
	blockIDs         []string
	destBlobTier     azblob.AccessTierType

	// Headers and other info that we will apply to the destination object.
	// 1. For S2S, these come from the source service.
	// 2. When sending local data, they are computed based on the properties of the local file
	headersToApply  azblob.BlobHTTPHeaders
	metadataToApply azblob.Metadata
	blobTagsToApply azblob.BlobTagsMap
	cpkToApply      azblob.ClientProvidedKeyOptions

	atomicChunksWritten    int32
	atomicPutListIndicator int32
	muBlockIDs             *sync.Mutex
}

func getVerifiedChunkParams(transferInfo TransferInfo, memLimit int64) (chunkSize int64, numChunks uint32, err error) {
	chunkSize = transferInfo.BlockSize
	srcSize := transferInfo.SourceSize
	numChunks = getNumChunks(srcSize, chunkSize)

	toGiB := func(bytes int64) float64 {
		return float64(bytes) / float64(1024*1024*1024)
	}

	if common.MinParallelChunkCountThreshold >= memLimit/chunkSize {
		glcm := common.GetLifecycleMgr()
		msg := fmt.Sprintf("Using a blocksize of %.2fGiB for file %s. AzCopy is limited to use %.2fGiB of memory."+
			"Consider providing at least %.2fGiB to AzCopy, using environment variable %s.",
			toGiB(chunkSize), transferInfo.Source, toGiB(memLimit),
			toGiB(common.MinParallelChunkCountThreshold*chunkSize),
			common.EEnvironmentVariable.BufferGB().Name)

		lowMemoryLimitAdvice.Do(func() { glcm.Info(msg) })
	}

	if chunkSize >= memLimit {
		err = fmt.Errorf("Cannot use a block size of %.2fGiB. AzCopy is limited to use only %.2fGiB of memory",
			toGiB(chunkSize), toGiB(memLimit))
		return
	}

	if chunkSize > common.MaxBlockBlobBlockSize {
		// mercy, please
		err = fmt.Errorf("block size of %.2fGiB for file %s of size %.2fGiB exceeds maxmimum allowed block size for a BlockBlob",
			toGiB(chunkSize), transferInfo.Source, toGiB(transferInfo.SourceSize))
		return
	}

	if numChunks > common.MaxNumberOfBlocksPerBlob {
		err = fmt.Errorf("Block size %d for source of size %d is not correct. Number of blocks will exceed the limit", chunkSize, srcSize)
		return
	}

	return
}

func newBlockBlobSenderBase(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, pacer pacer, srcInfoProvider ISourceInfoProvider, inferredAccessTierType azblob.AccessTierType) (*blockBlobSenderBase, error) {
	// compute chunk count
	chunkSize, numChunks, err := getVerifiedChunkParams(jptm.Info(), jptm.CacheLimiter().Limit())
	if err != nil {
		return nil, err
	}

	destURL, err := url.Parse(destination)
	if err != nil {
		return nil, err
	}

	destBlockBlobURL := azblob.NewBlockBlobURL(*destURL, p)

	props, err := srcInfoProvider.Properties()
	if err != nil {
		return nil, err
	}

	// If user set blob tier explicitly, override any value that our caller
	// may have guessed.
	destBlobTier := inferredAccessTierType
	blockBlobTierOverride, _ := jptm.BlobTiers()
	if blockBlobTierOverride != common.EBlockBlobTier.None() {
		destBlobTier = blockBlobTierOverride.ToAccessTierType()
	}

	if props.SrcMetadata["hdi_isfolder"] == "true" {
		destBlobTier = azblob.AccessTierNone
	}

	// Once track2 goes live, we'll not need to do this conversion/casting and can directly use CpkInfo & CpkScopeInfo
	cpkToApply := common.ToClientProvidedKeyOptions(jptm.CpkInfo(), jptm.CpkScopeInfo())

	return &blockBlobSenderBase{
		jptm:             jptm,
		sip:              srcInfoProvider,
		destBlockBlobURL: destBlockBlobURL,
		chunkSize:        chunkSize,
		numChunks:        numChunks,
		pacer:            pacer,
		blockIDs:         make([]string, numChunks),
		headersToApply:   props.SrcHTTPHeaders.ToAzBlobHTTPHeaders(),
		metadataToApply:  props.SrcMetadata.ToAzBlobMetadata(),
		blobTagsToApply:  props.SrcBlobTags.ToAzBlobTagsMap(),
		destBlobTier:     destBlobTier,
		cpkToApply:       cpkToApply,
		muBlockIDs:       &sync.Mutex{}}, nil
}

func (s *blockBlobSenderBase) SendableEntityType() common.EntityType {
	return common.EEntityType.File()
}

func (s *blockBlobSenderBase) ChunkSize() int64 {
	return s.chunkSize
}

func (s *blockBlobSenderBase) NumChunks() uint32 {
	return s.numChunks
}

func (s *blockBlobSenderBase) RemoteFileExists() (bool, time.Time, error) {
	return remoteObjectExists(s.destBlockBlobURL.GetProperties(s.jptm.Context(), azblob.BlobAccessConditions{}, s.cpkToApply))
}

func (s *blockBlobSenderBase) Prologue(ps common.PrologueState) (destinationModified bool) {
	if s.jptm.ShouldInferContentType() {
		s.headersToApply.ContentType = ps.GetInferredContentType(s.jptm)
	}
	return false
}

func (s *blockBlobSenderBase) Epilogue() {
	jptm := s.jptm

	s.muBlockIDs.Lock()
	blockIDs := s.blockIDs
	s.blockIDs = nil // so we know for sure that only this routine has access after we release the lock (nothing else should need it now, since we're in the epilogue. Nil-ing here is just being defensive)
	s.muBlockIDs.Unlock()
	shouldPutBlockList := getPutListNeed(&s.atomicPutListIndicator)
	if shouldPutBlockList == putListNeedUnknown && !jptm.WasCanceled() {
		panic(errors.New("'put list' need flag was never set"))
	}
	// TODO: finalize and wrap in functions whether 0 is included or excluded in status comparisons

	// commit block list if necessary
	if jptm.IsLive() && shouldPutBlockList == putListNeeded {
		jptm.Log(pipeline.LogDebug, fmt.Sprintf("Conclude Transfer with BlockList %s", blockIDs))

		// commit the blocks.
		if !ValidateTier(jptm, s.destBlobTier, s.destBlockBlobURL.BlobURL, s.jptm.Context()) {
			s.destBlobTier = azblob.DefaultAccessTier
		}

		blobTags := s.blobTagsToApply
		separateSetTagsRequired := separateSetTagsRequired(blobTags)
		if separateSetTagsRequired || len(blobTags) == 0 {
			blobTags = nil
		}

		// TODO: Remove this snippet once service starts supporting CPK with blob tier
		destBlobTier := s.destBlobTier
		if s.cpkToApply.EncryptionScope != nil || (s.cpkToApply.EncryptionKey != nil && s.cpkToApply.EncryptionKeySha256 != nil) {
			destBlobTier = azblob.AccessTierNone
		}

		if _, err := s.destBlockBlobURL.CommitBlockList(jptm.Context(), blockIDs, s.headersToApply, s.metadataToApply, azblob.BlobAccessConditions{}, destBlobTier, blobTags, s.cpkToApply); err != nil {
			jptm.FailActiveSend("Committing block list", err)
			return
		}

		if separateSetTagsRequired {
			if _, err := s.destBlockBlobURL.SetTags(jptm.Context(), nil, nil, nil, s.blobTagsToApply); err != nil {
				s.jptm.Log(pipeline.LogWarning, err.Error())
			}
		}
	}

	// Upload ADLS Gen 2 ACLs
	if jptm.FromTo() == common.EFromTo.BlobBlob() && jptm.Info().PreserveSMBPermissions.IsTruthy() {
		bURLParts := azblob.NewBlobURLParts(s.destBlockBlobURL.URL())
		bURLParts.BlobName = strings.TrimSuffix(bURLParts.BlobName, "/") // BlobFS does not like when we target a folder with the /
		bURLParts.Host = strings.ReplaceAll(bURLParts.Host, ".blob", ".dfs")
		// todo: jank, and violates the principle of interfaces
		fileURL := azbfs.NewFileURL(bURLParts.URL(), s.jptm.(*jobPartTransferMgr).jobPartMgr.(*jobPartMgr).secondaryPipeline)

		// We know for a fact our source is a "blob".
		acl, err := s.sip.(*blobSourceInfoProvider).AccessControl()
		if err != nil {
			jptm.FailActiveSend("Grabbing source ACLs", err)
		}
		acl.Permissions = "" // Since we're sending the full ACL, Permissions is irrelevant.
		_, err = fileURL.SetAccessControl(jptm.Context(), acl)
		if err != nil {
			jptm.FailActiveSend("Putting ACLs", err)
		}
	}
}

func (s *blockBlobSenderBase) Cleanup() {
	jptm := s.jptm

	// Cleanup
	if jptm.IsDeadInflight() && atomic.LoadInt32(&s.atomicChunksWritten) != 0 {
		// there is a possibility that some uncommitted blocks will be there
		// Delete the uncommitted blobs
		deletionContext, cancelFn := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelFn()
		if jptm.WasCanceled() {
			// If we cancelled, and the only blocks that exist are uncommitted, then clean them up.
			// This prevents customer paying for their storage for a week until they get garbage collected, and it
			// also prevents any issues with "too many uncommitted blocks" if user tries to upload the blob again in future.
			// But if there are committed blocks, leave them there (since they still safely represent the state before our job even started)
			blockList, err := s.destBlockBlobURL.GetBlockList(deletionContext, azblob.BlockListAll, azblob.LeaseAccessConditions{})
			hasUncommittedOnly := err == nil && len(blockList.CommittedBlocks) == 0 && len(blockList.UncommittedBlocks) > 0
			if hasUncommittedOnly {
				jptm.LogAtLevelForCurrentTransfer(pipeline.LogDebug, "Deleting uncommitted destination blob due to cancellation")
				// Delete can delete uncommitted blobs.
				_, _ = s.destBlockBlobURL.Delete(deletionContext, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
			}
		} else {
			// TODO: review (one last time) should we really do this?  Or should we just give better error messages on "too many uncommitted blocks" errors
			jptm.LogAtLevelForCurrentTransfer(pipeline.LogDebug, "Deleting destination blob due to failure")
			_, _ = s.destBlockBlobURL.Delete(deletionContext, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
		}
	}
}

func (s *blockBlobSenderBase) setBlockID(index int32, value string) {
	s.muBlockIDs.Lock()
	defer s.muBlockIDs.Unlock()
	if len(s.blockIDs[index]) > 0 {
		panic(errors.New("block id set twice for one block"))
	}
	s.blockIDs[index] = value
}

func (s *blockBlobSenderBase) generateEncodedBlockID() string {
	blockID := common.NewUUID().String()
	return base64.StdEncoding.EncodeToString([]byte(blockID))
}
