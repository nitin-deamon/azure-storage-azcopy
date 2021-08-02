// Copyright © 2017 Microsoft <nitinsingla@microsoft.com>
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

package scanner

import (
        "fmt"
	"log"
	"context"
        "errors"
        "strings"
	"runtime"

	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)
var azcopyLogPathFolder string
var scannerCurrentJobID common.JobID
var azcopyScanningLogger common.ILoggerResetable
var glcm = common.GetLifecycleMgr()

type rawScanCmdArgs struct {
	// from arguments
	src    string
	dst    string
	fromTo string
	//blobUrlForRedirection string

	// new include/exclude only apply to file names
	// implemented for remove (and sync) only
	include               string
	exclude               string
	includePath           string // NOTE: This gets handled like list-of-files! It may LOOK like a bug, but it is not.
	excludePath           string
	includeFileAttributes string
	excludeFileAttributes string
	includeBefore         string
	includeAfter          string
	legacyInclude         string // used only for warnings
	legacyExclude         string // used only for warnings
	listOfVersionIDs      string

	// filters from flags
	listOfFilesToCopy string
	recursive         bool
	followSymlinks    bool
	autoDecompress    bool
	// forceWrite flag is used to define the User behavior
	// to overwrite the existing blobs or not.
	forceWrite      string
	forceIfReadOnly bool

	// options from flags
	blockSizeMB              float64
	metadata                 string
	contentType              string
	contentEncoding          string
	contentDisposition       string
	contentLanguage          string
	cacheControl             string
	noGuessMimeType          bool
	preserveLastModifiedTime bool
	putMd5                   bool
	md5ValidationOption      string
	CheckLength              bool
	deleteSnapshotsOption    string

	blobTags string
	// defines the type of the blob at the destination in case of upload / account to account copy
	blobType      string
	blockBlobTier string
	pageBlobTier  string
	output        string // TODO: Is this unused now? replaced with param at root level?
	logVerbosity  string
	// list of blobTypes to exclude while enumerating the transfer
	excludeBlobType string
	// Opt-in flag to persist SMB ACLs to Azure Files.
	preserveSMBPermissions bool
	preserveOwner          bool // works in conjunction with preserveSmbPermissions
	// Opt-in flag to persist additional SMB properties to Azure Files. Named ...info instead of ...properties
	// because the latter was similar enough to preserveSMBPermissions to induce user error
	preserveSMBInfo bool
	// Opt-in flag to preserve the blob index tags during service to service transfer.
	s2sPreserveBlobTags bool
	// Flag to enable Window's special privileges
	backupMode bool
	// whether user wants to preserve full properties during service to service copy, the default value is true.
	// For S3 and Azure File non-single file source, as list operation doesn't return full properties of objects/files,
	// to preserve full properties AzCopy needs to send one additional request per object/file.
	s2sPreserveProperties bool
	// useful when preserveS3Properties set to true, enables get S3 objects' or Azure files' properties during s2s copy in backend, the default value is true
	s2sGetPropertiesInBackend bool
	// whether user wants to preserve access tier during service to service copy, the default value is true.
	// In some case, e.g. target is a GPv1 storage account, access tier cannot be set properly.
	// In such cases, use s2sPreserveAccessTier=false to bypass the access tier copy.
	// For more details, please refer to https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-storage-tiers
	s2sPreserveAccessTier bool
	// whether user wants to check if source has changed after enumerating, the default value is true.
	// For S2S copy, as source is a remote resource, validating whether source has changed need additional request costs.
	s2sSourceChangeValidation bool
	// specify how user wants to handle invalid metadata.
	s2sInvalidMetadataHandleOption string

	// internal override to enforce strip-top-dir
	internalOverrideStripTopDir bool

	// whether to include blobs that have metadata 'hdi_isfolder = true'
	includeDirectoryStubs bool

	// whether to disable automatic decoding of illegal chars on Windows
	disableAutoDecoding bool

	// Optional flag to encrypt user data with user provided key.
	// Key is provide in the REST request itself
	// Provided key (EncryptionKey and EncryptionKeySHA256) and its hash will be fetched from environment variables
	// Set EncryptionAlgorithm = "AES256" by default.
	cpkInfo bool
	// Key is present in AzureKeyVault and Azure KeyVault is linked with storage account.
	// Provided key name will be fetched from Azure Key Vault and will be used to encrypt the data
	cpkScopeInfo string
}

func (raw rawScanCmdArgs) scanner_cook() (cmd.CookedCopyCmdArgs, error) {
	cooked := cmd.CookedCopyCmdArgs{
	}
        err := cooked.LogVerbosity.Parse("INFO")
	if err != nil {
		return cooked, err
	}
	// set up the front end scanning logger
	azcopyScanningLogger = common.NewJobLogger(scannerCurrentJobID, cooked.LogVerbosity, azcopyLogPathFolder, "-scanning")
	azcopyScanningLogger.OpenLog()
	glcm.RegisterCloseFunc(func() {
		azcopyScanningLogger.CloseLog()
	})

	/* We support DFS by using blob end-point of the account. We replace dfs by blob in src and dst */
	if src, dst := cmd.InferArgumentLocation(raw.src), cmd.InferArgumentLocation(raw.dst);
				src == common.ELocation.BlobFS() || dst == common.ELocation.BlobFS() {
		if src == common.ELocation.BlobFS() && dst != common.ELocation.Local() {
			raw.src = strings.Replace(raw.src, ".dfs", ".blob", 1)
			glcm.Info("Switching to use blob endpoint on source account.")
		}

		if dst == common.ELocation.BlobFS() && src != common.ELocation.Local() {
			raw.dst = strings.Replace(raw.dst, ".dfs", ".blob", 1)
			glcm.Info("Switching to use blob endpoint on destination account.")
		}
	}
        fmt.Println(raw.src)
	fromTo, err := cmd.ValidateFromTo(raw.src, raw.dst, raw.fromTo) // TODO: src/dst
	if err != nil {
		return cooked, err
	}

	var tempSrc string
	tempDest := raw.dst

	if strings.EqualFold(tempDest, common.Dev_Null) && runtime.GOOS == "windows" {
		tempDest = common.Dev_Null // map all capitalization of "NUL"/"nul" to one because (on Windows) they all mean the same thing
	}

		// Strip the SAS from the source and destination whenever there is SAS exists in URL.
	// Note: SAS could exists in source of S2S copy, even if the credential type is OAuth for destination.
        fmt.Println(tempSrc)
        tempSrc = raw.src
	cooked.Source, err = cmd.SplitResourceString(tempSrc, fromTo.From())
	if err != nil {
		return cooked, err
	}

	cooked.Destination, err = cmd.SplitResourceString(tempDest, fromTo.To())
	if err != nil {
		return cooked, err
	}

	cooked.FromTo = fromTo
	cooked.Recursive = true
	cooked.FollowSymlinks = false
	cooked.ForceIfReadOnly = false

	// cooked.stripTopDir is effectively a workaround for the lack of wildcards in remote sources.
	// Local, however, still supports wildcards, and thus needs its top directory stripped whenever a wildcard is used.
	// Thus, we check for wildcards and instruct the processor to strip the top dir later instead of repeatedly checking cca.source for wildcards.
	if fromTo.From() == common.ELocation.Local() && strings.Contains(cooked.Source.ValueLocal(), "*") {
		cooked.StripTopDir = true
	}

	// Everything uses the new implementation of list-of-files now.
	// This handles both list-of-files and include-path as a list enumerator.
	// This saves us time because we know *exactly* what we're looking for right off the bat.
	// Note that exclude-path is handled as a filter unlike include-path.

	if raw.includeBefore != "" {
		// must set chooseEarliest = false, so that if there's an ambiguous local date, the latest will be returned
		// (since that's safest for includeBefore.  Better to choose the later time and do more work, than the earlier one and fail to pick up a changed file
		parsedIncludeBefore, err := cmd.IncludeBeforeDateFilter{}.ParseISO8601(raw.includeBefore, false)
		if err != nil {
			return cooked, err
		}
		cooked.IncludeBefore = &parsedIncludeBefore
	}

	if raw.includeAfter != "" {
		// must set chooseEarliest = true, so that if there's an ambiguous local date, the earliest will be returned
		// (since that's safest for includeAfter.  Better to choose the earlier time and do more work, than the later one and fail to pick up a changed file
		parsedIncludeAfter, err := cmd.IncludeAfterDateFilter{}.ParseISO8601(raw.includeAfter, true)
		if err != nil {
			return cooked, err
		}
		cooked.IncludeAfter = &parsedIncludeAfter
	}

	// Setting CPK-N
	cpkOptions := common.CpkOptions{}
	// Get the key (EncryptionKey and EncryptionKeySHA256) value from environment variables when required.
	cpkOptions.CpkInfo = false

	cooked.CpkOptions = cpkOptions

	// parse the filter patterns
	cooked.IncludePatterns = make([]string , 0)
	cooked.ExcludePatterns = make([]string, 0)
	cooked.ExcludePathPatterns = make([]string, 0)

	cooked.IncludeFileAttributes = make([]string, 0)
	cooked.ExcludeFileAttributes = make([]string, 0)

	return cooked, nil
}


func ScanInitEnumerator(cca cmd.CookedCopyCmdArgs, ctx context.Context) (*cmd.CopyEnumerator, error) {
	var traverser cmd.ResourceTraverser

	// Warn about GCP -> Blob being in preview. Also, we do not support GCP as destination.

	srcCredInfo := common.CredentialInfo{}
	var isPublic bool
	var err error

	if srcCredInfo, isPublic, err = cmd.GetCredentialInfoForLocation(ctx, cca.FromTo.From(), cca.Source.Value, cca.Source.SAS, true, cca.CpkOptions); err != nil {
		return nil, err
		// If S2S and source takes OAuthToken as its cred type (OR) source takes anonymous as its cred type, but it's not public and there's no SAS
	} else if cca.FromTo.From().IsRemote() && cca.FromTo.To().IsRemote() &&
		(srcCredInfo.CredentialType == common.ECredentialType.OAuthToken() ||
			(srcCredInfo.CredentialType == common.ECredentialType.Anonymous() && !isPublic && cca.Source.SAS == "")) {
		// TODO: Generate a SAS token if it's blob -> *
		return nil, errors.New("a SAS token (or S3 access key) is required as a part of the source in S2S transfers, unless the source is a public resource")
	}


	// Infer on download so that we get LMT and MD5 on files download
	// On S2S transfers the following rules apply:
	// If preserve properties is enabled, but get properties in backend is disabled, turn it on
	// If source change validation is enabled on files to remote, turn it on (consider a separate flag entirely?)
	getRemoteProperties := cca.ForceWrite == common.EOverwriteOption.IfSourceNewer() ||
		(cca.FromTo.From() == common.ELocation.File() && !cca.FromTo.To().IsRemote()) // If download, we still need LMT and MD5 from files.

	traverser, err = cmd.InitResourceTraverser(cca.Source, cca.FromTo.From(), &ctx, &srcCredInfo,
		&cca.FollowSymlinks, cca.ListOfFilesChannel, cca.Recursive, getRemoteProperties,
		cca.IncludeDirectoryStubs, func(common.EntityType) {}, cca.ListOfVersionIDs,
		cca.S2sPreserveBlobTags, cca.LogVerbosity.ToPipelineLogLevel(), cca.CpkOptions)

	if err != nil {
		return nil, err
	}

	// Ensure we're only copying from a directory with a trailing wildcard or recursive.
	isSourceDir := traverser.IsDirectory(true)
	if isSourceDir && !cca.Recursive && !cca.StripTopDir {
		return nil, errors.New("cannot use directory as source without --recursive or a trailing wildcard (/*)")
	}

	// Check if the destination is a directory so we can correctly decide where our files land
	isDestDir := true
	if cca.ListOfVersionIDs != nil && (!(cca.FromTo == common.EFromTo.BlobLocal() || cca.FromTo == common.EFromTo.BlobTrash()) || isSourceDir || !isDestDir) {
		log.Fatalf("Either source is not a blob or destination is not a local folder")
	}
	srcLevel, err := cmd.DetermineLocationLevel(cca.Source.Value, cca.FromTo.From(), true)

	if err != nil {
		return nil, err
	}

	dstLevel, err := cmd.DetermineLocationLevel(cca.Destination.Value, cca.FromTo.To(), false)

	if err != nil {
		return nil, err
	}

	// Disallow list-of-files and include-path on service-level traversal due to a major bug
	// TODO: Fix the bug.
	//       Two primary issues exist with the list-of-files implementation:
	//       1) Account name doesn't get trimmed from the path
	//       2) List-of-files is not considered an account traverser; therefore containers don't get made.
	//       Resolve these two issues and service-level list-of-files/include-path will work
	if cca.ListOfFilesChannel != nil && srcLevel == cmd.ELocationLevel.Service() {
		return nil, errors.New("cannot combine list-of-files or include-path with account traversal")
	}

	if (srcLevel == cmd.ELocationLevel.Object() || cca.FromTo.From().IsLocal()) && dstLevel == cmd.ELocationLevel.Service() {
		return nil, errors.New("cannot transfer individual files/folders to the root of a service. Add a container or directory to the destination URL")
	}

	// When copying a container directly to a container, strip the top directory
	if srcLevel == cmd.ELocationLevel.Container() && dstLevel == cmd.ELocationLevel.Container() && cca.FromTo.From().IsRemote() && cca.FromTo.To().IsRemote() {
		cca.StripTopDir = true
	}

	// Create a Remote resource resolver
	// Giving it nothing to work with as new names will be added as we traverse.
	var containerResolver cmd.BucketToContainerNameResolver
	containerResolver = cmd.NewS3BucketNameToAzureResourcesResolver(nil)
	if cca.FromTo == common.EFromTo.GCPBlob() {
		containerResolver = cmd.NewGCPBucketNameToAzureResourcesResolver(nil)
	}
//	existingContainers := make(map[string]bool)
//	var logDstContainerCreateFailureOnce sync.Once
	seenFailedContainers := make(map[string]bool) // Create map of already failed container conversions so we don't log a million items just for one container.

	dstContainerName := ""

	filters := cca.InitModularFilters()

	processor := func(object cmd.StoredObject) error {
		// Start by resolving the name and creating the container
		if object.ContainerName != "" {
			// set up the destination container name.
			cName := dstContainerName
			// if a destination container name is not specified OR copying service to container/folder, append the src container name.
			if cName == "" || (srcLevel == cmd.ELocationLevel.Service() && dstLevel > cmd.ELocationLevel.Service()) {
				cName, err = containerResolver.ResolveName(object.ContainerName)

				if err != nil {
					if _, ok := seenFailedContainers[object.ContainerName]; !ok {
						cmd.WarnStdoutAndScanningLog(fmt.Sprintf("failed to add transfers from container %s as it has an invalid name. Please manually transfer from this container to one with a valid name.", object.ContainerName))
						seenFailedContainers[object.ContainerName] = true
					}
					return nil
				}

				object.DstContainerName = cName
			}
		}

		// If above the service level, we already know the container name and don't need to supply it to makeEscapedRelativePath
		if srcLevel != cmd.ELocationLevel.Service() {
			object.ContainerName = ""

			// When copying directly TO a container or object from a container, don't drop under a sub directory
			if dstLevel >= cmd.ELocationLevel.Container() {
				object.DstContainerName = ""
			}
		}

		srcRelPath := cca.MakeEscapedRelativePath(true, isDestDir, object)
		//dstRelPath := cca.MakeEscapedRelativePath(false, isDestDir, object)
                message := fmt.Sprintf("ScannerRun: %v", srcRelPath)
                glcm.Info(message)
                return nil
	}
	finalizer := func() error {
		return nil
	}

	return cmd.NewCopyEnumerator(traverser, filters, processor, finalizer), nil
}

func  Scanner (src string, dst string) (err error) {
	raw := rawScanCmdArgs{}
        // Getting Environmental variable and setting corresponding variable
        cmd.EnumerationParallelism = ste.GetEnumerationPoolSize().Value
        cmd.EnumerationParallelStatFiles = ste.GetParallelStatFiles().Value
        // Creating new JOBID
        scannerCurrentJobID = common.NewJobID()
        // Context creation
        ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
        // Setting Source and destination
        raw.src = src
        raw.dst = dst
        // Filling structure needed for enumerator.
        cook, err := raw.scanner_cook()
        // Initializing Enumerator
        var e *cmd.CopyEnumerator
        e, err = ScanInitEnumerator(cook, ctx)
        if err != nil {
            fmt.Println("Init Enumerator Error")
            return err
        }
        // Traversing the folder
        err = e.Traverser.Traverse(cmd.NoPreProccessor, e.ObjectDispatcher, e.Filters)
	if err != nil {
            fmt.Println(err)
            fmt.Println("Traversing Error")
	}
	log.Println("Traversing done")
        return nil
}

