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
	"encoding/json"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/Azure/azure-storage-blob-go/azblob"
	chk "gopkg.in/check.v1"

	"github.com/nitin-deamon/azure-storage-azcopy/v10/common"
)

func (s *cmdIntegrationSuite) TestInferredStripTopDirDownload(c *chk.C) {
	bsu := getBSU()
	cURL, cName := createNewContainer(c, bsu)

	blobNames := []string{
		"*", // File name that we want to retain compatibility with
		"testFile",
		"DoYouPronounceItDataOrData",
		"sub*dir/Help I cannot so much into computer",
	}

	// ----- TEST # 1: Test inferred as false by using escaped * -----

	// set up container name
	scenarioHelper{}.generateBlobsFromList(c, cURL, blobNames, blockBlobDefaultData)

	dstDirName := scenarioHelper{}.generateLocalDirectory(c)

	rawContainerURL := scenarioHelper{}.getRawContainerURLWithSAS(c, cName)

	// Don't add /* while still in URL form-- it will get improperly encoded, and azcopy will ignore it.
	rawContainerString := rawContainerURL.String()
	rawContainerStringSplit := strings.Split(rawContainerString, "?")
	rawContainerStringSplit[0] += "/%2A"
	// now in theory: https://ciblobaccount.blob.core.windows.net/container/%2A
	// %2A is set to magic number %00 and not stripped
	// striptopdir should not be set

	// re join strings and create raw input
	raw := getDefaultRawCopyInput(strings.Join(rawContainerStringSplit, "?"), dstDirName)
	raw.recursive = false // default recursive is true in testing framework

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// Test inference of striptopdir
	cooked, err := raw.cook()
	c.Assert(err, chk.IsNil)
	c.Assert(cooked.StripTopDir, chk.Equals, false)

	// Test and ensure only one file is being downloaded
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		c.Assert(len(mockedRPC.transfers), chk.Equals, 1)
	})

	// ----- TEST # 2: Test inferred as true by using unescaped * -----

	rawContainerStringSplit = strings.Split(rawContainerString, "?")
	rawContainerStringSplit[0] += "/*"
	// now in theory: https://ciblobaccount.blob.core.windows.net/container/*
	// * is not set to magic number %00, * gets stripped
	// striptopdir should be set.

	// re join strings and create raw input
	raw = getDefaultRawCopyInput(strings.Join(rawContainerStringSplit, "?"), dstDirName)
	raw.recursive = false // default recursive is true in testing framework

	// reset RPC
	mockedRPC.reset()

	// Test inference of striptopdir
	cooked, err = raw.cook()
	c.Assert(err, chk.IsNil)
	c.Assert(cooked.StripTopDir, chk.Equals, true)

	// Test and ensure only 3 files get scheduled, nothing under the sub-directory
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		c.Assert(len(mockedRPC.transfers), chk.Equals, 3)
	})

	// ----- TEST # 3: Attempt to use the * in the folder name without encoding ----

	rawContainerStringSplit = strings.Split(rawContainerString, "?")
	rawContainerStringSplit[0] += "/sub*dir/*"
	// now in theory: https://ciblobaccount.blob.core.windows.net/container/sub*dir/*
	// *s are not replaced with magic number %00
	// should error out due to extra * in dir name

	// reset RPC
	mockedRPC.reset()

	// re join strings and create raw input
	raw = getDefaultRawCopyInput(strings.Join(rawContainerStringSplit, "?"), dstDirName)
	raw.recursive = false // default recursive is true in testing framework

	// test error
	cooked, err = raw.cook()
	c.Assert(err, chk.NotNil)
	c.Assert(err.Error(), StringContains, "cannot use wildcards")

	// no actual test needed-- this is where the error lives.

	// ----- TEST # 4: Encode %2A in the folder name and still use stripTopDir ----

	rawContainerStringSplit = strings.Split(rawContainerString, "?")
	rawContainerStringSplit[0] += "/sub%2Adir/*"
	// now in theory: https://ciblobaccount.blob.core.windows.net/container/sub%2Adir/*
	// %2A is replaced with magic number %00
	// should not error out; striptopdir should be true

	// reset RPC
	mockedRPC.reset()

	// re join strings and create raw input
	raw = getDefaultRawCopyInput(strings.Join(rawContainerStringSplit, "?"), dstDirName)
	raw.recursive = false // default recursive is true in testing framework

	// test cook
	cooked, err = raw.cook()
	c.Assert(err, chk.IsNil)
	c.Assert(cooked.StripTopDir, chk.Equals, true)

	// Test and ensure only one file got scheduled
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		c.Assert(len(mockedRPC.transfers), chk.Equals, 1)
	})
}

// Test downloading the entire account.
func (s *cmdIntegrationSuite) TestDownloadAccount(c *chk.C) {
	bsu := getBSU()
	rawBSU := scenarioHelper{}.getRawBlobServiceURLWithSAS(c)
	p, err := InitPipeline(ctx, common.ELocation.Blob(), common.CredentialInfo{CredentialType: common.ECredentialType.Anonymous()}, pipeline.LogNone)
	c.Assert(err, chk.IsNil)

	// Just in case there are no existing containers...
	curl, _ := createNewContainer(c, bsu)
	scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, curl, "")

	// Traverse the account ahead of time and determine the relative paths for testing.
	relPaths := make([]string, 0) // Use a map for easy lookup
	blobTraverser := newBlobAccountTraverser(&rawBSU, p, ctx, false, func(common.EntityType) {}, false, common.CpkOptions{})
	processor := func(object StoredObject) error {
		// Append the container name to the relative path
		relPath := "/" + object.ContainerName + "/" + object.relativePath
		relPaths = append(relPaths, relPath)
		return nil
	}
	err = blobTraverser.Traverse(noPreProccessor, processor, []ObjectFilter{})
	c.Assert(err, chk.IsNil)

	// set up a destination
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	raw := getDefaultCopyRawInput(rawBSU.String(), dstDirName)
	raw.recursive = true

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		validateDownloadTransfersAreScheduled(c, "", "", relPaths, mockedRPC)
	})
}

// Test downloading the entire account.
func (s *cmdIntegrationSuite) TestDownloadAccountWildcard(c *chk.C) {
	bsu := getBSU()
	rawBSU := scenarioHelper{}.getRawBlobServiceURLWithSAS(c)
	p, err := InitPipeline(ctx, common.ELocation.Blob(), common.CredentialInfo{CredentialType: common.ECredentialType.Anonymous()}, pipeline.LogNone)
	c.Assert(err, chk.IsNil)

	// Create a unique container to be targeted.
	cname := generateName("blah-unique-blah", 63)
	curl := bsu.NewContainerURL(cname)
	_, err = curl.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
	c.Assert(err, chk.IsNil)
	scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, curl, "")

	// update the raw BSU to match the unique container name
	rawBSU.Path = "/blah-unique-blah*"

	// Traverse the account ahead of time and determine the relative paths for testing.
	relPaths := make([]string, 0) // Use a map for easy lookup
	blobTraverser := newBlobAccountTraverser(&rawBSU, p, ctx, false, func(common.EntityType) {}, false, common.CpkOptions{})
	processor := func(object StoredObject) error {
		// Append the container name to the relative path
		relPath := "/" + object.ContainerName + "/" + object.relativePath
		relPaths = append(relPaths, relPath)
		return nil
	}
	err = blobTraverser.Traverse(noPreProccessor, processor, []ObjectFilter{})
	c.Assert(err, chk.IsNil)

	// set up a destination
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	raw := getDefaultCopyRawInput(rawBSU.String(), dstDirName)
	raw.recursive = true

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		validateDownloadTransfersAreScheduled(c, "", "", relPaths, mockedRPC)
	})
}

// regular blob->local file download
func (s *cmdIntegrationSuite) TestDownloadSingleBlobToFile(c *chk.C) {
	bsu := getBSU()
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	for _, blobName := range []string{"singleblobisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the container with a single blob
		blobList := []string{blobName}
		scenarioHelper{}.generateBlobsFromList(c, containerURL, blobList, blockBlobDefaultData)
		c.Assert(containerURL, chk.NotNil)

		// set up the destination as a single file
		dstDirName := scenarioHelper{}.generateLocalDirectory(c)
		defer os.RemoveAll(dstDirName)
		dstFileName := "whatever"
		scenarioHelper{}.generateLocalFilesFromList(c, dstDirName, blobList)

		// set up interceptor
		mockedRPC := interceptor{}
		Rpc = mockedRPC.intercept
		mockedRPC.init()

		// construct the raw input to simulate user input
		rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, blobList[0])
		raw := getDefaultCopyRawInput(rawBlobURLWithSAS.String(), filepath.Join(dstDirName, dstFileName))

		// the file was created after the blob, so no sync should happen
		runCopyAndVerify(c, raw, func(err error) {
			c.Assert(err, chk.IsNil)

			validateDownloadTransfersAreScheduled(c, "", "", []string{""}, mockedRPC)
		})

		// clean the RPC for the next test
		mockedRPC.reset()

		// now target the destination directory, the result should be the same
		raw = getDefaultCopyRawInput(rawBlobURLWithSAS.String(), dstDirName)

		// the file was created after the blob, so no sync should happen
		runCopyAndVerify(c, raw, func(err error) {
			c.Assert(err, chk.IsNil)

			// verify explicitly since the source and destination names will be different:
			// the source is "" since the given URL points to the blob itself
			// the destination should be the blob name, since the given local path points to the parent dir
			c.Assert(len(mockedRPC.transfers), chk.Equals, 1)
			c.Assert(mockedRPC.transfers[0].Source, chk.Equals, "")
			c.Assert(mockedRPC.transfers[0].Destination, chk.Equals, common.AZCOPY_PATH_SEPARATOR_STRING+blobName)
		})
	}
}

// regular container->directory download
func (s *cmdIntegrationSuite) TestDownloadBlobContainer(c *chk.C) {
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(c, bsu)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, containerURL, "")
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultCopyRawInput(rawContainerURLWithSAS.String(), dstDirName)
	raw.recursive = true

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(blobList))

		// validate that the right transfers were sent
		validateDownloadTransfersAreScheduled(c, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING+containerName+common.AZCOPY_PATH_SEPARATOR_STRING, blobList, mockedRPC)
	})

	// turn off recursive, this time nothing should be transferred
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.NotNil)
		c.Assert(len(mockedRPC.transfers), chk.Equals, 0)
	})
}

// regular vdir->dir download
func (s *cmdIntegrationSuite) TestDownloadBlobVirtualDirectory(c *chk.C) {
	bsu := getBSU()
	vdirName := "vdir1"

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(c, bsu)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, containerURL, vdirName+common.AZCOPY_PATH_SEPARATOR_STRING)
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, vdirName)
	raw := getDefaultCopyRawInput(rawContainerURLWithSAS.String(), dstDirName)
	raw.recursive = true

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(blobList))

		// validate that the right transfers were sent
		expectedTransfers := scenarioHelper{}.shaveOffPrefix(blobList, vdirName+common.AZCOPY_PATH_SEPARATOR_STRING)
		validateDownloadTransfersAreScheduled(c, common.AZCOPY_PATH_SEPARATOR_STRING,
			common.AZCOPY_PATH_SEPARATOR_STRING+vdirName+common.AZCOPY_PATH_SEPARATOR_STRING, expectedTransfers, mockedRPC)
	})

	// turn off recursive, this time nothing should be transferred
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.NotNil)
		c.Assert(len(mockedRPC.transfers), chk.Equals, 0)
	})
}

// blobs(from pattern)->directory download
// TODO the current pattern matching behavior is inconsistent with the posix filesystem
//   update test after re-writing copy enumerators
func (s *cmdIntegrationSuite) TestDownloadBlobContainerWithPattern(c *chk.C) {
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(c, bsu)
	blobsToIgnore := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, containerURL, "")
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)
	c.Assert(len(blobsToIgnore), chk.Not(chk.Equals), 0)

	// add special blobs that we wish to include
	blobsToInclude := []string{"important.pdf", "includeSub/amazing.pdf", "includeSub/wow/amazing.pdf"}
	scenarioHelper{}.generateBlobsFromList(c, containerURL, blobsToInclude, blockBlobDefaultData)

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	rawContainerURLWithSAS.Path = path.Join(rawContainerURLWithSAS.Path, string([]byte{0x00}))
	containerString := strings.ReplaceAll(rawContainerURLWithSAS.String(), "%00", "*")
	raw := getDefaultCopyRawInput(containerString, dstDirName)
	raw.recursive = true
	raw.include = "*.pdf"

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(blobsToInclude))

		// validate that the right transfers were sent
		validateDownloadTransfersAreScheduled(c, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING,
			blobsToInclude, mockedRPC)
	})

	// turn off recursive, this time nothing should be transferred
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// only the top pdf should be included
		c.Assert(len(mockedRPC.transfers), chk.Equals, 1)
		c.Assert(mockedRPC.transfers[0].Source, chk.Equals, mockedRPC.transfers[0].Destination)
		c.Assert(strings.HasSuffix(mockedRPC.transfers[0].Source, ".pdf"), chk.Equals, true)
		c.Assert(strings.Contains(mockedRPC.transfers[0].Source[1:], common.AZCOPY_PATH_SEPARATOR_STRING), chk.Equals, false)
	})
}

// test for include with one regular expression
func (s *cmdIntegrationSuite) TestDownloadBlobContainerWithRegexInclude(c *chk.C) {
	bsu := getBSU()

	// set up the container with  blobs
	containerURL, containerName := createNewContainer(c, bsu)
	blobsToIgnore := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, containerURL, "")
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)
	c.Assert(len(blobsToIgnore), chk.Not(chk.Equals), 0)

	// add blobs that we wish to include
	blobsToInclude := []string{"tessssssssssssst.txt", "subOne/tetingessssss.jpeg", "subOne/tessssst/hi.pdf"}
	scenarioHelper{}.generateBlobsFromList(c, containerURL, blobsToInclude, blockBlobDefaultData)

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	rawContainerURLWithSAS.Path = path.Join(rawContainerURLWithSAS.Path, string([]byte{0x00}))
	containerString := strings.ReplaceAll(rawContainerURLWithSAS.String(), "%00", "*")
	raw := getDefaultCopyRawInput(containerString, dstDirName)
	raw.recursive = true
	raw.includeRegex = "es{4,}"

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(blobsToInclude))
		//comparing is names of files match
		actualTransfer := []string{}
		for i := 0; i < len(mockedRPC.transfers); i++ {
			actualTransfer = append(actualTransfer, strings.Trim(mockedRPC.transfers[i].Source, "/"))
		}
		sort.Strings(actualTransfer)
		sort.Strings(blobsToInclude)
		c.Assert(actualTransfer, chk.DeepEquals, blobsToInclude)

		// validate that the right transfers were sent
		validateDownloadTransfersAreScheduled(c, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING,
			blobsToInclude, mockedRPC)
	})
}

//test multiple regular expression with include
func (s *cmdIntegrationSuite) TestDownloadBlobContainerWithMultRegexInclude(c *chk.C) {
	bsu := getBSU()

	// set up the container with  blobs
	containerURL, containerName := createNewContainer(c, bsu)
	blobsToIgnore := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, containerURL, "")
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)
	c.Assert(len(blobsToIgnore), chk.Not(chk.Equals), 0)

	// add blobs that we wish to include
	blobsToInclude := []string{"tessssssssssssst.txt", "zxcfile.txt", "subOne/tetingessssss.jpeg", "subOne/subTwo/tessssst.pdf"}
	scenarioHelper{}.generateBlobsFromList(c, containerURL, blobsToInclude, blockBlobDefaultData)

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	rawContainerURLWithSAS.Path = path.Join(rawContainerURLWithSAS.Path, string([]byte{0x00}))
	containerString := strings.ReplaceAll(rawContainerURLWithSAS.String(), "%00", "*")
	raw := getDefaultCopyRawInput(containerString, dstDirName)
	raw.recursive = true
	raw.includeRegex = "es{4,};^zxc"

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(blobsToInclude))
		// validate that the right transfers were sent

		//comparing is names of files, since not in order need to sort each string and the compare them
		actualTransfer := []string{}
		for i := 0; i < len(mockedRPC.transfers); i++ {
			actualTransfer = append(actualTransfer, strings.Trim(mockedRPC.transfers[i].Source, "/"))
		}
		sort.Strings(actualTransfer)
		sort.Strings(blobsToInclude)
		c.Assert(actualTransfer, chk.DeepEquals, blobsToInclude)

		validateDownloadTransfersAreScheduled(c, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING,
			blobsToInclude, mockedRPC)
	})
}

//testing empty expressions for both include and exclude
func (s *cmdIntegrationSuite) TestDownloadBlobContainerWithEmptyRegex(c *chk.C) {
	bsu := getBSU()

	// set up the container with  blobs
	containerURL, containerName := createNewContainer(c, bsu)
	//test empty regex flag so all blobs will be included since there is no filter
	blobsToInclude := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, containerURL, "")
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)
	c.Assert(len(blobsToInclude), chk.Not(chk.Equals), 0)

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	rawContainerURLWithSAS.Path = path.Join(rawContainerURLWithSAS.Path, string([]byte{0x00}))
	containerString := strings.ReplaceAll(rawContainerURLWithSAS.String(), "%00", "*")
	raw := getDefaultCopyRawInput(containerString, dstDirName)
	raw.recursive = true
	raw.includeRegex = ""
	raw.excludeRegex = ""

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(blobsToInclude))
		//do not need to check file names since all files for blobsToInclude are passed bc flags are empty
		// validate that the right transfers were sent
		validateDownloadTransfersAreScheduled(c, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING,
			blobsToInclude, mockedRPC)
	})
}

//testing exclude with one regular expression
func (s *cmdIntegrationSuite) TestDownloadBlobContainerWithRegexExclude(c *chk.C) {
	bsu := getBSU()

	// set up the container with  blobs
	containerURL, containerName := createNewContainer(c, bsu)
	blobsToInclude := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, containerURL, "")
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)
	c.Assert(len(blobsToInclude), chk.Not(chk.Equals), 0)

	// add blobs that we wish to exclude
	blobsToIgnore := []string{"tessssssssssssst.txt", "subOne/tetingessssss.jpeg", "subOne/subTwo/tessssst.pdf"}
	scenarioHelper{}.generateBlobsFromList(c, containerURL, blobsToIgnore, blockBlobDefaultData)

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	rawContainerURLWithSAS.Path = path.Join(rawContainerURLWithSAS.Path, string([]byte{0x00}))
	containerString := strings.ReplaceAll(rawContainerURLWithSAS.String(), "%00", "*")
	raw := getDefaultCopyRawInput(containerString, dstDirName)
	raw.recursive = true
	raw.excludeRegex = "es{4,}"

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		// validate that only blobsTo
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(blobsToInclude))
		//comparing is names of files, since not in order need to sort each string and the compare them
		actualTransfer := []string{}
		for i := 0; i < len(mockedRPC.transfers); i++ {
			actualTransfer = append(actualTransfer, strings.Trim(mockedRPC.transfers[i].Destination, "/"))
		}
		sort.Strings(actualTransfer)
		sort.Strings(blobsToInclude)
		c.Assert(actualTransfer, chk.DeepEquals, blobsToInclude)

		// validate that the right transfers were sent
		validateDownloadTransfersAreScheduled(c, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING,
			blobsToInclude, mockedRPC)
	})
}

//testing exclude with multiple regular expressions
func (s *cmdIntegrationSuite) TestDownloadBlobContainerWithMultRegexExclude(c *chk.C) {
	bsu := getBSU()

	// set up the container with  blobs
	containerURL, containerName := createNewContainer(c, bsu)
	blobsToInclude := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, containerURL, "")
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)
	c.Assert(len(blobsToInclude), chk.Not(chk.Equals), 0)

	// add blobs that we wish to exclude
	blobsToIgnore := []string{"tessssssssssssst.txt", "subOne/dogs.jpeg", "subOne/subTwo/tessssst.pdf"}
	scenarioHelper{}.generateBlobsFromList(c, containerURL, blobsToIgnore, blockBlobDefaultData)

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	rawContainerURLWithSAS.Path = path.Join(rawContainerURLWithSAS.Path, string([]byte{0x00}))
	containerString := strings.ReplaceAll(rawContainerURLWithSAS.String(), "%00", "*")
	raw := getDefaultCopyRawInput(containerString, dstDirName)
	raw.recursive = true
	raw.excludeRegex = "es{4,};o(g)"

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(blobsToInclude))
		//comparing is names of files, since not in order need to sort each string and the compare them
		actualTransfer := []string{}
		for i := 0; i < len(mockedRPC.transfers); i++ {
			actualTransfer = append(actualTransfer, strings.Trim(mockedRPC.transfers[i].Destination, "/"))
		}
		sort.Strings(actualTransfer)
		sort.Strings(blobsToInclude)
		c.Assert(actualTransfer, chk.DeepEquals, blobsToInclude)

		// validate that the right transfers were sent
		validateDownloadTransfersAreScheduled(c, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING,
			blobsToInclude, mockedRPC)
	})
}

func (s *cmdIntegrationSuite) TestDryrunCopyLocalToBlob(c *chk.C) {
	bsu := getBSU()

	// set up the local source
	blobsToInclude := []string{"AzURE2021.jpeg", "sub1/dir2/HELLO-4.txt", "sub1/test/testing.txt"}
	srcDirName := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(srcDirName)
	scenarioHelper{}.generateLocalFilesFromList(c, srcDirName, blobsToInclude)
	c.Assert(srcDirName, chk.NotNil)

	// set up the destination container
	dstContainerURL, dstContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, dstContainerURL)
	c.Assert(dstContainerURL, chk.NotNil)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedLcm := mockedLifecycleManager{dryrunLog: make(chan string, 50)}
	mockedLcm.SetOutputFormat(common.EOutputFormat.Text()) //text format
	glcm = &mockedLcm

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw := getDefaultCopyRawInput(srcDirName, rawContainerURLWithSAS.String())
	raw.dryrun = true
	raw.recursive = true

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		// validate that none where transferred
		c.Assert(len(mockedRPC.transfers), chk.Equals, 0)

		msg := mockedLcm.GatherAllLogs(mockedLcm.dryrunLog)
		for i := 0; i < len(blobsToInclude); i++ {
			c.Check(strings.Contains(msg[i], "DRYRUN: copy"), chk.Equals, true)
			c.Check(strings.Contains(msg[i], srcDirName), chk.Equals, true)
			c.Check(strings.Contains(msg[i], dstContainerURL.String()), chk.Equals, true)
		}

		c.Check(testDryrunStatements(blobsToInclude, msg), chk.Equals, true)
	})
}

func (s *cmdIntegrationSuite) TestDryrunCopyBlobToBlob(c *chk.C) {
	bsu := getBSU()

	// set up src container
	srcContainerURL, srcContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, srcContainerURL)
	blobsToInclude := []string{"AzURE2021.jpeg", "sub1/dir2/HELLO-4.txt", "sub1/test/testing.txt"}
	scenarioHelper{}.generateBlobsFromList(c, srcContainerURL, blobsToInclude, blockBlobDefaultData)
	c.Assert(srcContainerURL, chk.NotNil)

	// set up the destination
	dstContainerURL, dstContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, dstContainerURL)
	c.Assert(dstContainerURL, chk.NotNil)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedLcm := mockedLifecycleManager{dryrunLog: make(chan string, 50)}
	mockedLcm.SetOutputFormat(common.EOutputFormat.Text()) //text format
	glcm = &mockedLcm

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, srcContainerName)
	rawDstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw := getDefaultCopyRawInput(rawContainerURLWithSAS.String(), rawDstContainerURLWithSAS.String())
	raw.dryrun = true
	raw.recursive = true

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		// validate that none where transferred
		c.Assert(len(mockedRPC.transfers), chk.Equals, 0)

		msg := mockedLcm.GatherAllLogs(mockedLcm.dryrunLog)
		for i := 0; i < len(blobsToInclude); i++ {
			c.Check(strings.Contains(msg[i], "DRYRUN: copy"), chk.Equals, true)
			c.Check(strings.Contains(msg[i], srcContainerURL.String()), chk.Equals, true)
			c.Check(strings.Contains(msg[i], dstContainerURL.String()), chk.Equals, true)
		}

		c.Check(testDryrunStatements(blobsToInclude, msg), chk.Equals, true)
	})
}

func (s *cmdIntegrationSuite) TestDryrunCopyBlobToBlobJson(c *chk.C) {
	bsu := getBSU()
	// set up src container
	srcContainerURL, srcContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, srcContainerURL)
	blobsToInclude := []string{"AzURE2021.jpeg"}
	scenarioHelper{}.generateBlobsFromList(c, srcContainerURL, blobsToInclude, blockBlobDefaultData)
	c.Assert(srcContainerURL, chk.NotNil)

	// set up the destination
	dstContainerURL, dstContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, dstContainerURL)
	c.Assert(dstContainerURL, chk.NotNil)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedLcm := mockedLifecycleManager{dryrunLog: make(chan string, 50)}
	mockedLcm.SetOutputFormat(common.EOutputFormat.Json()) //json format
	glcm = &mockedLcm

	// construct the raw input to simulate user input
	rawSrcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, srcContainerName)
	rawDstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw := getDefaultCopyRawInput(rawSrcContainerURLWithSAS.String(), rawDstContainerURLWithSAS.String())
	raw.dryrun = true
	raw.recursive = true

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		// validate that none where transferred
		c.Assert(len(mockedRPC.transfers), chk.Equals, 0)

		msg := <-mockedLcm.dryrunLog
		copyMessage := common.CopyTransfer{}
		errMarshal := json.Unmarshal([]byte(msg), &copyMessage)
		c.Assert(errMarshal, chk.IsNil)
		//comparing some values of copyMessage
		c.Check(strings.Compare(strings.Trim(copyMessage.Source, "/"), blobsToInclude[0]), chk.Equals, 0)
		c.Check(strings.Compare(strings.Trim(copyMessage.Destination, "/"), blobsToInclude[0]), chk.Equals, 0)
		c.Check(strings.Compare(copyMessage.EntityType.String(), common.EEntityType.File().String()), chk.Equals, 0)
		c.Check(strings.Compare(string(copyMessage.BlobType), "BlockBlob"), chk.Equals, 0)
	})
}

func (s *cmdIntegrationSuite) TestDryrunCopyS3toBlob(c *chk.C) {
	skipIfS3Disabled(c)
	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	if err != nil {
		c.Skip("S3 client credentials not supplied")
	}

	// set up src s3 bucket
	bucketName := generateBucketName()
	createNewBucketWithName(c, s3Client, bucketName, createS3ResOptions{})
	defer deleteBucket(c, s3Client, bucketName, true)
	objectList := []string{"AzURE2021.jpeg"}
	scenarioHelper{}.generateObjects(c, s3Client, bucketName, objectList)

	// initialize dst container
	dstContainerName := generateContainerName()

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedLcm := mockedLifecycleManager{dryrunLog: make(chan string, 50)}
	mockedLcm.SetOutputFormat(common.EOutputFormat.Text()) //text format
	glcm = &mockedLcm

	// construct the raw input to simulate user input
	rawSrcS3ObjectURL := scenarioHelper{}.getRawS3ObjectURL(c, "", bucketName, "AzURE2021.jpeg")
	rawDstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcS3ObjectURL.String(), rawDstContainerURLWithSAS.String())
	raw.dryrun = true
	raw.recursive = true

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		// validate that none where transferred
		c.Assert(len(mockedRPC.transfers), chk.Equals, 0)

		msg := mockedLcm.GatherAllLogs(mockedLcm.dryrunLog)
		dstPath := strings.Split(rawDstContainerURLWithSAS.String(), "?")
		c.Check(strings.Contains(msg[0], "DRYRUN: copy"), chk.Equals, true)
		c.Check(strings.Contains(msg[0], rawSrcS3ObjectURL.String()), chk.Equals, true)
		c.Check(strings.Contains(msg[0], dstPath[0]), chk.Equals, true)

		c.Check(testDryrunStatements(objectList, msg), chk.Equals, true)
	})
}

func (s *cmdIntegrationSuite) TestDryrunCopyGCPtoBlob(c *chk.C) {
	skipIfGCPDisabled(c)
	gcpClient, err := createGCPClientWithGCSSDK()
	if err != nil {
		c.Skip("GCP client credentials not supplied")
	}
	// set up src gcp bucket
	bucketName := generateBucketName()
	createNewGCPBucketWithName(c, gcpClient, bucketName)
	defer deleteGCPBucket(c, gcpClient, bucketName, true)
	blobsToInclude := []string{"AzURE2021.jpeg"}
	scenarioHelper{}.generateGCPObjects(c, gcpClient, bucketName, blobsToInclude)
	c.Assert(gcpClient, chk.NotNil)

	// initialize dst container
	dstContainerName := generateContainerName()

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedLcm := mockedLifecycleManager{dryrunLog: make(chan string, 50)}
	mockedLcm.SetOutputFormat(common.EOutputFormat.Text()) //text format
	glcm = &mockedLcm

	// construct the raw input to simulate user input
	rawSrcGCPObjectURL := scenarioHelper{}.getRawGCPObjectURL(c, bucketName, "AzURE2021.jpeg") // Use default region
	rawDstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcGCPObjectURL.String(), rawDstContainerURLWithSAS.String())
	raw.dryrun = true
	raw.recursive = true

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		// validate that none where transferred
		c.Assert(len(mockedRPC.transfers), chk.Equals, 0)

		msg := mockedLcm.GatherAllLogs(mockedLcm.dryrunLog)
		dstPath := strings.Split(rawDstContainerURLWithSAS.String(), "?")
		c.Check(strings.Contains(msg[0], "DRYRUN: copy"), chk.Equals, true)
		c.Check(strings.Contains(msg[0], rawSrcGCPObjectURL.String()), chk.Equals, true)
		c.Check(strings.Contains(msg[0], dstPath[0]), chk.Equals, true)

		c.Check(testDryrunStatements(blobsToInclude, msg), chk.Equals, true)
	})
}
