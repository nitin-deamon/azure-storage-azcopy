// Copyright © Microsoft <wastore@microsoft.com>
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

package e2etest

import (
	"testing"

	"github.com/nitin-deamon/azure-storage-azcopy/v10/common"
)

// Purpose: Tests for preserving transferred properties, info and ACLs.  Both those possessed by the original source file/folder,
//   and those specified on the command line

func TestProperties_NameValueMetadataIsPreservedS2S(t *testing.T) {
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.AllS2S(), eValidate.Auto(), params{
		recursive: true,
	}, nil, testFiles{
		defaultSize: "1K",
		shouldTransfer: []interface{}{
			f("filea", with{nameValueMetadata: map[string]string{"foo": "abc", "bar": "def"}}),
			folder("fold1", with{nameValueMetadata: map[string]string{"other": "xyz"}}),
		},
	}, EAccountType.Standard(), "")
}

func TestProperties_NameValueMetadataCanBeUploaded(t *testing.T) {
	expectedMap := map[string]string{"foo": "abc", "bar": "def"}

	RunScenarios(t, eOperation.Copy(), eTestFromTo.AllUploads(), eValidate.Auto(), params{
		recursive: true,
		metadata:  "foo=abc;bar=def",
	}, nil, testFiles{
		defaultSize: "1K",
		shouldTransfer: []interface{}{
			folder("", verifyOnly{with{nameValueMetadata: expectedMap}}), // root folder
			f("filea", verifyOnly{with{nameValueMetadata: expectedMap}}),
		},
	}, EAccountType.Standard(), "")
}

func TestProperties_HNSACLs(t *testing.T) {
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.Other(common.EFromTo.BlobBlob()), eValidate.Auto(), params{
		recursive:              true,
		preserveSMBPermissions: true, // this flag is deprecated, but still held over to avoid breaking.
	}, nil, testFiles{
		defaultSize: "1K",
		shouldTransfer: []interface{}{
			folder(""),
			f("filea", with{adlsPermissionsACL: "user::rwx,group::rwx,other::--x"}),
		},
	}, EAccountType.HierarchicalNamespaceEnabled(), "")
}
