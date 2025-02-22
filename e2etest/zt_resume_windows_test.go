package e2etest

import (
	"testing"

	"github.com/nitin-deamon/azure-storage-azcopy/v10/common"
)

func TestResume_FolderState(t *testing.T) {
	// Create a child file before the folder itself, then persist the properties of the folder upon resume, knowing that we created the folder.
	RunScenarios(t, eOperation.CopyAndSync()|eOperation.Resume(), eTestFromTo.Other(common.EFromTo.LocalFile(), common.EFromTo.FileFile(), common.EFromTo.FileLocal()), eValidate.Auto(), params{
		recursive:       true,
		preserveSMBInfo: true,
		debugSkipFiles: []string{
			"a",
		},
	}, nil, testFiles{
		defaultSize: "1K",

		shouldTransfer: []interface{}{
			folder(""),
			folder("a", with{smbAttributes: 2}),
			f("a/b"),
		},
	}, EAccountType.Standard(), "")
}

func TestResume_NoCreateFolder(t *testing.T) {
	// Don't create the folder "ourselves", and let AzCopy find that out on a resume.
	RunScenarios(t, eOperation.Copy()|eOperation.Resume(), eTestFromTo.Other(common.EFromTo.LocalFile(), common.EFromTo.FileFile(), common.EFromTo.FileLocal()), eValidate.Auto(), params{
		recursive:       true,
		preserveSMBInfo: true,
		debugSkipFiles: []string{
			"a",
			"a/b",
		},
	}, &hooks{
		beforeResumeHook: func(h hookHelper) {
			// Create the folder in the middle of the transfer
			h.CreateFile(folder("a"), false)
		},
	}, testFiles{
		defaultSize: "1K",

		shouldTransfer: []interface{}{
			folder(""),
			folder("a"),
			f("a/b"),
			f("c"),
		},
		shouldSkip: []interface{}{
			folder("a", with{smbAttributes: 2}),
		},
	}, EAccountType.Standard(), "")
}
