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
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/nitin-deamon/azure-storage-azcopy/v10/common"
)

// encapsulates the interaction with the AzCopy instance that is being tested
// the flag names should be captured here so that in case they change, only 1 place needs to be updated
type TestRunner struct {
	flags map[string]string
}

func newTestRunner() TestRunner {
	return TestRunner{flags: make(map[string]string)}
}

var isLaunchedByDebugger = func() bool {
	// gops executable must be in the path. See https://github.com/google/gops
	gopsOut, err := exec.Command("gops", strconv.Itoa(os.Getppid())).Output()
	if err == nil && strings.Contains(string(gopsOut), "\\dlv.exe") {
		// our parent process is (probably) the Delve debugger
		return true
	}
	return false
}()

func (t *TestRunner) SetAllFlags(p params) {
	set := func(key string, value interface{}, dflt interface{}, formats ...string) {
		if value == dflt {
			return // nothing to do. The flag is not supposed to be set
		}

		format := "%v"
		if len(formats) > 0 {
			format = formats[0]
		}

		t.flags[key] = fmt.Sprintf(format, value)
	}

	// TODO: TODO: nakulkar-msft there will be many more to add here
	set("recursive", p.recursive, false)
	set("include-path", p.includePath, "")
	set("exclude-path", p.excludePath, "")
	set("include-pattern", p.includePattern, "")
	set("exclude-pattern", p.excludePattern, "")
	set("include-after", p.includeAfter, "")
	set("include-pattern", p.includePattern, "")
	set("exclude-path", p.excludePath, "")
	set("exclude-pattern", p.excludePattern, "")
	set("cap-mbps", p.capMbps, float32(0))
	set("block-size-mb", p.blockSizeMB, float32(0))
	set("s2s-detect-source-changed", p.s2sSourceChangeValidation, false)
	set("metadata", p.metadata, "")
	set("cancel-from-stdin", p.cancelFromStdin, false)
	set("preserve-smb-info", p.preserveSMBInfo, false)
	set("preserve-smb-permissions", p.preserveSMBPermissions, false)
	set("backup", p.backupMode, false)
	set("blob-tags", p.blobTags, "")
	set("blob-type", p.blobType, "")
	set("s2s-preserve-blob-tags", p.s2sPreserveBlobTags, false)
	set("cpk-by-name", p.cpkByName, "")
	set("cpk-by-value", p.cpkByValue, false)
	set("is-object-dir", p.isObjectDir, false)
	set("debug-skip-files", strings.Join(p.debugSkipFiles, ";"), "")
}

func (t *TestRunner) SetAwaitOpenFlag() {
	t.flags["await-open"] = "true"
}

func (t *TestRunner) computeArgs() []string {
	args := make([]string, 0)
	for key, value := range t.flags {
		args = append(args, fmt.Sprintf("--%s=%s", key, value))
	}

	return append(args, "--output-type=json")
}

// execCommandWithOutput replaces Go's exec.Command().Output, but appends an extra parameter and
// breaks up the c.Run() call into its component parts. Both changes are to assist debugging
func (t *TestRunner) execDebuggableWithOutput(name string, args []string, afterStart func() string, chToStdin <-chan string) ([]byte, error) {
	debug := isLaunchedByDebugger
	if debug {
		args = append(args, "--await-continue")
	}
	c := exec.Command(name, args...)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	stdin, err := c.StdinPipe()
	if err != nil {
		return make([]byte, 0), err
	}

	c.Stdout = &stdout
	c.Stderr = &stderr

	//instead of err := c.Run(), we do the following
	runErr := c.Start()
	if runErr == nil {
		defer func() {
			_ = c.Process.Kill() // in case we never finish c.Wait() below, and get paniced or killed
		}()

		if debug {
			beginAzCopyDebugging(stdin)
		}

		// perform a specific post-start action
		if afterStart != nil {
			msgToApp := afterStart() // perform a local action, here in the test suite, that may optionally produce a message to send to the the app
			if msgToApp != "" {
				_, _ = stdin.Write([]byte(msgToApp + "\n")) // TODO: maybe change this to use chToStdIn
			}
		}

		// allow on-going messages to stdin
		if chToStdin != nil {
			go func() {
				for {
					msg, ok := <-chToStdin
					if ok {
						_, _ = stdin.Write([]byte(msg + "\n"))
					} else {
						break
					}
				}
			}()
		}

		// wait for completion
		runErr = c.Wait()
	}

	// back to normal exec.Cmd.Output() processing
	if runErr != nil {
		if ee, ok := runErr.(*exec.ExitError); ok {
			ee.Stderr = stderr.Bytes()
		}
	}
	return stdout.Bytes(), runErr
}

func (t *TestRunner) ExecuteAzCopyCommand(operation Operation, src, dst string, afterStart func() string, chToStdin <-chan string) (CopyOrSyncCommandResult, bool, error) {
	capLen := func(b []byte) []byte {
		if len(b) < 1024 {
			return b
		} else {
			return append(b[:1024], byte('\n'))
		}
	}

	verb := ""
	switch operation {
	case eOperation.Copy():
		verb = "copy"
	case eOperation.Sync():
		verb = "sync"
	case eOperation.Remove():
		verb = "remove"
	case eOperation.Resume():
		verb = "jobs resume"
	default:
		panic("unsupported operation type")
	}

	args := append(strings.Split(verb, " "), src, dst)
	if operation == eOperation.Remove() {
		args = args[:2]
	} else if operation == eOperation.Resume() {
		args = args[:3]
	}
	args = append(args, t.computeArgs()...)
	out, err := t.execDebuggableWithOutput(GlobalInputManager{}.GetExecutablePath(), args, afterStart, chToStdin)

	wasClean := true
	stdErr := make([]byte, 0)
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			stdErr = capLen(ee.Stderr) // cap length of this, because it can be a panic. But don't cap stdout, because we need its last line in newCopyOrSyncCommandResult
			if len(stdErr) > 0 {
				wasClean = false // something was written to stderr, probably a panic
			}
		}
	}

	if wasClean {
		// either it succeeded, for it returned a failure code in a clean (non-panic) way.
		// In both cases, we want out to be parsed, to get us the job ID.  E.g. maybe 1 transfer out of several failed,
		// and that's what we'er actually testing for (so can't treat this as a fatal error).
		r, ok := newCopyOrSyncCommandResult(string(out))
		if ok {
			return r, true, err
		} else {
			err = fmt.Errorf("could not parse AzCopy output. Run error, if any, was '%w'", err)
		}
	}

	return CopyOrSyncCommandResult{},
		false,
		fmt.Errorf("azcopy run error: %w\n  with stderr: %s\n  and stdout: %s\n  from args %v", err, stdErr, out, args)
}

func (t *TestRunner) SetTransferStatusFlag(value string) {
	t.flags["with-status"] = value
}

func (t *TestRunner) ExecuteJobsShowCommand(jobID common.JobID) (JobsShowCommandResult, error) {
	args := append([]string{"jobs", "show", jobID.String()}, t.computeArgs()...)
	out, err := exec.Command(GlobalInputManager{}.GetExecutablePath(), args...).Output()
	if err != nil {
		return JobsShowCommandResult{}, err
	}

	return newJobsShowCommandResult(string(out)), nil
}

type CopyOrSyncCommandResult struct {
	jobID       common.JobID
	finalStatus common.ListSyncJobSummaryResponse
}

func newCopyOrSyncCommandResult(rawOutput string) (CopyOrSyncCommandResult, bool) {
	lines := strings.Split(rawOutput, "\n")

	// parse out the final status
	// -2 because the last line is empty
	if len(lines) < 2 {
		return CopyOrSyncCommandResult{}, false
	}
	finalLine := lines[len(lines)-2]
	finalMsg := common.JsonOutputTemplate{}
	err := json.Unmarshal([]byte(finalLine), &finalMsg)
	if err != nil {
		return CopyOrSyncCommandResult{}, false
	}

	jobSummary := common.ListSyncJobSummaryResponse{} // this is a superset of ListJobSummaryResponse, so works for both copy and sync
	err = json.Unmarshal([]byte(finalMsg.MessageContent), &jobSummary)
	if err != nil {
		return CopyOrSyncCommandResult{}, false
	}

	return CopyOrSyncCommandResult{jobID: jobSummary.JobID, finalStatus: jobSummary}, true
}

func (c *CopyOrSyncCommandResult) GetTransferList(status common.TransferStatus) ([]common.TransferDetail, error) {
	runner := newTestRunner()
	runner.SetTransferStatusFlag(status.String())

	// invoke AzCopy to get the status from the plan files
	result, err := runner.ExecuteJobsShowCommand(c.jobID)
	if err != nil {
		return make([]common.TransferDetail, 0), err
	}

	return result.transfers, nil
}

type JobsShowCommandResult struct {
	jobID     common.JobID
	transfers []common.TransferDetail
}

func newJobsShowCommandResult(rawOutput string) JobsShowCommandResult {
	lines := strings.Split(rawOutput, "\n")

	// parse out the final status
	// -2 because the last line is empty
	finalLine := lines[len(lines)-2]
	finalMsg := common.JsonOutputTemplate{}
	err := json.Unmarshal([]byte(finalLine), &finalMsg)
	if err != nil {
		panic(err)
	}

	listTransfersResponse := common.ListJobTransfersResponse{}
	err = json.Unmarshal([]byte(finalMsg.MessageContent), &listTransfersResponse)
	if err != nil {
		panic(err)
	}

	return JobsShowCommandResult{jobID: listTransfersResponse.JobID, transfers: listTransfersResponse.Details}
}
