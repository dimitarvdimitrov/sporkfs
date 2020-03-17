package test

import "fmt"

type Diff interface {
	HasDiff() bool
	Diff() string
}

type fileExistenceDiff struct {
	expectedFile        string
	exists, shouldExist bool
}

func fileShouldntExistDiff(fileName string, exists bool) Diff {
	return fileExistenceDiff{
		expectedFile: fileName,
		shouldExist:  false,
		exists:       exists,
	}
}

func fileShouldExistDiff(fileName string, exists bool) Diff {
	return fileExistenceDiff{
		expectedFile: fileName,
		shouldExist:  true,
		exists:       exists,
	}
}

func (d fileExistenceDiff) HasDiff() bool {
	return d.exists != d.shouldExist
}

func (d fileExistenceDiff) Diff() string {
	if !d.HasDiff() {
		return ""
	}

	shouldSuffix := "n't"
	if d.shouldExist {
		shouldSuffix = ""
	}

	return fmt.Sprintf("file %s should%s exist", d.expectedFile, shouldSuffix)
}

type fileContentsDiff struct {
	expectedContents string
	actualContents   string
}

func (d fileContentsDiff) HasDiff() bool {
	return d.expectedContents != d.actualContents
}

func (d fileContentsDiff) Diff() string {
	return fmt.Sprintf("expected: %q\nactual: %q\n", d.expectedContents, d.actualContents)
}

type nodeDiff struct {
	nodes []Diff
}

func (d nodeDiff) HasDiff() bool {
	for _, n := range d.nodes {
		if n.HasDiff() {
			return true
		}
	}
	return false
}

func (d nodeDiff) Diff() string {
	diff := fmt.Sprintf("# of nodes: %d\n", len(d.nodes))
	for i, n := range d.nodes {
		diff += fmt.Sprintf("node %d; has diff: %t\n%s\n", i, n.HasDiff(), n.Diff())
	}
	return diff
}

type timeoutDiff struct{}

func (timeoutDiff) HasDiff() bool {
	return true
}

func (timeoutDiff) Diff() string {
	return "action timed out and diff couldn't be gathered"
}
