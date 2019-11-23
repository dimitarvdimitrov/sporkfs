package main

import (
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"testing"

	"github.com/stretchr/testify/suite"
)

type E2eSuite struct {
	suite.Suite

	mountDir, dataDir string
	sporkProcess      *exec.Cmd
}

func (s *E2eSuite) TestCreateEmptyFile() {
	tmpFile, err := ioutil.TempFile(s.mountDir, "empty-file-")
	s.NoError(err)
	defer func() {
		s.NoError(tmpFile.Close())
		s.NoError(os.Remove(tmpFile.Name()))
	}()

	s.FileExists(tmpFile.Name())
}

func (s *E2eSuite) TestCreateFileAndWrite() {
	tmpFile, err := ioutil.TempFile(s.mountDir, "non-empty-file-")
	s.NoError(err)
	defer func() {
		s.NoError(tmpFile.Close())
		s.NoError(os.Remove(tmpFile.Name()))
	}()

	contentToWrite := []byte("this is a test")
	bytesWritten, err := tmpFile.Write(contentToWrite)
	s.NoError(err)
	s.Equal(len(contentToWrite), bytesWritten)

	contentFound, err := ioutil.ReadFile(tmpFile.Name())
	s.NoError(err)
	s.Equal(contentToWrite, contentFound)
}

func (s *E2eSuite) TestCreateEmptyDir() {
	tmpDir, err := ioutil.TempDir(s.mountDir, "empty-dir-")
	s.NoError(err)
	defer func() {
		s.NoError(os.RemoveAll(tmpDir))
	}()

	s.DirExists(tmpDir)
}

func (s *E2eSuite) TestCreateDirWithFiles() {
	tmpDir, err := ioutil.TempDir(s.mountDir, "non-empty-dir-")
	s.NoError(err)
	defer func() {
		s.NoError(os.RemoveAll(tmpDir))
	}()

	tmpFile, err := ioutil.TempFile(tmpDir, "specific-file")
	s.NoError(err)
	defer func() {
		s.NoError(tmpFile.Close())
	}()

	s.FileExists(tmpFile.Name())
}

func (s *E2eSuite) TestRenameFile() {
	tmpFile, err := ioutil.TempFile(s.mountDir, "name-1")
	s.NoError(err)
	s.NoError(tmpFile.Close())

	oldName := tmpFile.Name()
	newName := path.Dir(tmpFile.Name()) + "/name-2"
	s.NoError(os.Rename(oldName, newName))
	defer func() {
		s.NoError(os.Remove(newName))
	}()

	s.FileExists(newName)

	_, err = os.Stat(oldName)
	s.Error(err)
}

func (s *E2eSuite) TestMoveFile() {
	// setup
	firstParent, err := ioutil.TempDir(s.mountDir, "")
	s.NoError(err)
	defer func() {
		s.NoError(os.RemoveAll(firstParent))
	}()

	secondParent, err := ioutil.TempDir(s.mountDir, "")
	s.NoError(err)
	defer func() {
		s.NoError(os.RemoveAll(secondParent))
	}()

	tmpFile, err := ioutil.TempFile(firstParent, "")
	s.NoError(err)
	s.NoError(tmpFile.Close())

	oldName := tmpFile.Name()
	newBaseName := "another-name"
	newName := secondParent + "/" + newBaseName

	// actual testing
	err = os.Rename(oldName, newName)
	s.NoError(err)

	// test new one appears
	s.FileExists(newName)

	// test old one is gone
	_, err = os.Stat(oldName)
	s.Error(err)

	// test second parent dir has a child file
	files, err := ioutil.ReadDir(secondParent)
	s.NoError(err)
	s.Len(files, 1)
	s.Equal(newBaseName, files[0].Name())
}

func (s *E2eSuite) TestRenameDir() {
	// setup
	testDir, err := ioutil.TempDir(s.mountDir, "")
	s.NoError(err)

	tmpFile, err := ioutil.TempFile(testDir, "")
	s.NoError(err)
	s.NoError(tmpFile.Close())

	oldName := testDir
	newBaseName := "same-dir-new-name"
	newName := path.Dir(testDir) + "/" + newBaseName

	// actual testing
	err = os.Rename(oldName, newName)
	s.NoError(err)
	defer func() {
		s.NoError(os.RemoveAll(newName))
	}()

	// test new one appears
	s.DirExists(newName)

	// test old one is gone
	_, err = os.Stat(oldName)
	s.Error(err)

	// test second parent dir has a child file
	s.FileExists(newName + "/" + path.Base(tmpFile.Name()))
}

func (s *E2eSuite) SetupSuite() {
	var err error
	s.dataDir, err = ioutil.TempDir("", "spork-test")
	s.Require().NoError(err)

	s.mountDir, err = ioutil.TempDir("/mnt", "spork")
	s.Require().NoError(err)

	s.sporkProcess = exec.Command("../bin/sporkfs", s.mountDir, s.dataDir)
	s.Require().NoError(s.sporkProcess.Start())
}

func (s *E2eSuite) TearDownSuite() {
	_ = s.sporkProcess.Process.Kill()
	_, _ = s.sporkProcess.Process.Wait()

	s.NoError(os.RemoveAll(s.dataDir))
	s.NoError(os.RemoveAll(s.mountDir))
}

func TestE2e(t *testing.T) {
	suite.Run(t, new(E2eSuite))
}
