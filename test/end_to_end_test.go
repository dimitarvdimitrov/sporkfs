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

	_, err = os.Stat(tmpFile.Name())
	s.NoError(err)
}

func (s *E2eSuite) TestCreateAndWrite() {
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

func (s *E2eSuite) TestCreateEmptyDirectory() {
	tmpDir, err := ioutil.TempDir(s.mountDir, "empty-dir-")
	s.NoError(err)
	s.NoError(os.RemoveAll(tmpDir))
}

func (s *E2eSuite) TestCreateDirectoryWithFiles() {
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

	_, err = os.Stat(tmpFile.Name())
	s.NoError(err)

	filesFound, err := ioutil.ReadDir(tmpDir)
	s.NoError(err)
	s.Len(filesFound, 1)
	s.Equal(filesFound[0].Name(), path.Base(tmpFile.Name()))
}

func (s *E2eSuite) SetupSuite() {
	var err error
	s.dataDir, err = ioutil.TempDir("", "spork-test")
	s.Require().NoError(err)

	s.mountDir, err = ioutil.TempDir("/mnt", "spork")
	s.Require().NoError(err)

	s.T().Log(os.Getwd())

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
