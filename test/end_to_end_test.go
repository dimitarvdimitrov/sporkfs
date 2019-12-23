package main

import (
	"context"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/dimitarvdimitrov/sporkfs/fuse"
	"github.com/dimitarvdimitrov/sporkfs/log"
	"github.com/dimitarvdimitrov/sporkfs/raft"
	"github.com/dimitarvdimitrov/sporkfs/spork"
	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/dimitarvdimitrov/sporkfs/store/data"
	"github.com/dimitarvdimitrov/sporkfs/store/inventory"
	"github.com/dimitarvdimitrov/sporkfs/store/remote"
	"github.com/seaweedfs/fuse/fs"
	"github.com/seaweedfs/fuse/fs/fstestutil"
	"github.com/stretchr/testify/suite"
)

type E2eSuite struct {
	suite.Suite

	spork.Config
	cfgFile       string
	sporkInstance *fstestutil.Mount
}

func (s *E2eSuite) TestCreateEmptyFile() {
	tmpFile, err := ioutil.TempFile(s.MountPoint, "empty-file-")
	s.NoError(err)
	defer func() {
		s.NoError(tmpFile.Close())
		s.NoError(os.Remove(tmpFile.Name()))
	}()

	s.FileExists(tmpFile.Name())
}

func (s *E2eSuite) TestAppendToFile() {
	tmpFile, err := ioutil.TempFile(s.MountPoint, "file-to-append-to-")
	s.NoError(err)
	defer func() {
		s.NoError(os.Remove(tmpFile.Name()))
	}()

	contentToWrite := []byte("this is a test")
	bytesWritten, err := tmpFile.Write(contentToWrite)
	s.NoError(err)
	s.Equal(len(contentToWrite), bytesWritten)
	tmpFile.Close()

	f, err := os.OpenFile(tmpFile.Name(), os.O_APPEND|os.O_WRONLY|os.O_SYNC, store.ModeRegularFile)
	s.NoError(err)

	contentToAppend := []byte("\nappended text")
	bytesWritten, err = f.Write(contentToAppend)
	s.NoError(err)
	s.Equal(len(contentToAppend), bytesWritten)
	s.NoError(f.Close())
	time.Sleep(time.Millisecond * 10)

	contentFound, err := ioutil.ReadFile(tmpFile.Name())
	s.NoError(err)
	s.Equal(append(contentToWrite, contentToAppend...), contentFound)
}

func (s *E2eSuite) TestCreateEmptyDir() {
	tmpDir, err := ioutil.TempDir(s.MountPoint, "empty-dir-")
	s.NoError(err)
	defer func() {
		s.NoError(os.RemoveAll(tmpDir))
	}()

	s.DirExists(tmpDir)
}

func (s *E2eSuite) TestCreateDirWithFiles() {
	tmpDir, err := ioutil.TempDir(s.MountPoint, "non-empty-dir-")
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
	tmpFile, err := ioutil.TempFile(s.MountPoint, "name-1")
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
	firstParent, err := ioutil.TempDir(s.MountPoint, "")
	s.NoError(err)
	defer func() {
		s.NoError(os.RemoveAll(firstParent))
	}()

	secondParent, err := ioutil.TempDir(s.MountPoint, "")
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
	testDir, err := ioutil.TempDir(s.MountPoint, "")
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
	s.SetupConfig()

	peers := raft.NewPeerList(s.Peers)

	dataStorage, err := data.NewLocalDriver(s.DataDir + "/data")
	if err != nil {
		log.Fatalf("init data driver: %s", err)
	}
	cacheStorage, err := data.NewLocalDriver(s.DataDir + "/cache")
	if err != nil {
		log.Fatalf("init data driver: %s", err)
	}

	inv, err := inventory.NewDriver(s.DataDir + "/inventory")
	if err != nil {
		log.Fatalf("init inventory: %s", err)
	}

	fetcher, err := remote.NewFetcher(peers)
	if err != nil {
		log.Fatalf("init fetcher: %s", err)
	}

	invFiles := make(chan *store.File)

	sporkService := spork.New(dataStorage, cacheStorage, inv, fetcher, peers, invFiles)
	vfs := fuse.NewFS(&sporkService, invFiles)

	m, err := fstestutil.MountedT(s.T(), vfs, &fs.Config{
		Debug: func(msg interface{}) {
			log.Debug(msg)
		},
	})
	s.Require().NoError(err)
	s.sporkInstance = m
	s.MountPoint = m.Dir
	go vfs.WatchInvalidations(context.Background(), m.Server)
}

func (s *E2eSuite) SetupConfig() {
	var err error
	s.DataDir, err = ioutil.TempDir("/opt/spork/test/", "spork-data-")
	s.Require().NoError(err)

	err = os.Mkdir(s.DataDir+"/data", 0777)
	s.Require().NoError(err)

	s.MountPoint, err = ioutil.TempDir("", "spork-mount-")
	s.Require().NoError(err)

	s.writeCfg(s.DataDir)
}

func (s *E2eSuite) writeCfg(dataDir string) {
	s.Config = spork.Config{
		DataDir: dataDir,
		Peers: raft.Config{
			Redundancy: 1,
			AllPeers:   []string{"localhost:8080"},
			ThisPeer:   "localhost:8080",
		},
	}
	f, err := ioutil.TempFile("", "")
	s.Require().NoError(err)
	defer f.Close()

	err = toml.NewEncoder(f).Encode(s.Config)
	s.Require().NoError(err)
	s.cfgFile = f.Name()
}

func (s *E2eSuite) TearDownSuite() {
	s.sporkInstance.Close()

	s.NoError(os.RemoveAll(s.DataDir))
	s.NoError(os.RemoveAll(s.MountPoint))
	s.NoError(os.Remove(s.cfgFile))
}

func TestE2e(t *testing.T) {
	suite.Run(t, new(E2eSuite))
}
