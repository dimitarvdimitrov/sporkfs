package test

import (
	"io"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	// checkTimeout is how long to wait until a check is considered failed - e.g. how long after creating a file
	// to wait until the file is present on all the nodes
	checkTimeout  = time.Second
	numberOfNodes = 6
	redundancy    = 3
)

type E2eSuite struct {
	suite.Suite

	require *require.Assertions

	cluster *Cluster
}

func (s *E2eSuite) SetupSuite() {
	rand.Seed(time.Now().UnixNano())
	s.require = s.Require()

	binaryDir, _ := os.LookupEnv("SPORK_BIN")
	s.require.NotZero(binaryDir, "env var SPORK_BIN needs to be set to a path with a spork binary")

	c, err := NewCluster(checkTimeout, binaryDir, redundancy, numberOfNodes)
	s.require.NoError(err)
	s.require.NoError(c.Start())

	s.cluster = c
}

func (s *E2eSuite) TearDownSuite() {
	s.NoError(s.cluster.Destroy())
}

func (s *E2eSuite) SetupTest() {
	s.NoError(s.cluster.WaitReady(time.Second * 10))
}

func (s *E2eSuite) TestCreateFile() {
	const fileName = "create-file-text.txt"

	node := rand.Intn(numberOfNodes)
	f, err := s.cluster.CreateFile(fileName, node)
	s.NoError(err, "node: %d", node)
	s.NoError(f.Close(), "node: %d", node)

	s.NoDiff(s.cluster.FileExists(fileName))
}

func (s *E2eSuite) TestWriteToFile() {
	const (
		fileName = "write-file.txt"
		contents = "hope this works!\n"
	)

	node := rand.Intn(numberOfNodes)
	f, err := s.cluster.CreateFile(fileName, node)
	s.NoError(err, "node: %d", node)
	_, err = f.WriteString(contents)
	s.NoError(err, "node: %d", node)
	s.NoError(f.Close(), "node: %d", node)
	s.NoDiff(s.cluster.FileContains(fileName, []byte(contents)))
}

func (s *E2eSuite) TestDeleteFile() {
	const fileName = "delete-file.txt"

	node1, node2 := rand.Intn(numberOfNodes), rand.Intn(numberOfNodes)
	f, err := s.cluster.CreateFile(fileName, node1)
	s.NoError(err, "node: %d", node1)
	s.NoError(f.Close(), "node: %d", node1)
	s.NoDiff(s.cluster.FileExists(fileName))
	s.NoError(s.cluster.DeleteFile(fileName, node2), "node: %d", node2)
	s.NoDiff(s.cluster.FileDoesntExist(fileName))
}

func (s *E2eSuite) TestRenameFile() {
	const (
		fileName = "rename-file.txt"
		newName  = "renamed-file.txt"
	)
	node1, node2 := rand.Intn(numberOfNodes), rand.Intn(numberOfNodes)
	f, err := s.cluster.CreateFile(fileName, node1)
	if s.NoError(err, "node: %d", node1) {
		s.NoError(f.Close(), "node: %d", node1)
	}
	s.NoDiff(s.cluster.FileExists(fileName))
	s.NoError(s.cluster.RenameFile(fileName, newName, node2), "node: %d", node2)
	s.NoDiff(s.cluster.FileDoesntExist(fileName))
	s.NoDiff(s.cluster.FileExists(newName))
}

func (s *E2eSuite) TestSimultaneousWrites() {
	const (
		fileName = "simultaneous-writes.txt"
	)

	node1, node2 := rand.Intn(numberOfNodes), rand.Intn(numberOfNodes)

	f0, err := s.cluster.CreateFile(fileName, node1)
	s.NoError(err, "node %d", node1)
	s.NoDiff(s.cluster.FileExists(fileName))

	f2, err := s.cluster.OpenFile(fileName, node2)
	s.NoError(err, "node %d", node1)

	_, err = f2.WriteString("hello")
	s.NoError(err, "node %d", node2)

	_, err = f0.WriteString("world")
	s.NoError(err, "node %d", node1)

	var wg sync.WaitGroup

	closeFile := func(c io.Closer) {
		_ = c.Close()
		wg.Done()
	}

	wg.Add(2)
	go closeFile(f0)
	go closeFile(f2)
	wg.Wait()

	diff1 := s.cluster.FileContains(fileName, []byte("hello"))
	diff2 := s.cluster.FileContains(fileName, []byte("world"))

	s.True(!diff1.HasDiff() || !diff2.HasDiff())
}

func (s *E2eSuite) TestRedundancy() {
	randNode := rand.Intn(numberOfNodes)
	err := s.cluster.StopNode(randNode)
	s.NoError(err)

	secondNode := (randNode + 1) % numberOfNodes
	err = s.cluster.StopNode(secondNode)
	s.NoError(err)

	s.NoError(s.cluster.WaitReady(time.Second * 10))

	defer func() { s.NoError(s.cluster.StartNode(secondNode)) }()
	defer func() { s.NoError(s.cluster.StartNode(randNode)) }()

	const (
		fileName = "redundancy.txt"
		contents = "this better work!\n"
	)

	nodeToWriteTo := (randNode + 2) % numberOfNodes

	f, err := s.cluster.CreateFile(fileName, nodeToWriteTo)
	s.NoError(err, "node: %d", nodeToWriteTo)
	_, err = f.WriteString(contents)
	s.NoError(err, "node: %d", nodeToWriteTo)
	s.NoError(f.Close(), "node: %d", nodeToWriteTo)
	s.NoDiff(s.cluster.FileContains(fileName, []byte(contents)))
}

func (s *E2eSuite) TestSelfHealing() {
	randNode := rand.Intn(numberOfNodes)
	err := s.cluster.StopNode(randNode)
	s.NoError(err)

	secondNode := (randNode + 1) % numberOfNodes
	err = s.cluster.StopNode(secondNode)
	s.NoError(err)

	const (
		fileName = "self-healing.txt"
		contents = "this better work!\n"
	)

	s.NoError(s.cluster.WaitReady(time.Second * 10))

	nodeToWriteTo := (randNode + 2) % numberOfNodes
	f, err := s.cluster.CreateFile(fileName, nodeToWriteTo)
	s.NoError(err, "node: %d", nodeToWriteTo)

	_, err = f.WriteString(contents)
	s.NoError(err, "node: %d", nodeToWriteTo)
	s.NoError(f.Close(), "node: %d", nodeToWriteTo)
	s.NoDiff(s.cluster.FileContains(fileName, []byte(contents)))

	s.NoError(s.cluster.StartNode(secondNode))
	s.NoError(s.cluster.StartNode(randNode))
	s.NoError(s.cluster.WaitReady(time.Second * 10))

	s.NoDiff(s.cluster.FileContains(fileName, []byte(contents)))
}

func (s *E2eSuite) NoDiff(d Diff, msgAndArgs ...interface{}) bool {
	s.T().Helper()
	if d.HasDiff() {
		s.Fail(d.Diff(), msgAndArgs...)
		return false
	}
	return true
}

func TestE2e(t *testing.T) {
	if err := os.RemoveAll("./log"); err != nil && err != os.ErrNotExist {
		t.FailNow()
	}
	if err := os.Mkdir("./log", 0755); err != nil {
		t.FailNow()
	}
	// we run the tests multiple times so that catching a race condition is more likely
	for i := 0; i < 10; i++ {
		suite.Run(t, new(E2eSuite))
	}
}
