package test

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/dimitarvdimitrov/sporkfs/raft"
	"github.com/dimitarvdimitrov/sporkfs/spork"
)

const basePort = 70

type Cluster struct {
	checkTimeout time.Duration

	nodes    []*Node
	logFiles []*os.File
}

func NewCluster(checkTimeout time.Duration, binaryLocation string, redundancy, numberOfNodes int) (*Cluster, error) {
	configs, err := generateCfg(redundancy, numberOfNodes)
	if err != nil {
		return nil, err
	}

	logFiles, err := createLogFiles(configs)
	if err != nil {
		return nil, err
	}

	nodes := make([]*Node, numberOfNodes)
	for i, cfg := range configs {
		nodes[i], err = NewNode(binaryLocation, cfg, logFiles[i])
		if err != nil {
			return nil, err
		}
	}

	return &Cluster{checkTimeout, nodes, logFiles}, nil
}

func createLogFiles(configs []spork.Config) (files []*os.File, err error) {
	files = make([]*os.File, len(configs))
	for i, cfg := range configs {
		files[i], err = os.Create(cfg.ThisPeer + ".log")
		if err != nil {
			return
		}
	}
	return
}

func generateCfg(redundancy, numberOfNodes int) ([]spork.Config, error) {
	peers := make([]string, numberOfNodes)
	for i := range peers {
		peers[i] = fmt.Sprintf("localhost:%d", basePort+i)
	}

	configs := make([]spork.Config, numberOfNodes)

	for i := range configs {
		dataDir, err := ioutil.TempDir("", "")
		if err != nil {
			return nil, err
		}
		mountPoint, err := ioutil.TempDir("", "")
		if err != nil {
			return nil, err
		}

		configs[i] = spork.Config{
			DataDir:    dataDir,
			MountPoint: mountPoint,
			Config: raft.Config{
				AllPeers:   peers,
				ThisPeer:   peers[i],
				Redundancy: redundancy,
				DataDir:    dataDir,
			},
		}
	}
	return configs, nil
}

func (c *Cluster) Start() error {
	for i, n := range c.nodes {
		err := n.Start()
		if err != nil {
			for j := 0; j < i; j++ {
				_ = c.nodes[j].Stop()
			}
			return err
		}
	}
	return nil
}

func (c *Cluster) Destroy() (err error) {
	for i := range c.nodes {
		if e := c.nodes[i].Destroy(); e != nil {
			err = e
		}

		_ = c.logFiles[i].Close()
	}
	return
}

func (c *Cluster) WaitReady(duration time.Duration) error {
	ready := make(chan struct{})

	go func() {
		for _, n := range c.nodes {
			n.WaitReady()
		}
		close(ready)
	}()

	timeout := time.NewTimer(duration).C
	select {
	case <-ready:
		return nil
	case <-timeout:
		return fmt.Errorf("reached timeout without nodes being ready")
	}
}

// FileContains checks if all the nodes in the cluster have the same contents for a file. If
// the files aren't consistent with what is expected within the checkTimout, an error is returned.
func (c *Cluster) FileContains(fileName string, contents []byte) error {
	checkers := make([]func() bool, len(c.nodes))
	for i, n := range c.nodes {
		checkers[i] = func() bool { return n.FileContains(fileName, contents) }
	}
	return c.checkConsistent(checkers)
}

func (c *Cluster) FileExists(fileName string) error {
	checkers := make([]func() bool, len(c.nodes))
	for i, n := range c.nodes {
		checkers[i] = func() bool { return n.FileExists(fileName) }
	}
	return c.checkConsistent(checkers)
}

func (c *Cluster) FileDoesntExist(fileName string) error {
	checkers := make([]func() bool, len(c.nodes))
	for i, n := range c.nodes {
		checkers[i] = func() bool { return !n.FileExists(fileName) }
	}
	return c.checkConsistent(checkers)
}

// checkConsistent checks if the provided checkers all return true (the length of the checkers needs to be
// equal to the number of nodes). It returns when the checkers are all true or checkTimeout is reached.
func (c *Cluster) checkConsistent(checkers []func() bool) error {
	allConsistent := make(chan struct{})
	done := make(chan struct{})

	go func() {
		for allChecked := false; !allChecked; time.Sleep(time.Millisecond * 10) {
			select {
			case <-done:
				return
			default:
			}

			allChecked = true
			for _, check := range checkers {
				allChecked = allChecked && check()
			}
		}
		close(allConsistent)
	}()

	timeout := time.NewTimer(c.checkTimeout).C
	select {
	case <-timeout:
		close(done)
		return fmt.Errorf("files were not eventually consistent")
	case <-allConsistent:
		return nil
	}
}

// OpenFile opens a file for reading and writing with the node with index node (which starts from 0).
func (c *Cluster) OpenFile(fileName string, node int) (*os.File, error) {
	return c.nodes[node].OpenFile(fileName)
}

// CreateFIle creates a file for reading and writing with the node with index node (which starts from 0).
func (c *Cluster) CreateFile(fileName string, node int) (*os.File, error) {
	return c.nodes[node].CreateFile(fileName)
}

func (c *Cluster) DeleteFile(fileName string, node int) error {
	return c.nodes[node].DeleteFile(fileName)
}

func (c *Cluster) RenameFile(fileName, newName string, node int) error {
	return c.nodes[node].RenameFile(fileName, newName)
}
