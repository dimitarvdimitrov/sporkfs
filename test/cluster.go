package test

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/dimitarvdimitrov/sporkfs/log"

	"github.com/dimitarvdimitrov/sporkfs/raft"
	"github.com/dimitarvdimitrov/sporkfs/spork"
)

const basePort = 7340

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
	now := time.Now().Format("15:04:05")
	files = make([]*os.File, len(configs))

	for i, cfg := range configs {
		files[i], err = os.Create(fmt.Sprintf("./log/%s.%s.log", now, cfg.ThisPeer))
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

func (c *Cluster) StopNode(node int) error {
	return c.nodes[node].Stop()
}

func (c *Cluster) StartNode(node int) error {
	return c.nodes[node].Start()
}

func (c *Cluster) Destroy() error {
	var (
		err  error
		errM sync.Mutex
		wg   sync.WaitGroup
	)

	for i := range c.nodes {
		i := i
		wg.Add(1)
		go func() {
			if e := c.nodes[i].Destroy(); e != nil {
				errM.Lock()
				err = e
				errM.Unlock()
			}
			_ = c.logFiles[i].Close()
			wg.Done()
		}()
	}
	wg.Wait()
	return err
}

func (c *Cluster) WaitReady(duration time.Duration) error {
	ready := make(chan struct{})

	go func() {
		for allReady := false; !allReady; time.Sleep(time.Millisecond) {
			allReady = true
			for _, n := range c.nodes {
				if n.Stopped() {
					continue
				}
				select {
				case <-n.Ready():
				default:
					allReady = false
				}
			}
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

// TODO make those checkers (FileContains et.al) include all nodes even stopped ones, so that it's easier to debug which node
// 	actually breaks. Now the indexes reported by the nodeDiff aren't absolute, just relative to the number of live nodes.

// FileContains checks if all the nodes in the cluster have the same contents for a file. If
// the files aren't consistent with what is expected within the checkTimout, an error is returned.
func (c *Cluster) FileContains(fileName string, contents []byte) Diff {
	checkers := make([]func() Diff, 0, len(c.nodes))
	for _, n := range c.nodes {
		n := n
		if n.Stopped() {
			continue
		}
		checkers = append(checkers, func() Diff { return n.FileDiff(fileName, contents) })
	}
	return c.checkConsistent(checkers)
}

func (c *Cluster) FileExists(fileName string) Diff {
	checkers := make([]func() Diff, 0, len(c.nodes))
	for _, n := range c.nodes {
		n := n
		if n.Stopped() {
			continue
		}
		checkers = append(checkers, func() Diff { return n.FileExists(fileName) })
	}
	return c.checkConsistent(checkers)
}

func (c *Cluster) FileDoesntExist(fileName string) Diff {
	checkers := make([]func() Diff, 0, len(c.nodes))
	for _, n := range c.nodes {
		n := n
		if n.Stopped() {
			continue
		}
		checkers = append(checkers, func() Diff { return n.FileDoesntExist(fileName) })
	}
	return c.checkConsistent(checkers)
}

// checkConsistent checks if the provided checkers all return true.
// It returns when the checkers are all true or checkTimeout is reached.
func (c *Cluster) checkConsistent(checkers []func() Diff) Diff {
	allConsistent := make(chan struct{})
	done := make(chan struct{})
	checkId := rand.Int()
	var finalDiff Diff
	var finalDiffM sync.Mutex

	go func() {
		for haveDiffs := true; haveDiffs; time.Sleep(time.Millisecond * 10) {
			select {
			case <-done:
				return
			default:
			}

			diff := nodeDiff{[]Diff{}}
			haveDiffs = false
			log.Debug("doing a check", zap.Int("check_id", checkId))
			for _, check := range checkers {
				log.Debug("starting to perform a check", zap.Int("check_id", checkId))
				newDiff := check()
				log.Debug("performed a check", zap.Int("check_id", checkId))
				if newDiff.HasDiff() {
					log.Debug("new diff has diffs", zap.Int("check_id", checkId))
					haveDiffs = true
				}
				diff.nodes = append(diff.nodes, newDiff)
			}
			finalDiffM.Lock()
			finalDiff = diff
			finalDiffM.Unlock()
			log.Debug("finished a check", zap.Int("check_id", checkId))
		}
		close(allConsistent)
	}()

	timeout := time.NewTimer(c.checkTimeout).C
	select {
	case <-timeout:
		log.Debug("timing out on a check", zap.Int("check_id", checkId))
		close(done)

		finalDiffM.Lock()
		defer finalDiffM.Unlock()

		if finalDiff == nil {
			return timeoutDiff{}
		}
		return finalDiff
	case <-allConsistent:
		log.Debug("everything consistent", zap.Int("check_id", checkId))
		return finalDiff
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
