package test

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/dimitarvdimitrov/sporkfs/spork"
)

type Node struct {
	ready chan struct{}

	cfg            spork.Config
	cfgFile        string
	cmd            *exec.Cmd
	stdout         *logElectionWatcher
	binaryLocation string
}

// NewNode returns a node that uses the spork binary to run nodes. The binary's stdout & stderr are redirected
// into out.
func NewNode(binaryLocation string, cfg spork.Config, out io.Writer) (*Node, error) {
	cfgFile, err := ioutil.TempFile("", "")
	if err != nil {
		return nil, err
	}
	defer cfgFile.Close()

	err = toml.NewEncoder(cfgFile).Encode(cfg)
	if err != nil {
		return nil, err
	}

	return &Node{
		cfgFile:        cfgFile.Name(),
		stdout:         newLogElectionWatcher(out),
		binaryLocation: binaryLocation,
		cfg:            cfg,
	}, nil
}

func (n *Node) Destroy() error {
	defer os.Remove(n.cfgFile)
	defer os.RemoveAll(n.cfg.DataDir)
	defer os.RemoveAll(n.cfg.MountPoint)

	return n.Stop()
}

func (n *Node) Start() error {
	n.setupCmd()
	go n.watchReady()
	return n.cmd.Start()
}

func (n *Node) setupCmd() {
	cmd := exec.Command(n.binaryLocation, n.cfgFile)
	cmd.Stdout = n.stdout
	cmd.Stderr = n.stdout
	n.ready = make(chan struct{})
}

func (n *Node) watchReady() {
	select {
	case <-n.ready: // someone closes ready on a Stop of the node
	case <-n.stdout.elections:
		close(n.ready)
	}
}

// Stop send a stop signal to spork and waits for it to terminate.
func (n *Node) Stop() error {
	if n.cmd == nil {
		return nil
	}

	err := n.cmd.Process.Signal(syscall.SIGINT)
	if err != nil {
		return err
	}

	err = n.cmd.Wait()

	select {
	case <-n.ready: // already closed
	default:
		close(n.ready)
	}

	n.cmd = nil
	if _, ok := err.(*exec.ExitError); ok {
		// stopping was successful, but spork errored, but we don't care
		return nil
	}
	return err
}

func (n *Node) WaitReady() {
	<-n.ready
}

func (n *Node) FileContains(fileName string, contents []byte) bool {
	actualContents, err := ioutil.ReadFile(n.filePath(fileName))
	return err == nil && bytes.Equal(actualContents, contents)
}

func (n *Node) filePath(fileName string) string {
	return n.cfg.MountPoint + "/" + fileName
}

func (n *Node) FileExists(fileName string) bool {
	_, err := os.Stat(n.filePath(fileName))
	return err == nil
}

func (n *Node) OpenFile(fileName string) (*os.File, error) {
	return os.OpenFile(n.filePath(fileName), os.O_RDWR, 0)
}

func (n *Node) CreateFile(fileName string) (*os.File, error) {
	return os.Create(n.filePath(fileName))
}

func (n *Node) DeleteFile(fileName string) error {
	return os.Remove(n.filePath(fileName))
}

func (n *Node) RenameFile(fileName, newName string) error {
	return os.Rename(n.filePath(fileName), n.filePath(newName))
}
