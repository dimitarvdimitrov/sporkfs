package test

import (
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/dimitarvdimitrov/sporkfs/spork"
)

const activityTimeout = time.Second * 2

type Node struct {
	ready, stopped chan struct{}

	cfg            spork.Config
	cfgFile        string
	cmd            *exec.Cmd
	logger         *logActivityWatcher
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
		logger:         newLogActivityWatcher(out),
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
	n.setup()
	go n.watchActivity()
	return n.cmd.Start()
}

func (n *Node) setup() {
	cmd := exec.Command(n.binaryLocation, n.cfgFile)
	cmd.Stdout = n.logger
	cmd.Stderr = n.logger
	n.cmd = cmd
	n.ready = make(chan struct{})
	n.stopped = make(chan struct{})
}

func (n *Node) watchActivity() {
	t := time.NewTimer(activityTimeout)

	for {
		select {
		case <-n.stopped:
			return

		case <-n.logger.logActivity:
			select {
			case <-n.ready: // if we were ready, we aren't anymore
				n.ready = make(chan struct{})
			default: // if we weren't ready, keep being not ready
			}

			// drain and reset the timer
			if !t.Stop() {
				select {
				case <-t.C:
				default:
				}
			}
			t.Reset(activityTimeout)

		case <-t.C:
			select {
			case <-n.ready: // already closed
			default:
				close(n.ready)
			}
			t.Reset(activityTimeout)
		}
	}
}

// Stop sends a stop signal to spork and waits for it to terminate.
func (n *Node) Stop() error {
	if n.Stopped() {
		return nil
	}
	defer close(n.stopped)

	err := n.cmd.Process.Signal(syscall.SIGINT)
	if err != nil {
		return err
	}

	err = n.cmd.Wait()
	if _, ok := err.(*exec.ExitError); ok {
		// stopping was successful, but spork errored, but we don't care
		return nil
	}
	return err
}

func (n *Node) Stopped() bool {
	select {
	case <-n.stopped:
		return true
	default:
		return false
	}
}

// Ready returns a channel that will be closed iff the node hasn't had any log activity in the configured activityTimeout
func (n *Node) Ready() <-chan struct{} {
	return n.ready
}

func (n *Node) FileDiff(fileName string, contents []byte) Diff {
	actualContents, err := ioutil.ReadFile(n.filePath(fileName))
	if err != nil {
		actualContents = []byte(err.Error())
	}

	return fileContentsDiff{
		expectedContents: string(contents),
		actualContents:   string(actualContents),
	}
}

func (n *Node) filePath(fileName string) string {
	return n.cfg.MountPoint + "/" + fileName
}

func (n *Node) FileExists(fileName string) Diff {
	_, err := os.Stat(n.filePath(fileName))
	return fileShouldExistDiff(fileName, err == nil)
}

func (n *Node) FileDoesntExist(fileName string) Diff {
	_, err := os.Stat(n.filePath(fileName))
	return fileShouldntExistDiff(fileName, err == nil)
}

func (n *Node) OpenFile(fileName string) (*os.File, error) {
	return os.OpenFile(n.filePath(fileName), os.O_RDWR, 0)
}

func (n *Node) CreateFile(fileName string) (*os.File, error) {
	return os.OpenFile(n.filePath(fileName), os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666)
}

func (n *Node) DeleteFile(fileName string) error {
	return os.Remove(n.filePath(fileName))
}

func (n *Node) RenameFile(fileName, newName string) error {
	return os.Rename(n.filePath(fileName), n.filePath(newName))
}
