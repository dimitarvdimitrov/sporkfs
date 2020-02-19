package cache

import (
	"sync"
	"time"

	"github.com/dimitarvdimitrov/sporkfs/store/data"
)

const expiry = time.Minute * 5

type Cache interface {
	data.Driver

	// KeepAlive will reset the expiry time of the file. You don't have to
	// call it manually, it will be called before all read/write methods of the cache except Remove.
	KeepAlive(id, hash uint64)
}

type cache struct {
	*sync.Mutex

	data  data.Driver
	alive map[uint64]map[uint64]*time.Timer
}

func New(data data.Driver) *cache {
	return &cache{
		data:  data,
		Mutex: &sync.Mutex{},
		alive: make(map[uint64]map[uint64]*time.Timer),
	}
}

func (c *cache) Reader(id, version uint64, flags int) (data.Reader, error) {
	c.KeepAlive(id, version)
	return c.data.Reader(id, version, flags)
}

func (c *cache) Writer(id, version uint64, flags int) (data.Writer, error) {
	c.KeepAlive(id, version)
	return c.data.Writer(id, version, flags)
}

func (c *cache) Open(id, version uint64, flags int) (data.Reader, data.Writer, error) {
	c.KeepAlive(id, version)
	return c.data.Open(id, version, flags)
}

func (c *cache) Contains(id, version uint64) bool {
	c.KeepAlive(id, version)
	return c.data.Contains(id, version)
}

func (c *cache) ContainsAny(id uint64) bool {
	c.keepAliveAll(id)
	return c.data.ContainsAny(id)
}

func (c *cache) Remove(id, version uint64) {
	c.data.Remove(id, version)
}

func (c *cache) Size(id, version uint64) int64 {
	c.KeepAlive(id, version)
	return c.data.Size(id, version)
}

func (c *cache) Sync() {
	c.data.Sync()
}

func (c *cache) KeepAlive(id, hash uint64) {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.alive[id]; !ok {
		c.alive[id] = make(map[uint64]*time.Timer)
	}

	if t, ok := c.alive[id][hash]; ok {
		t.Stop()
	}
	c.alive[id][hash] = time.AfterFunc(expiry, c.cleanFunc(id, hash))
}

func (c *cache) cleanFunc(id, hash uint64) func() {
	return func() {
		c.Lock()
		defer c.Unlock()

		if c.alive[id][hash].Stop() {
			c.alive[id][hash] = time.AfterFunc(expiry, c.cleanFunc(id, hash))
			return
		}

		c.Remove(id, hash)
		delete(c.alive[id], hash)
		if len(c.alive[id]) == 0 {
			delete(c.alive, id)
		}
	}
}

func (c *cache) keepAliveAll(id uint64) {
	c.Lock()
	toKeepAlive := make([]uint64, 0, len(c.alive[id]))
	for hash := range c.alive[id] {
		toKeepAlive = append(toKeepAlive, hash)
	}
	c.Unlock()

	for _, hash := range toKeepAlive {
		c.KeepAlive(id, hash)
	}
}
