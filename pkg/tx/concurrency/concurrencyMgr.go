package concurrency

import "github.com/rajivharlalka/parodydb/pkg/fs"

type ConcurrencyMgr struct {
	locktbl *LockTable
	locks   map[*fs.BlockId]string
}

func NewConcurrencyMgr() *ConcurrencyMgr {
	ltbl := NewLockTable()
	return &ConcurrencyMgr{locktbl: ltbl, locks: make(map[*fs.BlockId]string)}
}

func (c *ConcurrencyMgr) SLock(blk *fs.BlockId) {
	if _, has := c.locks[blk]; !has {
		c.locktbl.SLock(blk)
		c.locks[blk] = "S"
	}
}

func (c *ConcurrencyMgr) XLock(blk *fs.BlockId) {
	if !c.hasXLock(blk) {
		c.SLock(blk)
		c.locktbl.XLock(blk)
		c.locks[blk] = "X"
	}
}

func (c *ConcurrencyMgr) Release() {
	for blk := range c.locks {
		c.locktbl.Unlock(blk)
	}
	for bi := range c.locks {
		delete(c.locks, bi)
	}
}

func (c *ConcurrencyMgr) hasXLock(blk *fs.BlockId) bool {
	lock, has := c.locks[blk]
	return has && lock == "X"
}
