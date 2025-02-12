package concurrency

import (
	"errors"
	"sync"
	"time"

	"github.com/rajivharlalka/parodydb/pkg/fs"
)

// ideated from : https://rybicki.io/blog/2024/11/03/multithreaded-code-java-golang.html
const maxWaitTime = 10 * time.Second

type LockTable struct {
	mu      sync.Mutex
	locks   map[*fs.BlockId]int
	waiters map[*fs.BlockId]chan struct{}
}

func NewLockTable() *LockTable {
	return &LockTable{
		locks:   make(map[*fs.BlockId]int),
		waiters: make(map[*fs.BlockId]chan struct{}),
		mu:      sync.Mutex{},
	}
}

func (lt *LockTable) SLock(blk *fs.BlockId) error {
	lt.mu.Lock()
	start := time.Now()

	// While an XLock is still held on this file...
	for lt.locks[blk] == -1 {
		ch := lt.getOrCreateWaitChannel(blk)
		lt.mu.Unlock()

		if time.Since(start) > maxWaitTime {
			return errors.New("lock abort error")
		}

		// Wait on the channel with a timeout
		select {
		case <-ch:
			// Continue when the lock is released
		case <-time.After(maxWaitTime):
			return errors.New("lock abort error")
		}

		lt.mu.Lock()
	}
	val := lt.locks[blk] // will not be negative
	lt.locks[blk] = val + 1
	lt.mu.Unlock()
	return nil
}

func (lt *LockTable) XLock(blk *fs.BlockId) error {
	lt.mu.Lock()
	start := time.Now()

	// While any lock is still held on this file...
	for lt.locks[blk] > 1 {
		ch := lt.getOrCreateWaitChannel(blk)
		lt.mu.Unlock()

		if time.Since(start) > maxWaitTime {
			return errors.New("lock abort error")
		}

		// Wait on the channel with a timeout
		select {
		case <-ch:
			// Continue when the lock is released
		case <-time.After(maxWaitTime):
			return errors.New("lock abort error")
		}

		lt.mu.Lock()
	}
	lt.locks[blk] = -1
	lt.mu.Unlock()
	return nil
}

func (lt *LockTable) Unlock(blk *fs.BlockId) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	val := lt.locks[blk]
	if val > 1 {
		lt.locks[blk] = val - 1
	} else {
		delete(lt.locks, blk)
	}
	// Signal all goroutines waiting for this file (and remove the channel)
	if ch, exists := lt.waiters[blk]; exists {
		close(ch)
		delete(lt.waiters, blk)
	}
}

func (lt *LockTable) getOrCreateWaitChannel(blk *fs.BlockId) chan struct{} {
	if ch, exists := lt.waiters[blk]; exists {
		return ch
	}
	ch := make(chan struct{})
	lt.waiters[blk] = ch
	return ch
}
