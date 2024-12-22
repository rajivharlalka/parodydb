package buffer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rajivharlalka/parodydb/pkg/fs"
	"github.com/rajivharlalka/parodydb/pkg/logmgr"
)

type BufferMgr struct {
	bufferPool   []*Buffer
	numAvailable int
	mu           *sync.Mutex
	notif        *sync.Cond
}

const MAX_TIME int = 10000

func NewBufferMgr(fm *fs.FileMgr, lm *logmgr.LogMgr, numBufs int) *BufferMgr {
	bm := new(BufferMgr)
	bm.bufferPool = make([]*Buffer, numBufs)
	bm.numAvailable = numBufs
	for i := 0; i < numBufs; i++ {
		bm.bufferPool[i] = newBuffer(fm, lm)
	}
	bm.mu = &sync.Mutex{}
	bm.notif = sync.NewCond(bm.mu)
	return bm
}

func (bm *BufferMgr) available() int {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	return bm.numAvailable
}

func (bm *BufferMgr) flushAll(txNum int) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	for _, buf := range bm.bufferPool {
		if buf.mofifyingTx() == txNum {
			buf.flush()
		}
	}
}

func (bm *BufferMgr) unpin(buff *Buffer) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	buff.unpin()
	if !buff.isPinned() {
		bm.numAvailable++
		bm.notif.Broadcast()
	}
}

func (bm *BufferMgr) pin(blk *fs.BlockId) (*Buffer, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(MAX_TIME))
	defer cancel()

	stop := context.AfterFunc(ctx, func() {
		bm.notif.L.Lock()
		bm.notif.Broadcast()
		bm.notif.L.Unlock()
	})

	defer stop()

	for {
		if buff, err := bm.tryToPin(blk); err != nil {
			return nil, err
		} else if buff != nil {
			return buff, nil
		}

		bm.notif.Wait()

		if ctx.Err() != nil {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return nil, fmt.Errorf("buffer abort exception: could not pin block %s: %s", blk.ToString(), ctx.Err().Error())
			}
			return nil, ctx.Err()
		}
	}
}

func waitingTooLong(t time.Time) bool {
	return time.Now().Unix()-t.Unix() > int64(MAX_TIME)
}

func (bm *BufferMgr) tryToPin(blk *fs.BlockId) (*Buffer, error) {
	buff := bm.findExistingBuffer(blk)
	if buff == nil {
		buff = bm.chooseUnpinnedBuffer()
		if buff == nil {
			return nil, nil
		}
		buff.assignToBlock(blk)
	}
	if !buff.isPinned() {
		bm.numAvailable--
	}
	buff.pin()
	return buff, nil
}

func (bm *BufferMgr) findExistingBuffer(blk *fs.BlockId) *Buffer {
	for _, buff := range bm.bufferPool {
		b := buff.Block
		if b != nil && b.Equals(blk) {
			return buff
		}
	}
	return nil
}

func (bm *BufferMgr) chooseUnpinnedBuffer() *Buffer {
	for _, buff := range bm.bufferPool {
		if !buff.isPinned() {
			return buff
		}
	}
	return nil
}
