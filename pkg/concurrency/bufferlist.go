package concurrency

import (
	"slices"
	"sync"

	"github.com/rajivharlalka/parodydb/pkg/buffer"
	"github.com/rajivharlalka/parodydb/pkg/fs"
)

type BufferList struct {
	buffers map[*fs.BlockId]*buffer.Buffer
	pins    []*fs.BlockId
	mu      sync.Mutex
	bm      *buffer.BufferMgr
}

func NewBufferList(bm *buffer.BufferMgr) *BufferList {
	return &BufferList{bm: bm}
}

func (bl *BufferList) getBuffer(blk *fs.BlockId) *buffer.Buffer {
	return bl.buffers[blk]
}

func (bl *BufferList) pin(blk *fs.BlockId) {
	buff, err := bl.bm.Pin(blk)
	if err != nil {
		panic(err.Error())
	}
	bl.mu.Lock()
	defer bl.mu.Unlock()
	bl.buffers[blk] = buff
	bl.pins = append(bl.pins, blk)
}

func (bl *BufferList) unpin(blk *fs.BlockId) {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	buff, has := bl.buffers[blk]
	if !has {
		return
	}
	bl.bm.Unpin(buff)
	for i := range bl.pins {
		if bl.pins[i] == blk {
			bl.pins = append(bl.pins[:i], bl.pins[i+1:]...)
			break
		}
	}
	if !slices.Contains(bl.pins, blk) {
		delete(bl.buffers, blk)
	}
}

func (bl *BufferList) unpinAll() {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	for _, blk := range bl.pins {
		buff := bl.buffers[blk]
		bl.bm.Unpin(buff)
	}
	for bi := range bl.buffers {
		delete(bl.buffers, bi)
	}
	bl.pins = []*fs.BlockId{}
}
