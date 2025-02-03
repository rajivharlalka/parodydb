package buffer

import (
	"github.com/rajivharlalka/parodydb/pkg/fs"
	"github.com/rajivharlalka/parodydb/pkg/logmgr"
)

type Buffer struct {
	fm       *fs.FileMgr
	lm       *logmgr.LogMgr
	Contents *fs.Page
	Block    *fs.BlockId
	pins     int
	txnum    int
	lsn      int
}

func newBuffer(fm *fs.FileMgr, lm *logmgr.LogMgr) *Buffer {
	return &Buffer{fm: fm, lm: lm, Contents: fs.NewPage(int(fm.BlockSize())), Block: nil, pins: 0, txnum: -1, lsn: -1}
}

func (b *Buffer) SetModified(txnum, lsn int) {
	b.txnum = txnum
	if lsn >= 0 {
		b.lsn = lsn
	}
}

func (b *Buffer) isPinned() bool {
	return b.pins > 0
}

func (b *Buffer) mofifyingTx() int {
	return b.txnum
}

func (b *Buffer) assignToBlock(block *fs.BlockId) {
	b.flush()
	b.Block = block
	b.fm.Read(b.Block, b.Contents)
	b.pins = 0
}

func (b *Buffer) flush() {
	if b.txnum >= 0 {
		b.lm.Flush(b.lsn)
		b.fm.Write(b.Block, b.Contents)
		b.txnum = -1
	}
}

func (b *Buffer) pin() {
	b.pins++
}

func (b *Buffer) unpin() {
	b.pins--
}
