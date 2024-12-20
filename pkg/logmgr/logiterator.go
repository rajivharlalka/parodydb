package logmgr

import "github.com/rajivharlalka/parodydb/pkg/fs"

type LogIterator struct {
	fm         *fs.FileMgr
	blk        *fs.BlockId
	page       *fs.Page
	currentPos int
	boundary   int
}

func NewLogIterator(fm *fs.FileMgr, blk *fs.BlockId) *LogIterator {
	logIterator := new(LogIterator)
	logIterator.fm = fm
	logIterator.blk = blk
	b := make([]byte, fm.BlockSize())
	logIterator.page = fs.NewPageFromBytes(b)
	logIterator.moveToBlock(blk)
	return logIterator
}

func (l *LogIterator) moveToBlock(blk *fs.BlockId) {
	l.fm.Read(blk, l.page)
	l.boundary = int(l.page.GetInt(0))
	l.currentPos = l.boundary
}

func (l *LogIterator) HasNext() bool {
	return l.currentPos < int(l.fm.BlockSize()) || l.blk.Number() > 0
}

func (l *LogIterator) Next() []byte {
	if l.currentPos == int(l.fm.BlockSize()) {
		l.blk = fs.NewBlockId(l.blk.FileName(), l.blk.Number()-1)
		l.moveToBlock(l.blk)
	}
	rec := l.page.GetBytes(l.currentPos)
	l.currentPos += 4 + len(rec)
	return rec
}
