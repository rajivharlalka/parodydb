package logmgr

import (
	"sync"

	"github.com/rajivharlalka/parodydb/pkg/fs"
)

type LogMgr struct {
	fm           *fs.FileMgr
	logfile      string
	logPage      *fs.Page
	currentblk   *fs.BlockId
	latestLSN    int
	lastSavedLSN int
	mu           *sync.Mutex
}

func NewLogMgr(fm *fs.FileMgr, logfile string) *LogMgr {
	b := make([]byte, fm.BlockSize())
	logPage := fs.NewPageFromBytes(b)
	logSize, _ := fm.Length(logfile)
	var currentblk *fs.BlockId
	if logSize == 0 {
		currentblk = appendNewBlock(fm, logfile, logPage)
	} else {
		currentblk = fs.NewBlockId(logfile, logSize-1)
		fm.Read(currentblk, logPage)
	}

	return &LogMgr{
		fm, logfile, logPage, currentblk, 0, 0, &sync.Mutex{},
	}
}

func (l *LogMgr) Flush(lsn int) {
	if lsn >= l.lastSavedLSN {
		l.flush()
	}
}

func (l *LogMgr) flush() {
	l.fm.Write(l.currentblk, l.logPage)
	l.lastSavedLSN = l.latestLSN
}

func appendNewBlock(fm *fs.FileMgr, logfile string, logpage *fs.Page) *fs.BlockId {
	blk := fm.Append(logfile)
	logpage.SetInt(0, int32(fm.BlockSize()))
	fm.Write(blk, logpage)
	return blk
}

func (l *LogMgr) Append(logrec []byte) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	boundary := l.logPage.GetInt(0)
	recordSize := len(logrec)
	bytesNeeded := recordSize + 4
	if boundary-int32(bytesNeeded) < 4 {
		l.flush()
		l.currentblk = appendNewBlock(l.fm, l.logfile, l.logPage)
		boundary = l.logPage.GetInt(0)
	}
	recPos := boundary - int32(bytesNeeded)
	l.logPage.SetBytes(int(recPos), logrec)
	l.logPage.SetInt(0, recPos)
	l.latestLSN += 1
	return l.latestLSN
}

func (l *LogMgr) Iterator() *LogIterator {
	l.flush()
	return NewLogIterator(l.fm, l.currentblk)
}
