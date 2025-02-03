package concurrency

import (
	"github.com/rajivharlalka/parodydb/pkg/buffer"
	"github.com/rajivharlalka/parodydb/pkg/fs"
	"github.com/rajivharlalka/parodydb/pkg/logmgr"
)

var nextTxNum int = 0

const END_OF_FILE int = -1

type Transaction struct {
	rm    *RecoveryMgr
	cm    *ConcurrencyMgr
	fm    *fs.FileMgr
	bl    *BufferList
	bm    *buffer.BufferMgr
	txnum int
}

func NewTransaction(fm *fs.FileMgr, lm *logmgr.LogMgr, bm *buffer.BufferMgr) *Transaction {
	cm := NewConcurrencyMgr()
	bl := NewBufferList(bm)
	tx := &Transaction{cm: cm, fm: fm, bm: bm, bl: bl, txnum: 0}
	tx.rm = newRecoveryMgr(tx, 0, lm, bm)
	return tx
}
