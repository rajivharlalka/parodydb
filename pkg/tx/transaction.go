package tx

import (
	"fmt"
	"sync/atomic"

	"github.com/rajivharlalka/parodydb/pkg/buffer"
	"github.com/rajivharlalka/parodydb/pkg/fs"
	"github.com/rajivharlalka/parodydb/pkg/logmgr"
	"github.com/rajivharlalka/parodydb/pkg/tx/concurrency"
)

var nextTxNum atomic.Int32

const END_OF_FILE int = -1

type Transaction struct {
	rm      *RecoveryMgr
	cm      *concurrency.ConcurrencyMgr
	fm      *fs.FileMgr
	buffers *BufferList
	bm      *buffer.BufferMgr
	txnum   int
}

func NewTransaction(fm *fs.FileMgr, lm *logmgr.LogMgr, bm *buffer.BufferMgr) *Transaction {
	cm := concurrency.NewConcurrencyMgr()
	bl := NewBufferList(bm)
	txnum := getNextTxNum()
	tx := &Transaction{cm: cm, fm: fm, bm: bm, buffers: bl, txnum: txnum}
	tx.rm = NewRecoveryMgr(tx, txnum, lm, bm)
	return tx
}

func (tx *Transaction) Commit() {
	tx.rm.Commit()
	tx.cm.Release()
	tx.buffers.unpinAll()
	fmt.Printf("Transaction %d committed\n", tx.txnum)
}

func (tx *Transaction) Rollback() {
	tx.rm.Rollback()
	tx.cm.Release()
	tx.buffers.unpinAll()
	fmt.Printf("Transaction %d rolled back\n", tx.txnum)
}

func (tx *Transaction) Recover() {
	tx.bm.FlushAll(tx.txnum)
	tx.rm.Recover()
}

func (tx *Transaction) Pin(blk *fs.BlockId) {
	tx.buffers.pin(blk)
}

func (tx *Transaction) Unpin(blk *fs.BlockId) {
	tx.buffers.unpin(blk)
}

func (tx *Transaction) GetInt(blk *fs.BlockId, offset int) int {
	tx.cm.SLock(blk)
	buff := tx.buffers.getBuffer(blk)
	return buff.Contents.GetInt(offset)
}

func (tx *Transaction) GetString(blk *fs.BlockId, offset int) string {
	tx.cm.SLock(blk)
	buff := tx.buffers.getBuffer(blk)
	return buff.Contents.GetString(offset)
}

func (tx *Transaction) SetInt(blk *fs.BlockId, offset int, value int, okToLog bool) {
	tx.cm.XLock(blk)
	buff := tx.buffers.getBuffer(blk)
	lsn := -1
	if okToLog {
		lsn = tx.rm.SetInt(buff, offset, value)
	}
	p := buff.Contents
	p.SetInt(offset, value)
	buff.SetModified(tx.txnum, lsn)
}

func (tx *Transaction) SetString(blk *fs.BlockId, offset int, value string, okToLog bool) {
	tx.cm.XLock(blk)
	buff := tx.buffers.getBuffer(blk)
	lsn := -1
	if okToLog {
		lsn = tx.rm.SetString(buff, offset, value)
	}
	p := buff.Contents
	p.SetString(offset, value)
	buff.SetModified(tx.txnum, lsn)
}

func (tx *Transaction) Size(filename string) int {
	dummy := fs.NewBlockId(filename, END_OF_FILE)
	tx.cm.SLock(dummy)
	size, _ := tx.fm.Length(filename)
	return size
}

func (tx *Transaction) Append(filename string) *fs.BlockId {
	dummy := fs.NewBlockId(filename, END_OF_FILE)
	tx.cm.XLock(dummy)
	return tx.fm.Append(filename)
}

func (tx *Transaction) BlockSize() int {
	return tx.fm.BlockSize()
}

func (tx *Transaction) AvailableBuffs() int {
	return tx.bm.Available()
}

func getNextTxNum() int {
	txnum := nextTxNum.Add(1)
	fmt.Printf("new transaction: %d\n", txnum)
	return int(txnum)
}
