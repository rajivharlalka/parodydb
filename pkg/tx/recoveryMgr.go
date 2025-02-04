package tx

import (
	"slices"

	"github.com/rajivharlalka/parodydb/pkg/buffer"
	"github.com/rajivharlalka/parodydb/pkg/logmgr"
)

type RecoveryMgr struct {
	lm    *logmgr.LogMgr
	bm    *buffer.BufferMgr
	tx    *Transaction
	txnum int
}

func NewRecoveryMgr(tx *Transaction, txnum int, lm *logmgr.LogMgr, bm *buffer.BufferMgr) *RecoveryMgr {
	WriteStartRecordToLog(lm, txnum)
	return &RecoveryMgr{lm, bm, tx, txnum}
}

func (rm *RecoveryMgr) Commit() {
	rm.bm.FlushAll(rm.txnum)
	lsn := WriteCommitRecordToLog(rm.lm, rm.txnum)
	rm.lm.Flush(lsn)
}

func (rm *RecoveryMgr) Rollback() {
	rm.doRollback()
	rm.bm.FlushAll(rm.txnum)
	lsn := WriteRollbackRecordToLog(rm.lm, rm.txnum)
	rm.lm.Flush(lsn)
}

func (rm *RecoveryMgr) Recover() {
	rm.doRecover()
	rm.bm.FlushAll(rm.txnum)
	lsn := WriteCheckpointRecordToLog(rm.lm, rm.txnum)
	rm.lm.Flush(lsn)
}

func (rm *RecoveryMgr) SetInt(buff *buffer.Buffer, offset int, newVal int) int {
	oldVal := buff.Contents.GetInt(offset)
	blk := buff.Block
	return WriteIntRecordToLog(rm.lm, rm.txnum, blk, offset, oldVal)
}

func (rm *RecoveryMgr) SetString(buff *buffer.Buffer, offset int, newVal string) int {
	oldVal := buff.Contents.GetString(offset)
	blk := buff.Block
	return WriteStringRecordToLog(rm.lm, rm.txnum, blk, offset, oldVal)
}

func (rm *RecoveryMgr) doRollback() {
	iter := rm.lm.Iterator()
	for iter.HasNext() {
		bytes := iter.Next()
		rec := CreateLogRecord(bytes)
		if rec.TxNumber() == rm.txnum {
			if rec.Op() == START {
				return
			}
			rec.Undo(rm.tx)
		}
	}
}

func (rm *RecoveryMgr) doRecover() {
	finishedTx := make([]int, 0)
	iter := rm.lm.Iterator()
	for iter.HasNext() {
		bytes := iter.Next()
		rec := CreateLogRecord(bytes)
		if rec.Op() == CHECKPOINT {
			return
		}

		if rec.Op() == COMMIT || rec.Op() == ROLLBACK {
			finishedTx = append(finishedTx, rec.TxNumber())
		} else if !slices.Contains(finishedTx, rm.txnum) {
			rec.Undo(rm.tx)
		}
	}
}
