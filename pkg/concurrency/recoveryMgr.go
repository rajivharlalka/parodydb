package concurrency

import (
	"slices"

	"github.com/rajivharlalka/parodydb/pkg/buffer"
	"github.com/rajivharlalka/parodydb/pkg/logmgr"
	"github.com/rajivharlalka/parodydb/pkg/logrecord"
)

type RecoveryMgr struct {
	lm    *logmgr.LogMgr
	bm    *buffer.BufferMgr
	tx    *Transaction
	txnum int
}

func newRecoveryMgr(tx *Transaction, txnum int, lm *logmgr.LogMgr, bm *buffer.BufferMgr) *RecoveryMgr {
	logrecord.WriteStartRecordToLog(lm, txnum)
	return &RecoveryMgr{lm, bm, tx, txnum}
}

func (rm *RecoveryMgr) Commit() {
	rm.bm.FlushAll(rm.txnum)
	lsn := logrecord.WriteCommitRecordToLog(rm.lm, rm.txnum)
	rm.lm.Flush(lsn)
}

func (rm *RecoveryMgr) Rollback() {
	rm.doRollback()
	rm.bm.FlushAll(rm.txnum)
	lsn := logrecord.WriteRollbackRecordToLog(rm.lm, rm.txnum)
	rm.lm.Flush(lsn)
}

func (rm *RecoveryMgr) Recover() {
	rm.doRecover()
	rm.bm.FlushAll(rm.txnum)
	lsn := logrecord.WriteCheckpointRecordToLog(rm.lm, rm.txnum)
	rm.lm.Flush(lsn)
}

func (rm *RecoveryMgr) SetInt(buff *buffer.Buffer, offset int, newVal int) int {
	oldVal := buff.Contents.GetInt(offset)
	blk := buff.Block
	return logrecord.WriteIntRecordToLog(rm.lm, rm.lm, blk, offset, oldVal)
}

func (rm *RecoveryMgr) SetString(buff *buffer.Buffer, offset int, newVal string) int {
	oldVal := buff.Contents.GetString(offset)
	blk := buff.Block
	return logrecord.WriteStringRecordToLog(rm.lm, rm.lm, blk, offset, oldVal)
}

func (rm *RecoveryMgr) doRollback() {
	iter := rm.lm.Iterator()
	for iter.HasNext() {
		bytes := iter.Next()
		rec := logrecord.CreateLogRecord(bytes)
		if rec.TxNumber() == rm.txnum {
			if rec.Op() == logrecord.START {
				return
			}
			rec.Undo(rm.txnum)
		}
	}
}

func (rm *RecoveryMgr) doRecover() {
	finishedTx := make([]int, 0)
	iter := rm.lm.Iterator()
	for iter.HasNext() {
		bytes := iter.Next()
		rec := logrecord.CreateLogRecord(bytes)
		if rec.Op() == logrecord.CHECKPOINT {
			return
		}

		if rec.Op() == logrecord.COMMIT || rec.Op() == logrecord.ROLLBACK {
			finishedTx = append(finishedTx, rec.TxNumber())
		} else if !slices.Contains(finishedTx, rm.txnum) {
			rec.Undo(rm.txnum)
		}
	}
}
