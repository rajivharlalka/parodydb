package tx

import (
	"fmt"

	"github.com/rajivharlalka/parodydb/pkg/fs"
	"github.com/rajivharlalka/parodydb/pkg/logmgr"
)

type RollbackRecord struct {
	txnum int
}

func NewRollbackRecord(p *fs.Page) *RollbackRecord {
	tpos := 4
	txnum := p.GetInt(tpos)

	return &RollbackRecord{txnum}
}

func (s *RollbackRecord) Op() int {
	return ROLLBACK
}

func (s *RollbackRecord) TxNumber() int {
	return s.txnum
}

// Implement
func (s *RollbackRecord) Undo(tx *Transaction) {}

func (s *RollbackRecord) ToString() string {
	return fmt.Sprintf("<ROLLBACK %d>", s.txnum)
}

// Implement
func WriteRollbackRecordToLog(lm *logmgr.LogMgr, txnum int) int {
	rec := make([]byte, 4*2)
	p := fs.NewPageFromBytes(rec)
	p.SetInt(0, ROLLBACK)
	p.SetInt(4, txnum)
	return lm.Append(rec)
}
