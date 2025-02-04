package tx

import (
	"encoding/binary"
	"fmt"

	"github.com/rajivharlalka/parodydb/pkg/fs"
	"github.com/rajivharlalka/parodydb/pkg/logmgr"
)

type CommitRecord struct {
	txnum int
}

func NewCommitRecord(p *fs.Page) *CommitRecord {
	tpos := binary.Size(0)
	txnum := p.GetInt(tpos)

	return &CommitRecord{txnum}
}

func (s *CommitRecord) Op() int {
	return COMMIT
}

func (s *CommitRecord) TxNumber() int {
	return s.txnum
}

// Implement
func (s *CommitRecord) Undo(tx *Transaction) {}

func (s *CommitRecord) ToString() string {
	return fmt.Sprintf("<COMMIT %d>", s.txnum)
}

// Implement
func WriteCommitRecordToLog(lm *logmgr.LogMgr, txnum int) int {
	rec := make([]byte, binary.Size(0)*2)
	p := fs.NewPageFromBytes(rec)
	p.SetInt(0, COMMIT)
	p.SetInt(binary.Size(0), txnum)
	return lm.Append(rec)
}
