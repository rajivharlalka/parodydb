package tx

import (
	"encoding/binary"
	"fmt"

	"github.com/rajivharlalka/parodydb/pkg/fs"
	"github.com/rajivharlalka/parodydb/pkg/logmgr"
)

type StartRecord struct {
	txnum int
}

func NewStartRecord(p *fs.Page) *StartRecord {
	tpos := binary.Size(0)
	txnum := p.GetInt(tpos)

	return &StartRecord{txnum}
}

func (s *StartRecord) Op() int {
	return START
}

func (s *StartRecord) TxNumber() int {
	return s.txnum
}

// Implement
func (s *StartRecord) Undo(tx *Transaction) {}

func (s *StartRecord) ToString() string {
	return fmt.Sprintf("<START %d >", s.txnum)
}

func WriteStartRecordToLog(lm *logmgr.LogMgr, txnum int) int {
	rec := make([]byte, binary.Size(0)*2)
	p := fs.NewPageFromBytes(rec)
	p.SetInt(0, START)
	p.SetInt(binary.Size(0), txnum)
	return lm.Append(rec)
}
