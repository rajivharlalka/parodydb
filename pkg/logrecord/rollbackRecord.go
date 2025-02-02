package logrecord

import (
	"encoding/binary"
	"fmt"

	"github.com/rajivharlalka/parodydb/pkg/fs"
	"github.com/rajivharlalka/parodydb/pkg/logmgr"
)

type RollbackRecord struct {
	txnum  int
	offset int
	val    string
	blk    *fs.BlockId
}

func NewRollbackRecord(p *fs.Page) *RollbackRecord {
	tpos := binary.Size(0)
	txnum := p.GetInt(tpos)
	fpos := tpos + binary.Size(0)
	filename := p.GetString(fpos)
	bpos := fpos + fs.MaxLength(len(filename))
	blknum := p.GetInt(bpos)
	blk := fs.NewBlockId(filename, blknum)
	opos := bpos + binary.Size(0)
	offset := p.GetInt(opos)
	vpos := opos + binary.Size(0)
	val := p.GetString(vpos)

	return &RollbackRecord{txnum, int(offset), val, blk}
}

func (s *RollbackRecord) Op() int {
	return ROLLBACK
}

func (s *RollbackRecord) TxNumber() int {
	return s.txnum
}

// Implement
func (s *RollbackRecord) Undo(txnum int) {}

func (s *RollbackRecord) ToString() string {
	return fmt.Sprintf("<ROLLBACK %d %v %d %s>", s.txnum, s.blk, s.offset, s.val)
}

// Implement
func WriteRollbackRecordToLog(lm *logmgr.LogMgr, txnum int) int {
	return 0
}
