package logrecord

import (
	"encoding/binary"
	"fmt"

	"github.com/rajivharlalka/parodydb/pkg/fs"
	"github.com/rajivharlalka/parodydb/pkg/logmgr"
)

type CommitRecord struct {
	txnum  int
	offset int
	val    string
	blk    fs.BlockId
}

func NewCommitRecord(p *fs.Page) *CommitRecord {
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

	return &CommitRecord{txnum, int(offset), val, *blk}
}

func (s *CommitRecord) Op() int {
	return COMMIT
}

func (s *CommitRecord) TxNumber() int {
	return s.txnum
}

// Implement
func (s *CommitRecord) Undo(txnum int) {}

func (s *CommitRecord) ToString() string {
	return fmt.Sprintf("<COMMIT %d %v %d %s>", s.txnum, s.blk, s.offset, s.val)
}

// Implement
func WriteCommitRecordToLog(lm *logmgr.LogMgr, txnum int) int {
	return 0
}
