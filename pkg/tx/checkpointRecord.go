package tx

import (
	"fmt"

	"github.com/rajivharlalka/parodydb/pkg/fs"
	"github.com/rajivharlalka/parodydb/pkg/logmgr"
)

type CheckpointRecord struct {
	txnum  int
	offset int
	val    string
	blk    fs.BlockId
}

func NewCheckpointRecord(p *fs.Page) *CheckpointRecord {
	tpos := 4
	txnum := p.GetInt(tpos)
	fpos := tpos + 4
	filename := p.GetString(fpos)
	bpos := fpos + fs.MaxLength(len(filename))
	blknum := p.GetInt(bpos)
	blk := fs.NewBlockId(filename, blknum)
	opos := bpos + 4
	offset := p.GetInt(opos)
	vpos := opos + 4
	val := p.GetString(vpos)

	return &CheckpointRecord{txnum, int(offset), val, *blk}
}

func (s *CheckpointRecord) Op() int {
	return CHECKPOINT
}

func (s *CheckpointRecord) TxNumber() int {
	return s.txnum
}

// Implement
func (s *CheckpointRecord) Undo(tx *Transaction) {}

func (s *CheckpointRecord) ToString() string {
	return fmt.Sprintf("<CHECKPOINT %d %v %d %s>", s.txnum, s.blk, s.offset, s.val)
}

// Implement
func WriteCheckpointRecordToLog(lm *logmgr.LogMgr, txnum int) int {
	return 0
}
