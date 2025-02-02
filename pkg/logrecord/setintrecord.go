package logrecord

import (
	"encoding/binary"
	"fmt"

	"github.com/rajivharlalka/parodydb/pkg/fs"
)

type SetIntRecord struct {
	txnum  int
	offset int
	val    string
	blk    *fs.BlockId
}

func NewSetIntRecord(p *fs.Page) *SetIntRecord {
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

	return &SetIntRecord{txnum, int(offset), val, blk}
}

func (s *SetIntRecord) Op() int {
	return SETINT
}

func (s *SetIntRecord) TxNumber() int {
	return s.txnum
}

// Implement
func (s *SetIntRecord) Undo(txnum int) {}

func (s *SetIntRecord) ToString() string {
	return fmt.Sprintf("<SETINT %d %v %d %s>", s.txnum, s.blk, s.offset, s.val)
}

func WriteIntRecordToLog(args ...interface{}) int {
	return 0
}
