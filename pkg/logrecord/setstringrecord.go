package logrecord

import (
	"encoding/binary"
	"fmt"

	"github.com/rajivharlalka/parodydb/pkg/fs"
)

type SetStringRecord struct {
	txnum  int
	offset int
	val    string
	blk    fs.BlockId
}

func NewSetStringRecord(p *fs.Page) *SetStringRecord {
	tpos := binary.Size(int32(0))
	txnum := p.GetInt(tpos)
	fpos := tpos + binary.Size(int32(0))
	filename := p.GetString(fpos)
	bpos := fpos + fs.MaxLength(len(filename))
	blknum := p.GetInt(bpos)
	blk := fs.NewBlockId(filename, blknum)
	opos := bpos + binary.Size(int32(0))
	offset := p.GetInt(opos)
	vpos := opos + binary.Size(int32(0))
	val := p.GetString(vpos)

	return &SetStringRecord{txnum, offset, val, *blk}
}

func (s *SetStringRecord) Op() int {
	return SETSTRING
}

func (s *SetStringRecord) TxNumber() int {
	return s.txnum
}

// Implement
func (s *SetStringRecord) Undo(txnum int) {}

func (s *SetStringRecord) ToString() string {
	return fmt.Sprintf("<SETSTRING %d %v %d %s>", s.txnum, s.blk, s.offset, s.val)
}

func WriteStringRecordToLog(args ...interface{}) int {
	return 0
}
