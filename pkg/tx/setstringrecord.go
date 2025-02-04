package tx

import (
	"encoding/binary"
	"fmt"

	"github.com/rajivharlalka/parodydb/pkg/fs"
	"github.com/rajivharlalka/parodydb/pkg/logmgr"
)

type SetStringRecord struct {
	txnum  int
	offset int
	val    string
	blk    *fs.BlockId
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

	return &SetStringRecord{txnum, offset, val, blk}
}

func (s *SetStringRecord) Op() int {
	return SETSTRING
}

func (s *SetStringRecord) TxNumber() int {
	return s.txnum
}

// Implement
func (s *SetStringRecord) Undo(tx *Transaction) {
	tx.Pin(s.blk)
	tx.SetString(s.blk, s.offset, s.val, false)
	tx.Unpin(s.blk)
}

func (s *SetStringRecord) ToString() string {
	return fmt.Sprintf("<SETSTRING %d %v %d %s>", s.txnum, s.blk, s.offset, s.val)
}

func WriteStringRecordToLog(lm *logmgr.LogMgr, txnum int, blk *fs.BlockId, offset int, val string) int {
	tpos := binary.Size(0)
	fpos := tpos + binary.Size(0)
	bpos := fpos + fs.MaxLength(len(blk.FileName()))
	opos := bpos + binary.Size(0)
	vpos := opos + fs.MaxLength(len(val))
	rec := make([]byte, vpos+binary.Size(0))
	p := fs.NewPageFromBytes(rec)
	p.SetInt(0, SETSTRING)
	p.SetInt(tpos, txnum)
	p.SetString(fpos, blk.FileName())
	p.SetInt(bpos, blk.Number())
	p.SetInt(opos, offset)
	p.SetString(vpos, val)
	return lm.Append(rec)
}
