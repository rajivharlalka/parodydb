package tx

import (
	"encoding/binary"
	"fmt"

	"github.com/rajivharlalka/parodydb/pkg/fs"
	"github.com/rajivharlalka/parodydb/pkg/logmgr"
)

type SetIntRecord struct {
	txnum  int
	offset int
	val    int
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
	val := p.GetInt(vpos)

	return &SetIntRecord{txnum, offset, val, blk}
}

func (s *SetIntRecord) Op() int {
	return SETINT
}

func (s *SetIntRecord) TxNumber() int {
	return s.txnum
}

// Implement
func (s *SetIntRecord) Undo(tx *Transaction) {
	tx.Pin(s.blk)
	tx.SetInt(s.blk, s.offset, s.val, false)
	tx.Unpin(s.blk)
}

func (s *SetIntRecord) ToString() string {
	return fmt.Sprintf("<SETINT %d %v %d %ds>", s.txnum, s.blk, s.offset, s.val)
}

func WriteIntRecordToLog(lm *logmgr.LogMgr, txnum int, blk *fs.BlockId, offset int, val int) int {
	tpos := binary.Size(0)
	fpos := tpos + binary.Size(0)
	bpos := fpos + fs.MaxLength(len(blk.FileName()))
	opos := bpos + binary.Size(0)
	vpos := opos + binary.Size(0)
	rec := make([]byte, vpos+binary.Size(0))
	p := fs.NewPageFromBytes(rec)
	p.SetInt(0, SETINT)
	p.SetInt(tpos, txnum)
	p.SetString(fpos, blk.FileName())
	p.SetInt(bpos, blk.Number())
	p.SetInt(opos, offset)
	p.SetInt(vpos, val)
	return lm.Append(rec)
}
