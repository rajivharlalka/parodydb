package logrecord

import (
	"encoding/binary"
	"fmt"

	"github.com/rajivharlalka/parodydb/pkg/fs"
	"github.com/rajivharlalka/parodydb/pkg/logmgr"
)

type StartRecord struct {
	txnum  int
	offset int
	val    string
	blk    fs.BlockId
}

func NewStartRecord(p *fs.Page) *StartRecord {
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

	return &StartRecord{txnum, int(offset), val, *blk}
}

func (s *StartRecord) Op() int {
	return START
}

func (s *StartRecord) TxNumber() int {
	return s.txnum
}

// Implement
func (s *StartRecord) Undo(txnum int) {}

func (s *StartRecord) ToString() string {
	return fmt.Sprintf("<START %d %v %d %s>", s.txnum, s.blk, s.offset, s.val)
}

func WriteStartRecordToLog(lm *logmgr.LogMgr, txnum int) {
	// Implement
}
