package record

import (
	"github.com/rajivharlalka/parodydb/pkg/fs"
	"github.com/rajivharlalka/parodydb/pkg/query"
	"github.com/rajivharlalka/parodydb/pkg/tx"
)

type TableScan struct {
	tx          *tx.Transaction
	layout      *Layout
	rp          *RecordPage
	filename    string
	currentslot int
}

func NewTableScan(tx *tx.Transaction, tblname string, layout *Layout) *TableScan {
	t := &TableScan{tx: tx, layout: layout}
	t.filename = tblname + ".tbl"
	if tx.Size(t.filename) == 0 {
		t.moveToNewBlock()
	} else {
		t.moveToBlock(0)
	}
	return t
}

func (t *TableScan) Close() {
	if t.rp != nil {
		t.tx.Unpin(t.rp.Block())
	}
}

func (t *TableScan) BeforeFirst() {
	t.moveToBlock(0)
}

func (t *TableScan) Next() bool {
	t.currentslot = t.rp.NextAfter(t.currentslot)
	for t.currentslot < 0 {
		if t.AtLastBlock() {
			return false
		}
		t.moveToBlock(t.rp.Block().Number() + 1)
		t.currentslot = t.rp.NextAfter(t.currentslot)
	}
	return true
}

func (t *TableScan) GetInt(fldname string) int {
	return t.rp.GetInt(t.currentslot, fldname)
}

func (t *TableScan) GetString(fldname string) string {
	return t.rp.GetString(t.currentslot, fldname)
}

func (t *TableScan) SetInt(fldname string, val int) {
	t.rp.SetInt(t.currentslot, fldname, val)
}

func (t *TableScan) SetString(fldname string, val string) {
	t.rp.SetString(t.currentslot, fldname, val)
}

func (t *TableScan) GetVal(fldname string) query.Constant {
	if t.layout.schema.Type(fldname) == INTEGER {
		return query.NewConstant(t.GetInt(fldname))
	} else {
		return query.NewConstant(t.GetString(fldname))
	}
}

func (t *TableScan) SetVal(fldname string, val query.Constant) {
	if t.layout.schema.Type(fldname) == INTEGER {
		t.SetInt(fldname, val.AsInt())
	} else {
		t.SetString(fldname, val.AsString())
	}
}

func (t *TableScan) HasField(fldname string) bool {
	return t.layout.schema.HasField(fldname)
}

func (t *TableScan) Insert() {
	t.currentslot = t.rp.InsertAfter(t.currentslot)
	for t.currentslot < 0 {
		if t.AtLastBlock() {
			t.moveToNewBlock()
		} else {
			t.moveToBlock(t.rp.Block().Number() + 1)
		}
		t.currentslot = t.rp.InsertAfter(t.currentslot)
	}
}

func (t *TableScan) Delete() {
	t.rp.Delete(t.currentslot)
}

func (t *TableScan) MoveToRid(rid Rid) {
	t.Close()
	blk := fs.NewBlockId(t.filename, rid.BlockNumber())
	t.rp = NewRecordPage(t.tx, blk, t.layout)
	t.currentslot = rid.slot
}

func (t *TableScan) GetRid() *Rid {
	return NewRID(t.rp.Block().Number(), t.currentslot)
}

func (t *TableScan) moveToBlock(blknum int) {
	t.Close()
	blk := fs.NewBlockId(t.filename, blknum)
	t.rp = NewRecordPage(t.tx, blk, t.layout)
	t.currentslot = -1
}

func (t *TableScan) moveToNewBlock() {
	t.Close()
	blk := t.tx.Append(t.filename)
	t.rp = NewRecordPage(t.tx, blk, t.layout)
	t.rp.Format()
	t.currentslot = -1
}

func (t *TableScan) AtLastBlock() bool {
	return t.rp.Block().Number() == t.tx.Size(t.filename)-1
}
