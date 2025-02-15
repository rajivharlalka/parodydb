package record_test

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/rajivharlalka/parodydb/pkg/buffer"
	"github.com/rajivharlalka/parodydb/pkg/fs"
	"github.com/rajivharlalka/parodydb/pkg/logmgr"
	"github.com/rajivharlalka/parodydb/pkg/record"
	"github.com/rajivharlalka/parodydb/pkg/tx"
)

type RecordTest struct {
	fm *fs.FileMgr
	bm *buffer.BufferMgr
	lm *logmgr.LogMgr
}

func TestRecord(t *testing.T) {
	test := &RecordTest{}
	test.fm, _ = fs.NewFileManager("record_test", 400)
	test.lm = logmgr.NewLogMgr(test.fm, "log")
	test.bm = buffer.NewBufferMgr(test.fm, test.lm, 8)

	tx := tx.NewTransaction(test.fm, test.lm, test.bm)
	sch := record.NewSchema()
	sch.AddIntField("A")
	sch.AddStringField("B", 9)
	layout := record.NewLayout(sch)
	for _, fldname := range layout.Schema().Fields() {
		offset := layout.Offset(fldname)
		fmt.Printf("%s has offset %d\n", fldname, offset)
	}
	blk := tx.Append("testfile")
	tx.Pin(blk)
	rp := record.NewRecordPage(tx, blk, layout)
	rp.Format()
	fmt.Println("Filling page with random records")
	slot := rp.InsertAfter(-1)
	for slot >= 0 {
		n := int(math.Floor(rand.Float64() * 50))
		rp.SetInt(slot, "A", n)
		rp.SetString(slot, "B", fmt.Sprintf("rec%d", n))
		fmt.Printf("inserting into slot %d: {%d, slot%d}\n", slot, n, n)
		slot = rp.InsertAfter(slot)
	}
	fmt.Println("Deleted these records with A-values <25")
	count := 0
	slot = rp.NextAfter(-1)
	for slot >= 0 {
		a := rp.GetInt(slot, "A")
		b := rp.GetString(slot, "B")
		if a < 25 {
			count++
			fmt.Printf("slot %d: {%d, %s}\n", slot, a, b)
			rp.Delete(slot)
		}
		slot = rp.NextAfter(slot)
	}
	fmt.Printf("%d values under 25 were deleted\n", count)
	fmt.Println("Remaining values:")
	slot = rp.NextAfter(-1)
	for slot >= 0 {

		a := rp.GetInt(slot, "A")
		b := rp.GetString(slot, "B")
		fmt.Printf("slot %d: {%d, %s}\n", slot, a, b)
		slot = rp.NextAfter(slot)
	}
	tx.Unpin(blk)
	tx.Commit()
}
