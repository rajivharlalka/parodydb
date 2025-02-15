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

type TableScanTest struct {
	fm *fs.FileMgr
	bm *buffer.BufferMgr
	lm *logmgr.LogMgr
}

func TestTableScan(t *testing.T) {
	test := &TableScanTest{}
	test.fm, _ = fs.NewFileManager("tablescan_test", 400)
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

	fmt.Println("Filling the table with 50 random records")
	ts := record.NewTableScan(tx, "T", layout)
	for i := 0; i < 50; i++ {
		ts.Insert()
		n := int(math.Floor(rand.Float64() * 50))
		ts.SetInt("A", n)
		ts.SetString("B", fmt.Sprintf("rec%d", n))
		fmt.Printf("inserting into slot %s: {%d, slot%d}\n", ts.GetRid().ToString(), n, n)
	}
	fmt.Println("Deleted these records with A-values <25")
	count := 0
	ts.BeforeFirst()
	for ts.Next() {
		a := ts.GetInt("A")
		b := ts.GetString("B")
		if a < 25 {
			count++
			fmt.Printf("slot %s: {%d, %s}\n", ts.GetRid().ToString(), a, b)
			ts.Delete()
		}
	}
	fmt.Printf("%d values under 25 were deleted\n", count)
	fmt.Println("Remaining values:")
	ts.BeforeFirst()
	for ts.Next() {

		a := ts.GetInt("A")
		b := ts.GetString("B")
		fmt.Printf("slot %s: {%d, %s}\n", ts.GetRid().ToString(), a, b)
	}
	ts.Close()
	tx.Commit()
}
