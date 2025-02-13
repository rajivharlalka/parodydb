package tx_test

import (
	"fmt"
	"testing"

	"github.com/rajivharlalka/parodydb/pkg/buffer"
	"github.com/rajivharlalka/parodydb/pkg/fs"
	"github.com/rajivharlalka/parodydb/pkg/logmgr"
	"github.com/rajivharlalka/parodydb/pkg/tx"
)

type TransactionTest struct {
	fm *fs.FileMgr
	bm *buffer.BufferMgr
	lm *logmgr.LogMgr
}

func TestTransactions(t *testing.T) {
	test := &TransactionTest{}
	test.fm, _ = fs.NewFileManager("transaction_test", 400)
	test.lm = logmgr.NewLogMgr(test.fm, "log")
	test.bm = buffer.NewBufferMgr(test.fm, test.lm, 8)

	tx1 := tx.NewTransaction(test.fm, test.lm, test.bm)
	blk := fs.NewBlockId("testfile", 1)
	tx1.Pin(blk)
	tx1.SetInt(blk, 80, 1, false)
	tx1.SetString(blk, 40, "one", false)
	tx1.Commit()

	tx2 := tx.NewTransaction(test.fm, test.lm, test.bm)
	tx2.Pin(blk)
	ival := tx2.GetInt(blk, 80)
	sval := tx2.GetString(blk, 40)
	fmt.Printf("initial value at 80:= %d\n", ival)
	fmt.Printf("initial value at 40:= %s\n", sval)
	newIval := ival + 1
	newSval := sval + "!"

	tx2.SetInt(blk, 80, newIval, true)
	tx2.SetString(blk, 40, newSval, true)
	tx2.Commit()

	tx3 := tx.NewTransaction(test.fm, test.lm, test.bm)
	tx3.Pin(blk)
	ival = tx3.GetInt(blk, 80)
	sval = tx3.GetString(blk, 40)
	fmt.Printf("new value at 80:= %d\n", ival)
	fmt.Printf("new value at 40:= %s\n", sval)
	tx3.SetInt(blk, 80, 9999, true)

	fmt.Printf("pre-rollback value at 80:= %d\n", tx3.GetInt(blk, 80))
	tx3.Rollback()

	tx4 := tx.NewTransaction(test.fm, test.lm, test.bm)
	tx4.Pin(blk)
	fmt.Printf("post-rollback value at 80:= %d\n", tx4.GetInt(blk, 80))
	tx4.Commit()
}
