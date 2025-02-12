package tx_test

import (
	"fmt"
	"testing"

	"github.com/rajivharlalka/parodydb/pkg/buffer"
	"github.com/rajivharlalka/parodydb/pkg/fs"
	"github.com/rajivharlalka/parodydb/pkg/logmgr"
	"github.com/rajivharlalka/parodydb/pkg/tx"
)

type RecoveryTest struct {
	fm   *fs.FileMgr
	bm   *buffer.BufferMgr
	lm   *logmgr.LogMgr
	blk0 *fs.BlockId
	blk1 *fs.BlockId
}

func TestRecoveryManager(t *testing.T) {
	test := &RecoveryTest{}
	test.fm, _ = fs.NewFileManager("recovery_test", 400)
	test.lm = logmgr.NewLogMgr(test.fm, "log")
	test.bm = buffer.NewBufferMgr(test.fm, test.lm, 8)
	test.blk0 = fs.NewBlockId("testfile", 0)
	test.blk1 = fs.NewBlockId("testfile", 1)
	if len, _ := test.fm.Length("testfile"); len == 0 {
		test.initialize()
		test.modify()
	} else {
		test.recover()
	}
}

func (t *RecoveryTest) initialize() {
	tx1 := tx.NewTransaction(t.fm, t.lm, t.bm)
	tx2 := tx.NewTransaction(t.fm, t.lm, t.bm)
	tx1.Pin(t.blk0)
	tx2.Pin(t.blk1)

	pos := 0
	for i := 0; i < 6; i++ {
		tx1.SetInt(t.blk0, pos, pos, false)
		tx2.SetInt(t.blk1, pos, pos, false)
		pos += 4
	}
	tx1.SetString(t.blk0, 30, "abc", false)
	tx2.SetString(t.blk1, 30, "def", false)
	tx1.Commit()
	tx2.Commit()
	t.printValues("After Initialization:")
}

func (t *RecoveryTest) modify() {
	tx3 := tx.NewTransaction(t.fm, t.lm, t.bm)
	tx4 := tx.NewTransaction(t.fm, t.lm, t.bm)
	tx3.Pin(t.blk0)
	tx4.Pin(t.blk1)

	pos := 0
	for i := 0; i < 6; i++ {
		tx3.SetInt(t.blk0, pos, pos+100, true)
		tx4.SetInt(t.blk1, pos, pos+100, true)
		pos += 4
	}
	tx3.SetString(t.blk0, 30, "uvw", true)
	tx4.SetString(t.blk1, 30, "xyz", true)

	t.bm.FlushAll(3)
	t.bm.FlushAll(4)
	t.printValues("After modification:")

	tx3.Rollback()
	t.printValues("After rollback:")
	// tx4 stops here without committing or rolling back, it will be undone during recovery
}

func (t *RecoveryTest) recover() {
	tx := tx.NewTransaction(t.fm, t.lm, t.bm)
	tx.Recover()
	t.printValues("After recovery:")
}

func (t *RecoveryTest) printValues(msg string) {
	fmt.Println(msg)
	p0 := fs.NewPage(t.fm.BlockSize())
	p1 := fs.NewPage(t.fm.BlockSize())
	t.fm.Read(t.blk0, p0)
	t.fm.Read(t.blk1, p1)

	pos := 0
	for i := 0; i < 6; i++ {
		fmt.Print(p0.GetInt(pos), " ")
		fmt.Print(p1.GetInt(pos), " ")
		pos += 4
	}
	fmt.Print(p0.GetString(30), " ")
	fmt.Print(p1.GetString(30), " ")
	fmt.Println()
}
