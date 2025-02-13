package concurrency_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rajivharlalka/parodydb/pkg/buffer"
	"github.com/rajivharlalka/parodydb/pkg/fs"
	"github.com/rajivharlalka/parodydb/pkg/logmgr"
	"github.com/rajivharlalka/parodydb/pkg/tx"
)

type ConcurrencyTest struct {
	fm *fs.FileMgr
	bm *buffer.BufferMgr
	lm *logmgr.LogMgr
}

func TestConcurrency(t *testing.T) {
	test := &ConcurrencyTest{}
	test.fm, _ = fs.NewFileManager("concurrency_test", 400)
	test.lm = logmgr.NewLogMgr(test.fm, "log")
	test.bm = buffer.NewBufferMgr(test.fm, test.lm, 8)
	wg := sync.WaitGroup{}
	wg.Add(3)
	go test.RunA(&wg)
	go test.RunB(&wg)
	go test.RunC(&wg)
	wg.Wait()
}

func (c *ConcurrencyTest) RunA(wg *sync.WaitGroup) {
	txA := tx.NewTransaction(c.fm, c.lm, c.bm)
	blk1 := fs.NewBlockId("testfile", 1)
	blk2 := fs.NewBlockId("testfile", 2)
	txA.Pin(blk1)
	txA.Pin(blk2)
	fmt.Println("TX A: Request Slock 1")
	txA.GetInt(blk1, 0)
	fmt.Println("TX A: receive Slock 1")
	time.Sleep(1000 * time.Millisecond)
	fmt.Println("TX A: Request Slock 2")
	txA.GetInt(blk2, 0)
	fmt.Println("TX A: receive Slock 2")
	txA.Commit()
	wg.Done()
}

func (c *ConcurrencyTest) RunB(wg *sync.WaitGroup) {
	txB := tx.NewTransaction(c.fm, c.lm, c.bm)
	blk1 := fs.NewBlockId("testfile", 1)
	blk2 := fs.NewBlockId("testfile", 2)
	txB.Pin(blk1)
	txB.Pin(blk2)
	fmt.Println("TX B: Request Xlock 2")
	txB.SetInt(blk2, 0, 0, false)
	fmt.Println("TX B: receive Xlock 2")
	time.Sleep(1000 * time.Millisecond)
	fmt.Println("TX B: Request Slock 1")
	txB.GetInt(blk1, 0)
	fmt.Println("TX B: receive Slock 1")
	txB.Commit()
	wg.Done()
}

func (c *ConcurrencyTest) RunC(wg *sync.WaitGroup) {
	txC := tx.NewTransaction(c.fm, c.lm, c.bm)
	blk1 := fs.NewBlockId("testfile", 1)
	blk2 := fs.NewBlockId("testfile", 2)
	txC.Pin(blk1)
	txC.Pin(blk2)
	fmt.Println("TX C: Request Xlock 1")
	txC.SetInt(blk1, 0, 0, false)
	fmt.Println("TX C: receive Xlock 1")
	time.Sleep(1000 * time.Millisecond)
	fmt.Println("TX C: Request Slock 2")
	txC.GetInt(blk2, 0)
	fmt.Println("TX C: receive Slock 2")
	txC.Commit()
	wg.Done()
}
