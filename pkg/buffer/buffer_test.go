package buffer

import (
	"testing"

	"github.com/rajivharlalka/parodydb/pkg/fs"
	"github.com/rajivharlalka/parodydb/pkg/logmgr"
)

func TestBuffers(t *testing.T) {
	fm, err := fs.NewFileManager("fs_test", 400)
	if err != nil {
		t.Fatalf("Failed to create File Manager %v", err)
	}
	lm := logmgr.NewLogMgr(fm, "logFile")
	bm := NewBufferMgr(fm, lm, 2)
	buff1, _ := bm.Pin(fs.NewBlockId("testFile", 1))
	p := buff1.Contents
	n := p.GetInt(80)
	buff1.SetModified(1, 0)
	p.SetInt(80, n+1)
	t.Logf("The new value is %d", n+1)
	bm.Unpin(buff1)

	buff2, _ := bm.Pin(fs.NewBlockId("testFile", 2))
	bm.Pin(fs.NewBlockId("testFile", 3))
	bm.Pin(fs.NewBlockId("testFile", 4))

	bm.Unpin(buff2)
	buff2, _ = bm.Pin(fs.NewBlockId("testFile", 1))
	p2 := buff2.Contents
	p2.SetInt(80, 9999)
	buff2.SetModified(1, 0)
	bm.Unpin(buff2)
}
