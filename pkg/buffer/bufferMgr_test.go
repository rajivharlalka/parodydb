package buffer

import (
	"testing"

	"github.com/rajivharlalka/parodydb/pkg/fs"
	"github.com/rajivharlalka/parodydb/pkg/logmgr"
)

func TestBufferMgr(t *testing.T) {
	fm, err := fs.NewFileManager("fs_test", 400)
	if err != nil {
		t.Fatalf("Failed to create File Manager %v", err)
	}
	lm := logmgr.NewLogMgr(fm, "logFile")
	bm := NewBufferMgr(fm, lm, 3)

	buff := make([]*Buffer, 6)
	buff[0], _ = bm.pin(fs.NewBlockId("testFile", 0))
	buff[1], _ = bm.pin(fs.NewBlockId("testFile", 1))
	buff[2], _ = bm.pin(fs.NewBlockId("testFile", 2))
	bm.unpin(buff[1])
	buff[1] = nil
	buff[3], _ = bm.pin(fs.NewBlockId("testFile", 0))
	buff[4], _ = bm.pin(fs.NewBlockId("testFile", 1))
	t.Logf("Available buffers: %d", bm.available())
	buff[5], err = bm.pin(fs.NewBlockId("testFile", 3))
	if err != nil {
		t.Logf("No Buffers Available")
	}

	bm.unpin(buff[2])
	buff[2] = nil
	buff[5], _ = bm.pin(fs.NewBlockId("testFile", 3))
	t.Log("Final Buffer Allocation\n")
	for i := 0; i < len(buff); i++ {
		b := buff[i]
		if b != nil {
			t.Logf("buff[%d] pinned to block %v", i, b.Block)
		}
	}
}

// System.out.println("Final Buffer Allocation:");
// for (int i=0; i<buff.length; i++) {
// Buffer b = buff[i];
// if (b != null)
// System.out.println("buff["+i+"] pinned to block "
//  + b.block());
// }
