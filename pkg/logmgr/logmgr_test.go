package logmgr_test

import (
	"fmt"
	"testing"

	"github.com/rajivharlalka/parodydb/pkg/fs"
	"github.com/rajivharlalka/parodydb/pkg/logmgr"
)

type LogTest struct {
	lm *logmgr.LogMgr
}

func TestLogOperations(t *testing.T) {
	// Create a FileMgr instance first
	fileMgr, err := fs.NewFileManager("logtest", 400)
	if err != nil {
		t.Fatalf("Failed to create file manager: %v", err)
	}

	// Create LogMgr with the FileMgr
	logManager := logmgr.NewLogMgr(fileMgr, "logfile")
	if err != nil {
		t.Fatalf("Failed to create log manager: %v", err)
	}

	test := &LogTest{
		lm: logManager,
	}

	// Test first batch of records
	test.createRecords(t, 1, 35)
	test.printLogRecords(t, "The log file now has these records:")

	// Test second batch of records
	test.createRecords(t, 36, 70)
	test.lm.Flush(65)
	test.printLogRecords(t, "The log file now has these records:")
}

func (lt *LogTest) printLogRecords(t *testing.T, msg string) {
	t.Log(msg)

	iter := lt.lm.Iterator()
	for iter.HasNext() {
		rec := iter.Next()

		p := fs.NewPageFromBytes(rec)
		s := p.GetString(0)

		npos := fs.MaxLength(len(s))
		val := p.GetInt(npos)

		t.Logf("[%s, %d]", s, val)
	}
	t.Log()
}

func (lt *LogTest) createRecords(t *testing.T, start, end int) {
	t.Log("Creating records: ")
	for i := start; i <= end; i++ {
		rec, err := createLogRecord(fmt.Sprintf("record%d", i), i+100)
		if err != nil {
			t.Errorf("Failed to create log record: %v", err)
			continue
		}

		lsn := lt.lm.Append(rec)

		t.Logf("%d ", lsn)
	}
	t.Log()
}

func createLogRecord(s string, n int) ([]byte, error) {
	npos := fs.MaxLength(len(s))
	p := fs.NewPageFromBytes(make([]byte, npos+4)) // Page size will be determined by the content

	p.SetString(0, s)

	p.SetInt(npos, int32(n))

	return p.Contents(), nil
}
