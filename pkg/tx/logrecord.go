package tx

import (
	"github.com/rajivharlalka/parodydb/pkg/fs"
)

const (
	CHECKPOINT = iota
	START
	COMMIT
	ROLLBACK
	SETINT
	SETSTRING
)

type LogRecord interface {
	Op() int
	TxNumber() int
	Undo(tx *Transaction)
}

// Factory function to create LogRecord from byte slice
func CreateLogRecord(bytes []byte) LogRecord {
	p := fs.NewPageFromBytes(bytes)
	switch p.GetInt(0) {
	case CHECKPOINT:
		return NewCheckpointRecord(p)
	case START:
		return NewStartRecord(p)
	case COMMIT:
		return NewCommitRecord(p)
	case ROLLBACK:
		return NewRollbackRecord(p)
	case SETINT:
		return NewSetIntRecord(p)
	case SETSTRING:
		return NewSetStringRecord(p)
	default:
		return nil
	}
}
