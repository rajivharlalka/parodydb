package server

import (
	"fmt"

	"github.com/rajivharlalka/parodydb/pkg/buffer"
	"github.com/rajivharlalka/parodydb/pkg/fs"
	"github.com/rajivharlalka/parodydb/pkg/logmgr"
	"github.com/rajivharlalka/parodydb/pkg/tx"
)

const (
	BLOCK_SIZE  int    = 400
	LOG_FILE    string = "parodydb.log"
	BUFFER_SIZE int    = 8
)

type ParodyDB struct {
	fm *fs.FileMgr
	bm *buffer.BufferMgr
	lm *logmgr.LogMgr
}

func newSimpleDB(dirname string, blockSize int, buffSize int) *ParodyDB {
	fm, err := fs.NewFileManager(dirname, blockSize)
	if err != nil {
		panic(fmt.Sprintf("cannot create database, error %v", err.Error()))
	}
	lm := logmgr.NewLogMgr(fm, LOG_FILE)
	bm := buffer.NewBufferMgr(fm, lm, buffSize)
	return &ParodyDB{fm, bm, lm}
}

func NewSimpleDB(dirname string) *ParodyDB {
	parody := newSimpleDB(dirname, BLOCK_SIZE, BUFFER_SIZE)
	tx := parody.NewTx()
	isNew := parody.fm.IsNew()

	if isNew {
		fmt.Print("Createing New database!\n")
	}

	tx.Commit()
	return parody
}

func (p *ParodyDB) NewTx() *tx.Transaction {
	return tx.NewTransaction(p.fm, p.lm, p.bm)
}

func (p *ParodyDB) FileMgr() *fs.FileMgr {
	return p.fm
}

func (p *ParodyDB) LogMgr() *logmgr.LogMgr {
	return p.lm
}

func (p *ParodyDB) BufferMgr() *buffer.BufferMgr {
	return p.bm
}
