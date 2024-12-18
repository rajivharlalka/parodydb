package fs

import (
	"fmt"
	"hash/fnv"
)

type BlockId struct {
	filename string
	blknum   int16
}

var blockId *BlockId

func NewBlockId(fileName string, blkNum int16) *BlockId {
	return &BlockId{
		filename: fileName,
		blknum:   blkNum,
	}
}

func (bid *BlockId) FileName() string {
	return bid.filename
}

func (bid *BlockId) Number() int16 {
	return bid.blknum
}

func (bid *BlockId) Equals(other *BlockId) bool {
	return bid.blknum == other.blknum && bid.filename == other.filename
}

func (bid *BlockId) ToString() string {
	return fmt.Sprintf("[file %s,block %d]", bid.filename, bid.blknum)
}

func (bid *BlockId) HashCode() uint64 {
	st := bid.ToString()
	h := fnv.New64a()
	h.Write([]byte(st))
	return h.Sum64()
}
