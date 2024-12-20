package fs

import (
	"errors"
	"os"
	"path"
	"sync"
)

type FileMgr struct {
	dbDirectory *os.File
	blockSize   uint32
	files       map[string]*os.File
	mu          *sync.Mutex
}

func NewFileManager(directory string, blkSize int) (*FileMgr, error) {
	dir, err := os.Open(directory)
	if err != nil && errors.Is(err, os.ErrNotExist) {
		err := os.Mkdir(directory, os.ModeDir|os.ModePerm)
		if err != nil {
			return nil, err
		}

		dir, err = os.Open(directory)
	}
	if err != nil {
		return nil, err
	}

	return &FileMgr{dbDirectory: dir, blockSize: uint32(blkSize), files: make(map[string]*os.File), mu: &sync.Mutex{}}, nil
}

func (f *FileMgr) Read(blk *BlockId, p *Page) {
	f.mu.Lock()
	defer f.mu.Unlock()
	file := f.getFile(blk.filename)
	file.Seek(int64(blk.blknum)*int64(f.blockSize), 0)
	bytesRead, err := file.Read(p.buffer)
	if err != nil {
		panic(err)
	}
	if bytesRead != len(p.buffer) {
		panic("mismatch in bytes read")
	}
}

func (f *FileMgr) Write(blk *BlockId, p *Page) {
	f.mu.Lock()
	defer f.mu.Unlock()
	file := f.getFile(blk.filename)

	file.Seek(int64(blk.blknum)*int64(f.blockSize), 0)
	bytesWritten, err := file.Write(p.buffer)
	if err != nil {
		panic(err)
	}
	if bytesWritten != len(p.buffer) {
		panic("mismatch in bytes read")
	}
}

func (f *FileMgr) Append(filename string) *BlockId {
	size, _ := f.Length(filename)
	blk := NewBlockId(filename, size)
	b := make([]byte, f.blockSize)

	f.mu.Lock()
	defer f.mu.Unlock()

	file := f.getFile(blk.filename)

	file.Seek(int64(blk.blknum)*int64(f.blockSize), 0)
	bytesWritten, err := file.Write(b)
	if err != nil {
		panic(err)
	}
	if bytesWritten != len(b) {
		panic("mismatch in bytes read")
	}

	return blk
}

func (f *FileMgr) Length(filename string) (int16, error) {
	file := f.getFile(filename)
	stat, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return int16(stat.Size()) / int16(f.blockSize), nil
}

func (f *FileMgr) BlockSize() uint32 {
	return f.blockSize
}

func (f *FileMgr) getFile(filename string) *os.File {
	file, ok := f.files[filename]
	if ok {
		return file
	}
	p := path.Join(f.dbDirectory.Name(), filename)
	file, err := os.Create(p)
	if err != nil {
		panic(err)
	}
	f.files[filename] = file
	return file
}
