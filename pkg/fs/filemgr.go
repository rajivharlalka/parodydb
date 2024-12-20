package fs

import (
	"errors"
	"os"
	"path"
	"sync"
)

type fileMgr struct {
	dbDirectory *os.File
	blockSize   uint32
	files       map[string]*os.File
	mu          *sync.Mutex
}

func NewFileManager(directory string, blkSize int) (*fileMgr, error) {
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

	return &fileMgr{dbDirectory: dir, blockSize: uint32(blkSize), files: make(map[string]*os.File), mu: &sync.Mutex{}}, nil
}

func (f *fileMgr) Read(blk BlockId, p Page) {
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

func (f *fileMgr) Write(blk BlockId, p Page) {
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

func (f *fileMgr) Append(filename string) {
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
}

func (f *fileMgr) Length(filename string) (int16, error) {
	file := f.getFile(filename)
	stat, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return int16(stat.Size()) / int16(f.blockSize), nil
}

func (f *fileMgr) BlockSize() uint32 {
	return f.blockSize
}

func (f *fileMgr) getFile(filename string) *os.File {
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
