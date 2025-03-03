package fs

import (
	"errors"
	"io"
	"os"
	"path"
	"sync"
)

type FileMgr struct {
	dbDirectory *os.File
	blockSize   int
	files       map[string]*os.File
	mu          *sync.Mutex
	isNew       bool
}

func NewFileManager(directory string, blkSize int) (*FileMgr, error) {
	dir, err := os.Open(directory)
	isNew := false
	if err != nil {
		isNew = errors.Is(err, os.ErrNotExist)

		if isNew {
			err := os.Mkdir(directory, os.ModeDir|os.ModePerm)
			if err != nil {
				return nil, err
			}

			dir, _ = os.Open(directory)
		} else {
			return nil, err
		}
	}

	return &FileMgr{dbDirectory: dir, blockSize: blkSize, files: make(map[string]*os.File), mu: &sync.Mutex{}, isNew: isNew}, nil
}

func (f *FileMgr) Read(blk *BlockId, p *Page) {
	f.mu.Lock()
	defer f.mu.Unlock()
	file := f.getFile(blk.filename)
	file.Seek(int64(blk.blknum)*int64(f.blockSize), 0)
	bytesRead, err := file.Read(p.buffer)
	if err != nil {
		if err == io.EOF {
			return
		}
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

	_, err := file.Seek(int64(blk.blknum)*int64(f.blockSize), 0)
	if err != nil {
		panic(err)
	}
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

func (f *FileMgr) Length(filename string) (int, error) {
	file := f.getFile(filename)
	stat, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return int(int32(stat.Size()) / int32(f.blockSize)), nil
}

func (f *FileMgr) BlockSize() int {
	return f.blockSize
}

func (f *FileMgr) getFile(filename string) *os.File {
	file, has := f.files[filename]
	if has {
		return file
	}
	var err error
	p := path.Join(f.dbDirectory.Name(), filename)
	if file, err = os.OpenFile(p, os.O_RDWR, os.ModeAppend); errors.Is(err, os.ErrNotExist) {
		file, err = os.Create(p)
		if err != nil {
			panic(err)
		}
	}
	f.files[filename] = file
	return file
}

func (f *FileMgr) IsNew() bool {
	return f.isNew
}
