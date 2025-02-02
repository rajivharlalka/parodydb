package fs

import (
	"bytes"
	"encoding/binary"
)

// Page represents a fixed-size page of bytes that can store various data types
type Page struct {
	buffer []byte
}

// NewPage creates a new Page with the specified block size
func NewPage(blocksize int) *Page {
	return &Page{
		buffer: make([]byte, blocksize),
	}
}

// NewPageFromBytes creates a new Page from an existing byte slice
func NewPageFromBytes(b []byte) *Page {
	return &Page{
		buffer: b,
	}
}

// GetInt reads an integer from the specified offset
func (p *Page) GetInt(offset int) int {
	return int(binary.BigEndian.Uint32(p.buffer[offset:]))
}

// SetInt writes an integer to the specified offset
func (p *Page) SetInt(offset int, n int) {
	binary.BigEndian.PutUint32(p.buffer[offset:], uint32(n))
}

// GetBytes reads a byte slice from the specified offset
// The first 4 bytes at the offset specify the length of the data
func (p *Page) GetBytes(offset int) []byte {
	length := p.GetInt(offset)
	start := offset + 4 // Skip the length bytes
	return bytes.Clone(p.buffer[start : start+int(length)])
}

// SetBytes writes a byte slice to the specified offset
// It first writes the length of the slice (4 bytes) followed by the actual data
func (p *Page) SetBytes(offset int, b []byte) {
	p.SetInt(offset, len(b))
	copy(p.buffer[offset+4:], b)
}

// GetString reads a string from the specified offset
func (p *Page) GetString(offset int) string {
	b := p.GetBytes(offset)
	return string(b)
}

// SetString writes a string to the specified offset
func (p *Page) SetString(offset int, s string) {
	p.SetBytes(offset, []byte(s))
}

// MaxLength calculates the maximum number of bytes needed to store a string of given length
// In Go, since we're using ASCII, each char is 1 byte
func MaxLength(strlen int) int {
	return 4 + strlen // 4 bytes for length + string bytes
}

// Contents returns the underlying byte slice
// This is equivalent to the package-private contents() method in Java
func (p *Page) Contents() []byte {
	return p.buffer
}
