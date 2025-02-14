package record

import "sync"

type Layout struct {
	schema   *Schema
	offsets  map[string]int
	slotsize int
	mu       *sync.Mutex
}

func NewLayout(sch *Schema) *Layout {
	pos := 4 // empty/inuse flag bits
	offsets := make(map[string]int)
	for _, fldname := range sch.fields {
		offsets[fldname] = pos
		pos += sch.lengthInBytes(fldname)
	}
	return &Layout{schema: sch, offsets: offsets, slotsize: pos, mu: &sync.Mutex{}}
}

func NewLayoutFromData(sch *Schema, offsets *map[string]int, slotsize int) *Layout {
	return &Layout{sch, *offsets, slotsize, &sync.Mutex{}}
}

func (l *Layout) Schema() *Schema {
	return l.schema
}

func (l *Layout) Offset(fldname string) int {
	return l.offsets[fldname]
}

func (l *Layout) Slotsize() int {
	return l.slotsize
}
