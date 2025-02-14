package record

import (
	"sync"

	"github.com/rajivharlalka/parodydb/pkg/fs"
)

type Schema struct {
	fields []string
	info   map[string]*FieldInfo
	mu     *sync.Mutex
}

func NewSchema() *Schema {
	return &Schema{fields: make([]string, 0), info: map[string]*FieldInfo{}, mu: &sync.Mutex{}}
}

func (s *Schema) AddField(fldName string, fieldType int, length int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fields = append(s.fields, fldName)
	s.info[fldName] = NewFieldInfo(fieldType, length)
}

func (s *Schema) AddIntField(fldName string) {
	s.AddField(fldName, INTEGER, 0)
}

func (s *Schema) AddStringField(fldName string, length int) {
	s.AddField(fldName, VARCHAR, length)
}

func (s *Schema) Add(fldname string, sch *Schema) {
	fldType := sch.Type(fldname)
	length := sch.Length(fldname)
	s.AddField(fldname, fldType, length)
}

func (s *Schema) AddAll(sch *Schema) {
	for _, fldname := range sch.fields {
		s.Add(fldname, sch)
	}
}

func (s *Schema) Fields() []string {
	return s.fields
}

func (s *Schema) HasField(fldname string) bool {
	for _, fieldName := range s.fields {
		if fldname == fieldName {
			return true
		}
	}
	return false
}

func (s *Schema) Length(fldname string) int {
	return s.info[fldname].length
}

func (s *Schema) Type(fldname string) int {
	return s.info[fldname].fieldType
}

func (s *Schema) lengthInBytes(fldname string) int {
	fldType := s.Type(fldname)
	if fldType == INTEGER {
		return 4
	} else {
		return fs.MaxLength(s.Length(fldname))
	}
}
