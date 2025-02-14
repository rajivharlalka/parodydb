package record

type FieldInfo struct {
	fieldType int
	length    int
}

func NewFieldInfo(f, l int) *FieldInfo {
	return &FieldInfo{f, l}
}

const (
	INTEGER = iota
	VARCHAR
)
