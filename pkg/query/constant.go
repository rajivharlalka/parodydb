package query

import (
	"fmt"
)

// Constant represents a value that can be either an integer or a string.
type Constant struct {
	ival *int
	sval *string
}

// NewIntConstant creates a Constant holding an integer.
func NewConstant(ival interface{}) Constant {
	switch v := ival.(type) {
	case int:
		return Constant{ival: &v, sval: nil}
	case string:
		return Constant{ival: nil, sval: &v}
	default:
		return Constant{}
	}
}

// AsInt returns the integer value, panics if it is not an integer.
func (c Constant) AsInt() int {
	if c.ival == nil {
		panic("Constant does not hold an integer")
	}
	return *c.ival
}

// AsString returns the string value, panics if it is not a string.
func (c Constant) AsString() string {
	if c.sval == nil {
		panic("Constant does not hold a string")
	}
	return *c.sval
}

// Equals checks if two Constants are equal.
func (c Constant) Equals(other Constant) bool {
	if c.ival != nil && other.ival != nil {
		return *c.ival == *other.ival
	}
	if c.sval != nil && other.sval != nil {
		return *c.sval == *other.sval
	}
	return false
}

// CompareTo compares two Constants, returns -1, 0, or 1.
// It assumes both values are of the same type.
func (c Constant) CompareTo(other Constant) int {
	if c.ival != nil && other.ival != nil {
		if *c.ival < *other.ival {
			return -1
		} else if *c.ival > *other.ival {
			return 1
		}
		return 0
	}
	if c.sval != nil && other.sval != nil {
		if *c.sval < *other.sval {
			return -1
		} else if *c.sval > *other.sval {
			return 1
		}
		return 0
	}
	panic("Cannot compare different types")
}

// HashCode returns a hash-like value.
func (c Constant) HashCode() int {
	if c.ival != nil {
		return *c.ival
	}
	if c.sval != nil {
		hash := 0
		for _, ch := range *c.sval {
			hash = 31*hash + int(ch)
		}
		return hash
	}
	return 0
}

// String converts Constant to a string representation.
func (c Constant) String() string {
	if c.ival != nil {
		return fmt.Sprintf("%d", *c.ival)
	}
	if c.sval != nil {
		return *c.sval
	}
	return "null"
}
