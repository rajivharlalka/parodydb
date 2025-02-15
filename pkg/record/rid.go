package record

import "fmt"

type Rid struct {
	blkname int
	slot    int
}

func NewRID(blkname, slot int) *Rid {
	return &Rid{blkname, slot}
}

func (r *Rid) BlockNumber() int {
	return r.blkname
}

func (r *Rid) Slot() int {
	return r.slot
}

func (r *Rid) Equals(obj any) bool {
	rid := obj.(Rid)
	return r.blkname == rid.blkname && r.slot == rid.slot
}

func (r *Rid) ToString() string {
	return fmt.Sprintf("[%d, %d]", r.blkname, r.slot)
}
