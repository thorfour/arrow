package hashing

import (
	"unsafe"

	"golang.org/x/exp/constraints"
)

type BasicMemoTable[T constraints.Integer | constraints.Float] struct {
	tbl     *HashTable[T]
	nullIdx int32
}

// NewBasicMemoTable[T] returns a new memotable with num entries pre-allocated to reduce further
// allocations when inserting.
func NewBasicMemoTable[T constraints.Integer | constraints.Float](num int64) *BasicMemoTable[T] {
	return &BasicMemoTable[T]{tbl: NewHashTable[T](uint64(num)), nullIdx: KeyNotFound}
}

type BasicTypeTraits[T any] struct{}

func (BasicMemoTable[T]) TypeTraits() TypeTraits {
	return BasicTypeTraits[T]{}
}

func (BasicTypeTraits[T]) BytesRequired(n int) int {
	var v T
	size := int(unsafe.Sizeof(v))
	return size * n
}

// Reset allows this table to be re-used by dumping all the data currently in the table.
func (s *BasicMemoTable[T]) Reset() {
	s.tbl.Reset(32)
	s.nullIdx = KeyNotFound
}

// Size returns the current number of inserted elements into the table including if a null
// has been inserted.
func (s *BasicMemoTable[T]) Size() int {
	sz := int(s.tbl.size)
	if _, ok := s.GetNull(); ok {
		sz++
	}
	return sz
}

// GetNull returns the index of an inserted null or KeyNotFound along with a bool
// that will be true if found and false if not.
func (s *BasicMemoTable[T]) GetNull() (int, bool) {
	return int(s.nullIdx), s.nullIdx != KeyNotFound
}

// GetOrInsertNull will return the index of the null entry or insert a null entry
// if one currently doesn't exist. The found value will be true if there was already
// a null in the table, and false if it inserted one.
func (s *BasicMemoTable[T]) GetOrInsertNull() (idx int, found bool) {
	idx, found = s.GetNull()
	if !found {
		idx = s.Size()
		s.nullIdx = int32(idx)
	}
	return
}

// CopyValues will copy the values from the memo table out into the passed in slice
// which must be of the appropriate type.
func (s *BasicMemoTable[T]) CopyValues(out []T) {
	s.CopyValuesSubset(0, out)
}

// CopyValuesSubset is like CopyValues but only copies a subset of values starting
// at the provided start index
func (s *BasicMemoTable[T]) CopyValuesSubset(start int, out []T) {
	s.tbl.CopyValuesSubset(start, out)
}

func (s *BasicMemoTable[T]) WriteOut(out []byte) {
	s.tbl.CopyValues(CastFromBytes[T](out))
}

func (s *BasicMemoTable[T]) WriteOutSubset(start int, out []byte) {
	s.tbl.CopyValuesSubset(start, CastFromBytes[T](out))
}

func (s *BasicMemoTable[T]) WriteOutLE(out []byte) {
	s.tbl.WriteOut(out)
}

func (s *BasicMemoTable[T]) WriteOutSubsetLE(start int, out []byte) {
	s.tbl.WriteOutSubset(start, out)
}

// Get returns the index of the requested value in the hash table or KeyNotFound
// along with a boolean indicating if it was found or not.
func (s *BasicMemoTable[T]) Get(val T) (int, bool) {

	h := hashInt(uint64(val), 0)
	if e, ok := s.tbl.Lookup(h, func(v T) bool { return val == v }); ok {
		return int(e.payload.memoIdx), ok
	}
	return KeyNotFound, false
}

// GetOrInsert will return the index of the specified value in the table, or insert the
// value into the table and return the new index. found indicates whether or not it already
// existed in the table (true) or was inserted by this call (false).
func (s *BasicMemoTable[T]) GetOrInsert(val T) (idx int, found bool, err error) {

	h := hashInt(uint64(val), 0)
	e, ok := s.tbl.Lookup(h, func(v T) bool {
		return val == v
	})

	if ok {
		idx = int(e.payload.memoIdx)
		found = true
	} else {
		idx = s.Size()
		s.tbl.Insert(e, h, val, int32(idx))
	}
	return
}
