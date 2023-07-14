package hashing

import (
	"reflect"
	"unsafe"

	"github.com/apache/arrow/go/v13/arrow/bitutil"
)

type payload[T any] struct {
	val     T
	memoIdx int32
}

type entry[T any] struct {
	h       uint64
	payload payload[T]
}

func (e entry[T]) Valid() bool { return e.h != sentinel }

type HashTable[T any] struct {
	cap     uint64
	capMask uint64
	size    uint64

	entries []entry[T]
}

func NewHashTable[T any](cap uint64) *HashTable[T] {
	initCap := uint64(bitutil.NextPowerOf2(int(max(cap, 32))))
	ret := &HashTable[T]{
		cap:     initCap,
		capMask: initCap - 1,
		size:    0,
	}

	ret.entries = make([]entry[T], initCap)
	return ret
}

func CastFromBytes[T any](b []byte) []T {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var v T
	size := int(unsafe.Sizeof(v))
	return unsafe.Slice((*T)(unsafe.Pointer(h.Data)), cap(b)/size)[:len(b)/size]
}

// Reset drops all of the values in this hash table and re-initializes it
// with the specified initial capacity as if by calling New, but without having
// to reallocate the object.
func (h *HashTable[T]) Reset(cap uint64) {
	h.cap = uint64(bitutil.NextPowerOf2(int(max(cap, 32))))
	h.capMask = h.cap - 1
	h.size = 0
	h.entries = make([]entry[T], h.cap)
}

// CopyValues is used for copying the values out of the hash table into the
// passed in slice, in the order that they were first inserted
func (h *HashTable[T]) CopyValues(out []T) {
	h.CopyValuesSubset(0, out)
}

// CopyValuesSubset copies a subset of the values in the hashtable out, starting
// with the value at start, in the order that they were inserted.
func (h *HashTable[T]) CopyValuesSubset(start int, out []T) {
	h.VisitEntries(func(e *entry[T]) {
		idx := e.payload.memoIdx - int32(start)
		if idx >= 0 {
			out[idx] = e.payload.val
		}
	})
}

func (h *HashTable[T]) WriteOut(out []byte) {
	h.WriteOutSubset(0, out)
}

func (h *HashTable[T]) WriteOutSubset(start int, out []byte) {
	h.CopyValuesSubset(start, CastFromBytes[T](out))
}

func (h *HashTable[T]) needUpsize() bool { return h.size*uint64(loadFactor) >= h.cap }

func (HashTable[T]) fixHash(v uint64) uint64 {
	if v == sentinel {
		return 42
	}
	return v
}

// Lookup retrieves the entry for a given hash value assuming it's payload value returns
// true when passed to the cmp func. Returns a pointer to the entry for the given hash value,
// and a boolean as to whether it was found. It is not safe to use the pointer if the bool is false.
func (h *HashTable[T]) Lookup(v uint64, cmp func(T) bool) (*entry[T], bool) {
	idx, ok := h.lookup(v, h.capMask, cmp)
	return &h.entries[idx], ok
}

func (h *HashTable[T]) lookup(v uint64, szMask uint64, cmp func(T) bool) (uint64, bool) {
	const perturbShift uint8 = 5

	var (
		idx     uint64
		perturb uint64
		e       *entry[T]
	)

	v = h.fixHash(v)
	idx = v & szMask
	perturb = (v >> uint64(perturbShift)) + 1

	for {
		e = &h.entries[idx]
		if e.h == v && cmp(e.payload.val) {
			return idx, true
		}

		if e.h == sentinel {
			return idx, false
		}

		// perturbation logic inspired from CPython's set/dict object
		// the goal is that all 64 bits of unmasked hash value eventually
		// participate int he probing sequence, to minimize clustering
		idx = (idx + perturb) & szMask
		perturb = (perturb >> uint64(perturbShift)) + 1
	}
}

func (h *HashTable[T]) upsize(newcap uint64) error {
	newMask := newcap - 1

	oldEntries := h.entries
	h.entries = make([]entry[T], newcap)
	for _, e := range oldEntries {
		if e.Valid() {
			idx, _ := h.lookup(e.h, newMask, func(T) bool { return false })
			h.entries[idx] = e
		}
	}
	h.cap = newcap
	h.capMask = newMask
	return nil
}

// Insert updates the given entry with the provided hash value, payload value and memo index.
// The entry pointer must have been retrieved via lookup in order to actually insert properly.
func (h *HashTable[T]) Insert(e *entry[T], v uint64, val T, memoIdx int32) error {
	e.h = h.fixHash(v)
	e.payload.val = val
	e.payload.memoIdx = memoIdx
	h.size++

	if h.needUpsize() {
		h.upsize(h.cap * uint64(loadFactor) * 2)
	}
	return nil
}

// VisitEntries will call the passed in function on each *valid* entry in the hash table,
// a valid entry being one which has had a value inserted into it.
func (h *HashTable[T]) VisitEntries(visit func(*entry[T])) {
	for _, e := range h.entries {
		if e.Valid() {
			visit(&e)
		}
	}
}
