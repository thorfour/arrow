package hashing

import (
	"bytes"
	"unsafe"
)

type BinaryType interface {
	[]byte | string
}

type BinaryBuilder[T BinaryType] interface {
	Reserve(int)
	ReserveData(int)
	Retain()
	Resize(int)
	ResizeData(int)
	Release()
	DataLen() int
	Value(int) []byte
	Len() int
	AppendNull()
	Append(T)
}

type GenericBinaryMemoTable[T BinaryType] struct {
	tbl     *HashTable[int32]
	builder BinaryBuilder[T]
	nullIdx int
}

// NewGenericBinaryMemoTable[T] returns a hash table for Binary data, the passed in allocator will
// be utilized for the BinaryBuilder, if nil then memory.DefaultAllocator will be used.
// initial and valuesize can be used to pre-allocate the table to reduce allocations. With
// initial being the initial number of entries to allocate for and valuesize being the starting
// amount of space allocated for writing the actual binary data.
func NewGenericBinaryMemoTable[T BinaryType](initial, valuesize int, bldr BinaryBuilder[T]) *GenericBinaryMemoTable[T] {
	bldr.Reserve(int(initial))
	datasize := valuesize
	if datasize <= 0 {
		datasize = initial * 4
	}
	bldr.ReserveData(datasize)
	return &GenericBinaryMemoTable[T]{tbl: NewHashTable[int32](uint64(initial)), builder: bldr, nullIdx: KeyNotFound}
}

func (GenericBinaryMemoTable[T]) TypeTraits() TypeTraits {
	return unimplementedtraits{}
}

// Reset dumps all of the data in the table allowing it to be reutilized.
func (s *GenericBinaryMemoTable[T]) Reset() {
	s.tbl.Reset(32)
	s.builder.Resize(0)
	s.builder.ResizeData(0)
	s.builder.Reserve(int(32))
	s.builder.ReserveData(int(32) * 4)
	s.nullIdx = KeyNotFound
}

// GetNull returns the index of a null that has been inserted into the table or
// KeyNotFound. The bool returned will be true if there was a null inserted into
// the table, and false otherwise.
func (s *GenericBinaryMemoTable[T]) GetNull() (int, bool) {
	return int(s.nullIdx), s.nullIdx != KeyNotFound
}

// Size returns the current size of the memo table including the null value
// if one has been inserted.
func (s *GenericBinaryMemoTable[T]) Size() int {
	sz := int(s.tbl.size)
	if _, ok := s.GetNull(); ok {
		sz++
	}
	return sz
}

// helper function to get the hash value regardless of the underlying binary type
func (GenericBinaryMemoTable[T]) getHash(val T) uint64 {
	return Hash([]byte(val), 0)
}

// helper function to append the given value to the builder regardless
// of the underlying binary type.
func (b *GenericBinaryMemoTable[T]) appendVal(val T) {
	b.builder.Append(val)
}

func (b *GenericBinaryMemoTable[T]) lookup(h uint64, val []byte) (*entry[int32], bool) {
	return b.tbl.Lookup(h, func(i int32) bool {
		return bytes.Equal(val, b.builder.Value(int(i)))
	})
}

// Get returns the index of the specified value in the table or KeyNotFound,
// and a boolean indicating whether it was found in the table.
func (b *GenericBinaryMemoTable[T]) Get(val T) (int, bool) {
	if p, ok := b.lookup(b.getHash(val), []byte(val)); ok {
		return int(p.payload.val), ok
	}
	return KeyNotFound, false
}

// GetOrInsert returns the index of the given value in the table, if not found
// it is inserted into the table. The return value 'found' indicates whether the value
// was found in the table (true) or inserted (false) along with any possible error.
func (b *GenericBinaryMemoTable[T]) GetOrInsert(val T) (idx int, found bool, err error) {
	h := b.getHash(val)
	p, found := b.lookup(h, []byte(val))
	if found {
		idx = int(p.payload.val)
	} else {
		idx = b.Size()
		b.appendVal(val)
		b.tbl.Insert(p, h, int32(idx), -1)
	}
	return
}

// GetOrInsertNull retrieves the index of a null in the table or inserts
// null into the table, returning the index and a boolean indicating if it was
// found in the table (true) or was inserted (false).
func (b *GenericBinaryMemoTable[T]) GetOrInsertNull() (idx int, found bool) {
	idx, found = b.GetNull()
	if !found {
		idx = b.Size()
		b.nullIdx = idx
		b.builder.AppendNull()
	}
	return
}

func (b *GenericBinaryMemoTable[T]) Value(i int) []byte {
	return b.builder.Value(i)
}

// helper function to get the offset into the builder data for a given
// index value.
func (b *GenericBinaryMemoTable[T]) findOffset(idx int) uintptr {
	if b.builder.DataLen() == 0 {
		// only empty strings, short circuit
		return 0
	}

	val := b.builder.Value(idx)
	for len(val) == 0 {
		idx++
		if idx >= b.builder.Len() {
			break
		}
		val = b.builder.Value(idx)
	}
	if len(val) != 0 {
		return uintptr(unsafe.Pointer(&val[0]))
	}
	return uintptr(b.builder.DataLen()) + b.findOffset(0)
}

// CopyOffsets copies the list of offsets into the passed in slice, the offsets
// being the start and end values of the underlying allocated bytes in the builder
// for the individual values of the table. out should be at least sized to Size()+1
func (b *GenericBinaryMemoTable[T]) CopyOffsets(out []int32) {
	b.CopyOffsetsSubset(0, out)
}

// CopyOffsetsSubset is like CopyOffsets but instead of copying all of the offsets,
// it gets a subset of the offsets in the table starting at the index provided by "start".
func (b *GenericBinaryMemoTable[T]) CopyOffsetsSubset(start int, out []int32) {
	if b.builder.Len() <= start {
		return
	}

	first := b.findOffset(0)
	delta := b.findOffset(start)
	sz := b.Size()
	for i := start; i < sz; i++ {
		offset := int32(b.findOffset(i) - delta)
		out[i-start] = offset
	}

	out[sz-start] = int32(b.builder.DataLen() - (int(delta) - int(first)))
}

// CopyLargeOffsets copies the list of offsets into the passed in slice, the offsets
// being the start and end values of the underlying allocated bytes in the builder
// for the individual values of the table. out should be at least sized to Size()+1
func (b *GenericBinaryMemoTable[T]) CopyLargeOffsets(out []int64) {
	b.CopyLargeOffsetsSubset(0, out)
}

// CopyLargeOffsetsSubset is like CopyOffsets but instead of copying all of the offsets,
// it gets a subset of the offsets in the table starting at the index provided by "start".
func (b *GenericBinaryMemoTable[T]) CopyLargeOffsetsSubset(start int, out []int64) {
	if b.builder.Len() <= start {
		return
	}

	first := b.findOffset(0)
	delta := b.findOffset(start)
	sz := b.Size()
	for i := start; i < sz; i++ {
		offset := int64(b.findOffset(i) - delta)
		out[i-start] = offset
	}

	out[sz-start] = int64(b.builder.DataLen() - (int(delta) - int(first)))
}

// CopyValues copies the raw binary data bytes out, out should be a []byte
// with at least ValuesSize bytes allocated to copy into.
func (b *GenericBinaryMemoTable[T]) CopyValues(out []byte) {
	b.CopyValuesSubset(0, out)
}

// CopyValuesSubset copies the raw binary data bytes out starting with the value
// at the index start, out should be a []byte with at least ValuesSize bytes allocated
func (b *GenericBinaryMemoTable[T]) CopyValuesSubset(start int, out []byte) {
	if b.builder.Len() <= start {
		return
	}

	var (
		first  = b.findOffset(0)
		offset = b.findOffset(int(start))
		length = b.builder.DataLen() - int(offset-first)
	)

	copy(out, b.builder.Value(start)[0:length])
}

func (b *GenericBinaryMemoTable[T]) WriteOut(out []byte) {
	b.CopyValues(out)
}

func (b *GenericBinaryMemoTable[T]) WriteOutSubset(start int, out []byte) {
	b.CopyValuesSubset(start, out)
}

// CopyFixedWidthValues exists to cope with the fact that the table doesn't keep
// track of the fixed width when inserting the null value the databuffer holds a
// zero length byte slice for the null value (if found)
func (b *GenericBinaryMemoTable[T]) CopyFixedWidthValues(start, width int, out []byte) {
	if start >= b.Size() {
		return
	}

	null, exists := b.GetNull()
	if !exists || null < start {
		// nothing to skip, proceed as usual
		b.CopyValuesSubset(start, out)
		return
	}

	var (
		leftOffset  = b.findOffset(start)
		nullOffset  = b.findOffset(null)
		leftSize    = nullOffset - leftOffset
		rightOffset = leftOffset + uintptr(b.ValuesSize())
	)

	if leftSize > 0 {
		copy(out, b.builder.Value(start)[0:leftSize])
	}

	rightSize := rightOffset - nullOffset
	if rightSize > 0 {
		// skip the null fixed size value
		copy(out[int(leftSize)+width:], b.builder.Value(null + 1)[0:rightSize])
	}
}

// VisitValues exists to run the visitFn on each value currently in the hash table.
func (b *GenericBinaryMemoTable[T]) VisitValues(start int, visitFn func([]byte)) {
	for i := int(start); i < b.Size(); i++ {
		visitFn(b.builder.Value(i))
	}
}

// Release is used to tell the underlying builder that it can release the memory allocated
// when the reference count reaches 0, this is safe to be called from multiple goroutines
// simultaneously
func (b *GenericBinaryMemoTable[T]) Release() { b.builder.Release() }

// Retain increases the ref count, it is safe to call it from multiple goroutines
// simultaneously.
func (b *GenericBinaryMemoTable[T]) Retain() { b.builder.Retain() }

// ValuesSize returns the current total size of all the raw bytes that have been inserted
// into the memotable so far.
func (b *GenericBinaryMemoTable[T]) ValuesSize() int { return b.builder.DataLen() }
