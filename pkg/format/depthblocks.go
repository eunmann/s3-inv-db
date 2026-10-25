package format

import (
	"math"
	"math/bits"
)

// DepthBlockSize is how many consecutive rows of one depth a block
// summary bounds. Blocks start at each depth's first row and never
// straddle two depths.
const DepthBlockSize = 64

// BlockBounds bounds the stats of the prefixes in one depth block:
// every prefix's object count lies in [MinCount, MaxCount] and its
// bytes in [MinBytes, MaxBytes]. The bounds are stored with 11
// significant bits, rounded outward (a relative error below 0.1%), so
// they may sit just outside the block's actual extremes, never inside.
type BlockBounds struct {
	MinCount, MaxCount uint64
	MinBytes, MaxBytes uint64
}

// boundMantBits is the stored mantissa of a bound code; the leading bit
// is implicit above the first 1<<boundMantBits values.
const boundMantBits = 10

// boundMaxExp is the largest exponent whose value fits in a uint64.
const boundMaxExp = 64 - boundMantBits

// boundValue decodes a bound code. Codes below 1<<boundMantBits are the
// value itself; above, the top six bits are an exponent e >= 1 and the
// value is (1<<boundMantBits | mantissa) << (e-1). Values grow with the
// code; codes past the uint64 range saturate.
func boundValue(c uint16) uint64 {
	e, m := uint(c>>boundMantBits), uint64(c&(1<<boundMantBits-1))
	if e == 0 {
		return m
	}
	if e > boundMaxExp {
		return math.MaxUint64
	}

	return (1<<boundMantBits | m) << (e - 1)
}

// boundCodeDown returns the largest code whose value is at most v.
func boundCodeDown(v uint64) uint16 {
	if v < 1<<boundMantBits {
		return uint16(v)
	}
	e := uint(bits.Len64(v)) - boundMantBits

	return uint16(e<<boundMantBits | uint(v>>(e-1))&(1<<boundMantBits-1))
}

// boundCodeUp returns the smallest code whose value is at least v.
func boundCodeUp(v uint64) uint16 {
	c := boundCodeDown(v)
	if boundValue(c) < v {
		c++
	}

	return c
}

// boundCodeBits is one bound code's width; a block packs four into a
// uint64: MinCount, MaxCount, MinBytes, MaxBytes from the low end.
const boundCodeBits = 16

// packBlockBounds stores a block's extremes as four bound codes, the
// minimums rounded down and the maximums up.
func packBlockBounds(b BlockBounds) uint64 {
	return uint64(boundCodeDown(b.MinCount)) |
		uint64(boundCodeUp(b.MaxCount))<<boundCodeBits |
		uint64(boundCodeDown(b.MinBytes))<<(2*boundCodeBits) |
		uint64(boundCodeUp(b.MaxBytes))<<(3*boundCodeBits)
}

func unpackBlockBounds(u uint64) BlockBounds {
	return BlockBounds{
		MinCount: boundValue(uint16(u)),
		MaxCount: boundValue(uint16(u >> boundCodeBits)),
		MinBytes: boundValue(uint16(u >> (2 * boundCodeBits))),
		MaxBytes: boundValue(uint16(u >> (3 * boundCodeBits))),
	}
}

// blockAcc accumulates one depth's open block while it is built.
type blockAcc struct {
	n      int
	bounds BlockBounds
}

func (a *blockAcc) add(count, bytes uint64) {
	if a.n == 0 {
		a.bounds = BlockBounds{MinCount: count, MaxCount: count, MinBytes: bytes, MaxBytes: bytes}
	} else {
		a.bounds.MinCount = min(a.bounds.MinCount, count)
		a.bounds.MaxCount = max(a.bounds.MaxCount, count)
		a.bounds.MinBytes = min(a.bounds.MinBytes, bytes)
		a.bounds.MaxBytes = max(a.bounds.MaxBytes, bytes)
	}
	a.n++
}

// SpanBlocks is the run of depth blocks covering one span of a depth's
// rows, numbered from 0 at the span's first block.
type SpanBlocks struct {
	d     *DepthIndex
	span  Span
	start uint64 // first row of block 0
	base  uint64 // block 0's index in the blocks file
	n     uint64
}

// SpanBlocks returns the blocks covering span, rows of one depth.
func (d *DepthIndex) SpanBlocks(depth uint32, span Span) SpanBlocks {
	if span.Len() == 0 || depth > d.maxDepth {
		return SpanBlocks{}
	}
	depthStart := d.starts[depth]
	first := (span.Lo - depthStart) / DepthBlockSize
	last := (span.Hi - 1 - depthStart) / DepthBlockSize

	return SpanBlocks{
		d:     d,
		span:  span,
		start: depthStart + first*DepthBlockSize,
		base:  d.blockBase[depth] + first,
		n:     last - first + 1,
	}
}

// Len is the number of blocks.
func (s SpanBlocks) Len() uint64 { return s.n }

// Bounds returns block i's bounds. A block the span covers only partly
// keeps the bounds of the whole block, still bounds for its part.
func (s SpanBlocks) Bounds(i uint64) BlockBounds {
	return unpackBlockBounds(s.d.blocks.UnsafeGetU64(s.base + i))
}

// Entries returns the rows of block i inside the span.
func (s SpanBlocks) Entries(i uint64) Span {
	lo := s.start + i*DepthBlockSize

	return Span{Lo: max(lo, s.span.Lo), Hi: min(lo+DepthBlockSize, s.span.Hi)}
}

// MaxFloor returns how small the largest value behind a stored maximum
// can be: a block whose MaxCount (or MaxBytes) is bound holds a value
// in [MaxFloor(bound), bound].
func MaxFloor(bound uint64) uint64 {
	c := boundCodeUp(bound)
	if c == 0 {
		return 0
	}

	return boundValue(c-1) + 1
}

// MinCeil returns how large the smallest value behind a stored minimum
// can be: a block whose MinCount (or MinBytes) is bound holds a value
// in [bound, MinCeil(bound)].
func MinCeil(bound uint64) uint64 {
	c := boundCodeDown(bound)
	if c == math.MaxUint16 {
		return math.MaxUint64
	}
	next := boundValue(c + 1)
	if next == math.MaxUint64 {
		return math.MaxUint64
	}

	return next - 1
}
