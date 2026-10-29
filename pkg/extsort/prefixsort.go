package extsort

import (
	"cmp"
	"math/bits"
	"slices"
	"strings"
)

// keyedRow is a row with its prefix beside the pointer, for sorting,
// and key, the prefix's digit at the sort's current depth (digitKey).
type keyedRow struct {
	prefix string
	row    *PrefixRow
	key    uint64
}

const (
	// DigitBytes is how many prefix bytes one key holds: the eighth
	// byte of the key says how many of them the prefix has.
	digitBytes = 7
	// InsertionSortMax is the range size at and below which
	// multikeySort finishes with an insertion sort.
	insertionSortMax = 16
	// Ninther is the range size from which a pivot is the median of
	// three medians of three rather than of three keys.
	ninther = 128
)

// digitKey is the digit of s at depth d: s[d:d+7] big-endian in the top
// seven bytes, zero padded, and how many bytes that is in the low byte.
// For strings that agree on their first d bytes, keys order as the
// strings do on the next seven, a string that ends first ordering
// before, and equal keys counting fewer than seven bytes mean equal
// strings.
func digitKey(s string, d int) uint64 {
	rest := s[d:]
	if len(rest) > digitBytes {
		_ = rest[7]
		k := uint64(rest[0])<<56 | uint64(rest[1])<<48 | uint64(rest[2])<<40 | uint64(rest[3])<<32 |
			uint64(rest[4])<<24 | uint64(rest[5])<<16 | uint64(rest[6])<<8 | uint64(rest[7])

		return k&^0xff | digitBytes
	}
	var k uint64
	for i := range len(rest) {
		k |= uint64(rest[i]) << (8 * (digitBytes - i))
	}

	return k | uint64(len(rest))
}

func loadDigits(s []keyedRow, d int) {
	for i := range s {
		s[i].key = digitKey(s[i].prefix, d)
	}
}

// sortKeyed orders s by prefix in strings.Compare order; its keys must
// hold the prefixes' digits at depth 0. A comparison sort re-reads the
// leading path two prefixes share on every comparison, and spilled
// prefixes share long ones; this is a multikey quicksort (Bentley and
// Sedgewick) over seven-byte digits, which partitions on the keys held
// in s and reads a prefix's bytes about once per digit.
func sortKeyed(s []keyedRow) {
	multikeySort(s, 0, bits.Len(uint(len(s))))
}

// multikeySort sorts s, whose prefixes agree on their first d bytes and
// whose keys hold their digits at d, by three-way partitions on the key:
// the equal part continues at the next digit. It loops on the largest
// part and recurses on the others, which are at most half of s, so the
// stack stays within log2(len(s)) frames. Budget is how many badly
// unbalanced partitions s may still take at this depth before it falls
// back to pdqsort, bounding the worst case as introsort does.
func multikeySort(s []keyedRow, d, budget int) {
	for len(s) > insertionSortMax {
		if budget == 0 {
			slices.SortFunc(s, func(a, b keyedRow) int { return compareKeyed(a, b, d) })

			return
		}
		lt, gt := partition3(s, pivotKey(s))
		less, equal, greater := s[:lt], s[lt:gt], s[gt:]
		if equal[0].key&0xff < digitBytes {
			equal = nil
		} else {
			loadDigits(equal, d+digitBytes)
		}
		if largest := max(len(less), len(greater)); largest > len(s)-len(s)/8 {
			budget--
		}
		switch {
		case len(equal) >= len(less) && len(equal) >= len(greater):
			multikeySort(less, d, budget)
			multikeySort(greater, d, budget)
			s, d, budget = equal, d+digitBytes, bits.Len(uint(len(equal)))
		case len(less) >= len(greater):
			multikeySort(equal, d+digitBytes, bits.Len(uint(len(equal))))
			multikeySort(greater, d, budget)
			s = less
		default:
			multikeySort(less, d, budget)
			multikeySort(equal, d+digitBytes, bits.Len(uint(len(equal))))
			s = greater
		}
	}
	insertionSort(s, d)
}

// partition3 reorders s into keys below v, equal to v and above it,
// returning where the equal run starts and ends; the pivot v must be a
// key in s.
//
//nolint:gocritic // gocritic wants named returns, nonamedreturns forbids them
func partition3(s []keyedRow, v uint64) (int, int) {
	lt, i, gt := 0, 0, len(s)
	for i < gt {
		switch k := s[i].key; {
		case k < v:
			s[lt], s[i] = s[i], s[lt]
			lt++
			i++
		case k > v:
			gt--
			s[i], s[gt] = s[gt], s[i]
		default:
			i++
		}
	}

	return lt, gt
}

// pivotKey is the median of three keys of s, or of three such medians
// for a large s, so sorted, reversed and organ-pipe inputs split evenly.
func pivotKey(s []keyedRow) uint64 {
	n := len(s)
	if n < ninther {
		return median3(s[0].key, s[n/2].key, s[n-1].key)
	}
	e, m := n/8, n/2

	return median3(
		median3(s[0].key, s[e].key, s[2*e].key),
		median3(s[m-e].key, s[m].key, s[m+e].key),
		median3(s[n-1-2*e].key, s[n-1-e].key, s[n-1].key),
	)
}

func median3(a, b, c uint64) uint64 {
	if a > b {
		a, b = b, a
	}

	return max(a, min(b, c))
}

func insertionSort(s []keyedRow, d int) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && compareKeyed(s[j], s[j-1], d) < 0; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// compareKeyed compares a and b, whose prefixes agree on their first d
// bytes and whose keys hold their digits at d.
func compareKeyed(a, b keyedRow, d int) int {
	if a.key != b.key {
		return cmp.Compare(a.key, b.key)
	}
	if a.key&0xff < digitBytes {
		return 0
	}

	return strings.Compare(a.prefix[d+digitBytes:], b.prefix[d+digitBytes:])
}
