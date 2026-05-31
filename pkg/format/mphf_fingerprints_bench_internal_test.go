package format

import (
	"fmt"
	"hash/fnv"
	"testing"
	"unsafe"

	"github.com/relab/bbhash"
)

// BenchmarkHashingStrategies compares the hashing approaches the
// streaming MPHF builder could pick between in its inner loop. The
// production path is FNV1a over a unsafe-coerced byte slice; the
// other rows are the alternatives we A/B against when the inner
// loop becomes hot again.
func BenchmarkHashingStrategies(b *testing.B) {
	prefixes := generateRealisticPrefixes(1000)
	prefixBytes := make([][]byte, len(prefixes))
	for i, p := range prefixes {
		prefixBytes[i] = []byte(p)
	}

	b.Run("FNV1a_String_Alloc", func(b *testing.B) {
		b.ReportAllocs()
		for i := range b.N {
			h := fnv.New64a()
			_, _ = h.Write([]byte(prefixes[i%len(prefixes)]))
			_ = h.Sum64()
		}
	})

	b.Run("FNV1a_Bytes_NoAlloc", func(b *testing.B) {
		b.ReportAllocs()
		for i := range b.N {
			h := fnv.New64a()
			_, _ = h.Write(prefixBytes[i%len(prefixBytes)])
			_ = h.Sum64()
		}
	})

	b.Run("FNV1_ZeroCopy", func(b *testing.B) {
		b.ReportAllocs()
		for i := range b.N {
			_ = fnvZeroCopy(prefixBytes[i%len(prefixBytes)])
		}
	})

	b.Run("FNV1a_ZeroCopy", func(b *testing.B) {
		b.ReportAllocs()
		for i := range b.N {
			_ = fnv1aZeroCopy(prefixBytes[i%len(prefixBytes)])
		}
	})

	// Sink prevents compiler from eliminating the conversions below.
	var sink []byte
	b.Run("StringToBytes_Conversion", func(b *testing.B) {
		b.ReportAllocs()
		for i := range b.N {
			sink = []byte(prefixes[i%len(prefixes)])
		}
	})

	b.Run("UnsafeStringToBytes", func(b *testing.B) {
		b.ReportAllocs()
		for i := range b.N {
			sink = unsafeStringToBytes(prefixes[i%len(prefixes)])
		}
	})
	_ = sink
}

// BenchmarkPerPrefixWork measures the per-prefix work the fingerprint
// phase does, layered as concentric supersets:
//
//	Full     = KeyHash + Find + computeFingerprintBytes
//	ZeroCopy = same shape with the FNV zero-copy variants
//	NoFP     = KeyHash + Find only (drops the fingerprint compute cost)
//
// Reveals what fraction of the per-prefix cost is the fingerprint
// itself vs the mph.Find lookup.
func BenchmarkPerPrefixWork(b *testing.B) {
	const n = 1_000_000
	prefixes := generateRealisticPrefixes(n)
	prefixBytes := make([][]byte, n)
	for i, p := range prefixes {
		prefixBytes[i] = []byte(p)
	}

	hashes := make([]uint64, n)
	for i, p := range prefixes {
		hashes[i] = hashString(p)
	}

	mph, err := bbhash.New(hashes, bbhash.Gamma(2.0))
	if err != nil {
		b.Fatalf("bbhash.New: %v", err)
	}

	fingerprints := make([]uint64, n)
	positions := make([]uint64, n)

	b.Run("Full_KeyHash+Find+Fingerprint", func(b *testing.B) {
		b.ReportAllocs()
		for i := range b.N {
			idx := i % n
			pb := prefixBytes[idx]
			keyHash := hashBytes(pb)
			hashVal := mph.Find(keyHash)
			hashPos := int(hashVal - 1)
			fingerprints[hashPos] = computeFingerprintBytes(pb)
			positions[hashPos] = uint64(idx)
		}
	})

	b.Run("ZeroCopy_KeyHash+Find+Fingerprint", func(b *testing.B) {
		b.ReportAllocs()
		for i := range b.N {
			idx := i % n
			pb := prefixBytes[idx]
			keyHash := fnv1aZeroCopy(pb)
			hashVal := mph.Find(keyHash)
			hashPos := int(hashVal - 1)
			fingerprints[hashPos] = fnvZeroCopy(pb)
			positions[hashPos] = uint64(idx)
		}
	})

	b.Run("NoFingerprint_KeyHash+Find", func(b *testing.B) {
		b.ReportAllocs()
		for i := range b.N {
			idx := i % n
			pb := prefixBytes[idx]
			keyHash := hashBytes(pb)
			hashVal := mph.Find(keyHash)
			hashPos := int(hashVal - 1)
			positions[hashPos] = uint64(idx)
			_ = hashPos
		}
	})
}

// BenchmarkBBHashScaling measures how the bbhash.New construction
// cost grows with input size. Distinct from BenchmarkMPHFBuild
// because it skips Add and only times the MPHF construction step.
// Sizes capped at 1M: generateRealisticPrefixes' segment modulus
// produces duplicate keys past that, which bbhash rejects.
func BenchmarkBBHashScaling(b *testing.B) {
	sizes := []int{100_000, 500_000, 1_000_000}
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			prefixes := generateRealisticPrefixes(n)
			hashes := make([]uint64, n)
			for i, p := range prefixes {
				hashes[i] = hashString(p)
			}
			b.ResetTimer()
			b.ReportAllocs()
			for range b.N {
				if _, err := bbhash.New(hashes, bbhash.Gamma(2.0)); err != nil {
					b.Fatalf("bbhash.New: %v", err)
				}
			}
		})
	}
}

// fnv1aZeroCopy is a zero-allocation FNV-1a implementation used by
// the hashing-strategies micro-bench.
func fnv1aZeroCopy(b []byte) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	hash := uint64(offset64)
	for _, c := range b {
		hash ^= uint64(c)
		hash *= prime64
	}
	return hash
}

// fnvZeroCopy is a zero-allocation FNV-1 implementation used by the
// per-prefix and hashing-strategies micro-benches.
func fnvZeroCopy(b []byte) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	hash := uint64(offset64)
	for _, c := range b {
		hash *= prime64
		hash ^= uint64(c)
	}
	return hash
}

// unsafeStringToBytes converts a string to a byte slice without
// copying. The returned slice must not be modified.
func unsafeStringToBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
