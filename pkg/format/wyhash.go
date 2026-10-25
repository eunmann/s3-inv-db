package format

import "math/bits"

// wyhash is wyhash final version 4.3 (Wang Yi, public domain,
// github.com/wangyi-fudan/wyhash) with its default secret: it reads the
// key 8 bytes at a time and mixes with 64×64→128-bit multiplies, about
// 5× faster than byte-at-a-time FNV on index prefixes. It must match
// the reference bit for bit: an index stores its hashes, and
// wyhash_test.go checks it against vectors from the reference C code.
func wyhash(key string, seed uint64) uint64 {
	n := len(key)
	seed ^= wymix(seed^wyp0, wyp1)
	var a, b uint64
	switch {
	case n <= 16:
		switch {
		case n >= 4:
			q := (n >> 3) << 2
			a = wyr4(key)<<32 | wyr4(key[q:])
			b = wyr4(key[n-4:])<<32 | wyr4(key[n-4-q:])
		case n > 0:
			a = uint64(key[0])<<16 | uint64(key[n>>1])<<8 | uint64(key[n-1])
		}
	default:
		// p is the read offset and i the bytes left after it. The last
		// two loads end at the key's end and may reach back before p,
		// into bytes already mixed, exactly as the reference does.
		p, i := 0, n
		if i >= wyStripe {
			see1, see2 := seed, seed
			for i >= wyStripe {
				seed = wymix(wyr8(key[p:])^wyp1, wyr8(key[p+8:])^seed)
				see1 = wymix(wyr8(key[p+16:])^wyp2, wyr8(key[p+24:])^see1)
				see2 = wymix(wyr8(key[p+32:])^wyp3, wyr8(key[p+40:])^see2)
				p, i = p+wyStripe, i-wyStripe
			}
			seed ^= see1 ^ see2
		}
		for i > 16 {
			seed = wymix(wyr8(key[p:])^wyp1, wyr8(key[p+8:])^seed)
			p, i = p+16, i-16
		}
		a, b = wyr8(key[p+i-16:]), wyr8(key[p+i-8:])
	}
	hi, lo := bits.Mul64(a^wyp1, b^seed)

	return wymix(lo^wyp0^uint64(n), hi^wyp1)
}

// wyStripe is the bytes the long-key loop mixes per round, as three
// independent 16-byte lanes.
const wyStripe = 48

// The reference's default secret (_wyp).
const (
	wyp0 = 0x2d358dccaa6c78a5
	wyp1 = 0x8bb84b93962eacc9
	wyp2 = 0x4b33a62ed433d4a3
	wyp3 = 0x4d5a2da51de1aa47
)

func wymix(a, b uint64) uint64 {
	hi, lo := bits.Mul64(a, b)

	return hi ^ lo
}

// wyr8 and wyr4 load 8 and 4 bytes little-endian; the compiler merges
// the byte reads into one load.
func wyr8(s string) uint64 {
	_ = s[7]

	return uint64(s[0]) | uint64(s[1])<<8 | uint64(s[2])<<16 | uint64(s[3])<<24 |
		uint64(s[4])<<32 | uint64(s[5])<<40 | uint64(s[6])<<48 | uint64(s[7])<<56
}

func wyr4(s string) uint64 {
	_ = s[3]

	return uint64(s[0]) | uint64(s[1])<<8 | uint64(s[2])<<16 | uint64(s[3])<<24
}
