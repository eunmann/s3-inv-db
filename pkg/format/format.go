// Package format defines the columnar file formats for the index.
package format

import "encoding/binary"

const (
	// MagicNumber identifies s3inv-index files.
	MagicNumber uint32 = 0x53334944 // "S3ID"
	// Version is the current format version.
	Version uint32 = 1
)

// Header is the common header for all columnar files.
type Header struct {
	Magic   uint32
	Version uint32
	Count   uint64 // Number of elements
	Width   uint32 // Element width in bytes (e.g., 4 for u32, 8 for u64)
}

// HeaderSize is the size of the header in bytes.
const HeaderSize = 4 + 4 + 8 + 4 // 20 bytes

// EncodeHeader writes a header to a byte slice.
func EncodeHeader(h Header) []byte {
	buf := make([]byte, HeaderSize)
	binary.LittleEndian.PutUint32(buf[0:4], h.Magic)
	binary.LittleEndian.PutUint32(buf[4:8], h.Version)
	binary.LittleEndian.PutUint64(buf[8:16], h.Count)
	binary.LittleEndian.PutUint32(buf[16:20], h.Width)
	return buf
}

// DecodeHeader reads a header from a byte slice.
func DecodeHeader(buf []byte) (Header, error) {
	if len(buf) < HeaderSize {
		return Header{}, ErrInvalidHeader
	}
	return Header{
		Magic:   binary.LittleEndian.Uint32(buf[0:4]),
		Version: binary.LittleEndian.Uint32(buf[4:8]),
		Count:   binary.LittleEndian.Uint64(buf[8:16]),
		Width:   binary.LittleEndian.Uint32(buf[16:20]),
	}, nil
}
