package format

import (
	"testing"
)

func TestHeaderRoundTrip(t *testing.T) {
	h := Header{
		Magic:   MagicNumber,
		Version: Version,
		Count:   12345,
		Width:   8,
	}

	encoded := EncodeHeader(h)
	if len(encoded) != HeaderSize {
		t.Fatalf("encoded size = %d, want %d", len(encoded), HeaderSize)
	}

	decoded, err := DecodeHeader(encoded)
	if err != nil {
		t.Fatalf("DecodeHeader failed: %v", err)
	}

	if decoded.Magic != h.Magic {
		t.Errorf("Magic = %x, want %x", decoded.Magic, h.Magic)
	}
	if decoded.Version != h.Version {
		t.Errorf("Version = %d, want %d", decoded.Version, h.Version)
	}
	if decoded.Count != h.Count {
		t.Errorf("Count = %d, want %d", decoded.Count, h.Count)
	}
	if decoded.Width != h.Width {
		t.Errorf("Width = %d, want %d", decoded.Width, h.Width)
	}
}

func TestDecodeHeaderTooShort(t *testing.T) {
	_, err := DecodeHeader(make([]byte, HeaderSize-1))
	if err != ErrInvalidHeader {
		t.Errorf("DecodeHeader(short) = %v, want ErrInvalidHeader", err)
	}
}
