package extsort

import (
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// One sync.Pool per compression level. Zstd encoders allocate multi-MB
// window buffers; reusing them across run files cuts per-flush
// allocations dramatically.
var zstdEncoderPools = map[zstd.EncoderLevel]*sync.Pool{
	zstd.SpeedFastest:           {},
	zstd.SpeedDefault:           {},
	zstd.SpeedBetterCompression: {},
}

// Decoder instances are reused across run-file reads.
var zstdDecoderPool sync.Pool

func acquireZstdEncoder(level zstd.EncoderLevel) (*zstd.Encoder, error) {
	pool := zstdEncoderPools[level]
	if pool == nil {
		enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(level))
		if err != nil {
			return nil, fmt.Errorf("zstd new writer: %w", err)
		}
		return enc, nil
	}
	if v := pool.Get(); v != nil {
		enc, ok := v.(*zstd.Encoder)
		if ok {
			return enc, nil
		}
	}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(level))
	if err != nil {
		return nil, fmt.Errorf("zstd new writer: %w", err)
	}
	return enc, nil
}

func releaseZstdEncoder(level zstd.EncoderLevel, enc *zstd.Encoder) {
	if enc == nil {
		return
	}
	pool := zstdEncoderPools[level]
	if pool == nil {
		return
	}
	// Reset(nil) detaches the encoder from its current sink so the
	// next user can re-target it without re-allocating window buffers.
	enc.Reset(nil)
	pool.Put(enc)
}

func acquireZstdDecoder() (*zstd.Decoder, error) {
	if v := zstdDecoderPool.Get(); v != nil {
		dec, ok := v.(*zstd.Decoder)
		if ok {
			return dec, nil
		}
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("zstd new reader: %w", err)
	}
	return dec, nil
}

func releaseZstdDecoder(dec *zstd.Decoder) {
	if dec == nil {
		return
	}
	// Reset(nil) detaches from the current source.
	if err := dec.Reset(nil); err != nil {
		dec.Close()
		return
	}
	zstdDecoderPool.Put(dec)
}
