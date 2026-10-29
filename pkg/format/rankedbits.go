package format

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"os"
	"path/filepath"
)

// A ranked bit vector file: Header (Count = bits, Width = 8), the bits
// as little-endian uint64 words, then a rank directory holding, for
// every rankBlockWords words, the set bits before them. rank(i) is one
// directory entry plus at most rankBlockWords popcounts.

// rankedBitsWriter streams a ranked bit vector one bit at a time. Its
// memory stays constant: the words and the directory each stream to
// their own file, and Close appends the directory to the bits.
type rankedBitsWriter struct {
	path    string
	f       *hashedFile
	bw      *bufio.Writer
	dirPath string
	dirF    *os.File
	dirBW   *bufio.Writer
	word    uint64
	n       uint64
	set     uint64
}

func newRankedBitsWriter(path string, sums *Checksums) (*rankedBitsWriter, error) {
	f, err := createHashedFile(path, sums)
	if err != nil {
		return nil, err
	}
	dirPath := path + ".dir"
	dirF, err := os.Create(dirPath)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("create %s: %w", filepath.Base(dirPath), err), f.Close(), removeIfErr(path))
	}
	w := &rankedBitsWriter{
		path: path, f: f, bw: bufio.NewWriterSize(f, writeBufSize),
		dirPath: dirPath, dirF: dirF, dirBW: bufio.NewWriterSize(dirF, writeBufSize),
	}
	// The bits start after the header, which Close writes with the bit
	// count.
	if err := f.seek(HeaderSize); err != nil {
		return nil, errors.Join(fmt.Errorf("skip %s header: %w", filepath.Base(path), err), w.abort())
	}

	return w, nil
}

// add appends one bit.
func (w *rankedBitsWriter) add(bit bool) error {
	i := w.n % wordBits
	if i == 0 && (w.n/wordBits)%rankBlockWords == 0 {
		if err := putU64(w.dirBW, w.set); err != nil {
			return fmt.Errorf("write rank directory: %w", err)
		}
	}
	if bit {
		w.word |= 1 << i
		w.set++
	}
	w.n++
	if w.n%wordBits == 0 {
		if err := putU64(w.bw, w.word); err != nil {
			return fmt.Errorf("write %s: %w", filepath.Base(w.path), err)
		}
		w.word = 0
	}

	return nil
}

// addBits appends the first n bits of src, bit i at bit i%64 of word
// i/64; the bits of src past n are ignored.
func (w *rankedBitsWriter) addBits(src []uint64, n uint64) error {
	for k := uint64(0); k < n; {
		i := w.n % wordBits
		if i == 0 && (w.n/wordBits)%rankBlockWords == 0 {
			if err := putU64(w.dirBW, w.set); err != nil {
				return fmt.Errorf("write rank directory: %w", err)
			}
		}
		take := min(wordBits-i, n-k)
		v := bitsAt(src, k, take)
		w.word |= v << i
		w.set += uint64(bits.OnesCount64(v))
		w.n += take
		k += take
		if w.n%wordBits == 0 {
			if err := putU64(w.bw, w.word); err != nil {
				return fmt.Errorf("write %s: %w", filepath.Base(w.path), err)
			}
			w.word = 0
		}
	}

	return nil
}

// bitsAt returns the n bits (1..64) of src from bit k, as addBits numbers
// them, in the low bits.
func bitsAt(src []uint64, k, n uint64) uint64 {
	word, shift := k/wordBits, k%wordBits
	v := src[word] >> shift
	if shift+n > wordBits {
		v |= src[word+1] << (wordBits - shift)
	}
	if n < wordBits {
		v &= 1<<n - 1
	}

	return v
}

// setBit sets bit i of the bits a range writer collects for addBits,
// growing them to hold it.
func setBit(words []uint64, i uint64, bit bool) []uint64 {
	for uint64(len(words)) <= i/wordBits {
		words = append(words, 0)
	}
	if bit {
		words[i/wordBits] |= 1 << (i % wordBits)
	}

	return words
}

func (w *rankedBitsWriter) close() error {
	err := w.finish()
	if err != nil {
		return errors.Join(err, w.abort())
	}

	return removeIfErr(w.dirPath)
}

func (w *rankedBitsWriter) finish() error {
	if w.n%wordBits != 0 {
		if err := putU64(w.bw, w.word); err != nil {
			return fmt.Errorf("write %s: %w", filepath.Base(w.path), err)
		}
	}
	if err := w.dirBW.Flush(); err != nil {
		return fmt.Errorf("flush rank directory: %w", err)
	}
	if _, err := w.dirF.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind rank directory: %w", err)
	}
	if _, err := io.Copy(w.bw, w.dirF); err != nil {
		return fmt.Errorf("append rank directory: %w", err)
	}
	if err := w.bw.Flush(); err != nil {
		return fmt.Errorf("flush %s: %w", filepath.Base(w.path), err)
	}
	head := EncodeHeader(Header{Magic: MagicNumber, Version: Version, Count: w.n, Width: 8})
	if _, err := w.f.WriteAt(head, 0); err != nil {
		return fmt.Errorf("write %s header: %w", filepath.Base(w.path), err)
	}
	w.f.startWriteback()

	return errors.Join(w.dirF.Close(), w.f.Close())
}

func (w *rankedBitsWriter) abort() error {
	return errors.Join(w.dirF.Close(), w.f.Close(), removeIfErr(w.dirPath), removeIfErr(w.path))
}

// rankedBitsReader reads a ranked bit vector file.
type rankedBitsReader struct {
	mmap  *MmapFile
	n     uint64
	words uint64
}

func openRankedBits(path string) (*rankedBitsReader, error) {
	name := filepath.Base(path)
	mmap, err := OpenMmap(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", name, err)
	}
	data := mmap.Data()
	fail := func(err error) (*rankedBitsReader, error) {
		mmap.Close()

		return nil, fmt.Errorf("%s: %w", name, err)
	}
	if len(data) < HeaderSize {
		return fail(ErrInvalidHeader)
	}
	header, err := DecodeHeader(data[:HeaderSize])
	if err != nil {
		return fail(fmt.Errorf("decode header: %w", err))
	}
	if header.Magic != MagicNumber {
		return fail(ErrMagicMismatch)
	}
	if header.Version != Version {
		return fail(ErrVersionMismatch)
	}
	words := bitWords(header.Count)
	want := uint64(HeaderSize) + 8*words + 8*((words+rankBlockWords-1)/rankBlockWords)
	if uint64(len(data)) < want {
		return fail(fmt.Errorf("%w: %d < %d", ErrFileTooSmall, len(data), want))
	}

	return &rankedBitsReader{mmap: mmap, n: header.Count, words: words}, nil
}

func (r *rankedBitsReader) word(i uint64) uint64 {
	return binary.LittleEndian.Uint64(r.mmap.Data()[HeaderSize+8*i:])
}

// get reports bit i. No bounds check.
func (r *rankedBitsReader) get(i uint64) bool {
	return r.word(i/wordBits)&(1<<(i%wordBits)) != 0
}

// rank returns the set bits before bit i, for i in [0, n].
func (r *rankedBitsReader) rank(i uint64) uint64 {
	if r.words == 0 {
		return 0
	}
	w := i / wordBits
	// i == n on a block boundary is past the last directory entry;
	// count on from the last block instead.
	block := min(w/rankBlockWords, (r.words-1)/rankBlockWords)
	count := binary.LittleEndian.Uint64(r.mmap.Data()[HeaderSize+8*(r.words+block):])
	for j := block * rankBlockWords; j < w; j++ {
		count += uint64(bits.OnesCount64(r.word(j)))
	}
	if bit := i % wordBits; bit != 0 {
		count += uint64(bits.OnesCount64(r.word(w) & (1<<bit - 1)))
	}

	return count
}

func (r *rankedBitsReader) close() error {
	if r == nil {
		return nil
	}

	return r.mmap.Close()
}
