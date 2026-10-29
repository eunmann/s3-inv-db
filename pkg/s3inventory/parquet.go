package s3inventory

import (
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"unsafe"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding"
)

// Sentinel errors for parquet inventory reading. Static errors satisfy
// err113 by letting callers use errors.Is and fmt.Errorf("...: %w", err).
var (
	// ErrSizeMismatch indicates buffered bytes did not match the declared size.
	ErrSizeMismatch = errors.New("size mismatch")
	// ErrMissingKeyColumn indicates the parquet schema has no recognized key column.
	ErrMissingKeyColumn = errors.New("parquet schema missing 'key' column")
	// ErrMissingSizeColumn indicates the parquet schema has no recognized size column.
	ErrMissingSizeColumn = errors.New("parquet schema missing 'size' column")
	// ErrDuplicateColumn indicates two parquet columns canonicalize to the same name.
	ErrDuplicateColumn = errors.New("parquet schema has duplicate column")
	// ErrColumnOutOfRange indicates a configured column index beyond the schema.
	ErrColumnOutOfRange = errors.New("parquet column index out of range")
	// ErrColumnsMisaligned indicates the read columns disagree on a row group's row count.
	ErrColumnsMisaligned = errors.New("parquet columns hold different row counts")
	// ErrMalformedPage indicates a page whose values disagree with its
	// definition levels or dictionary.
	ErrMalformedPage = errors.New("malformed parquet page")
)

// ParquetReader reads S3 inventory records from Parquet files. It reads
// only the column chunks it needs (key, size, storage class, access
// tier, delete marker), a batch of rows at a time per column, so the
// other columns of an inventory (ETags, dates, owners, ACLs) are
// never decoded.
type ParquetReader struct {
	tempFile     *os.File
	file         *parquet.File
	rowGroups    []parquet.RowGroup
	pendingErr   error
	key          parquetColumn[string]
	size         parquetColumn[uint64]
	storage      parquetColumn[string]
	accessTier   parquetColumn[string]
	deleteMarker parquetColumn[bool]
	cols         [numParquetColumns]batchColumn
	currentRGIdx int
	anomalies    Anomalies
	rgOpen       bool
	bufIdx       int
	bufLen       int
}

// The columns a ParquetReader reads, in cols order.
const (
	pqKey = iota
	pqSize
	pqStorage
	pqAccessTier
	pqDeleteMarker
	numParquetColumns
)

// parquetBatchRows is how many rows each column decodes per batch.
const parquetBatchRows = 1024

// ParquetReaderConfig configures the Parquet reader.
type ParquetReaderConfig struct {
	// KeyCol is the column index for the object key (required).
	KeyCol int

	// SizeCol is the column index for the object size (required).
	SizeCol int

	// StorageCol is the column index for the storage class (-1 if not available).
	StorageCol int

	// AccessTierCol is the column index for the access tier (-1 if not available).
	AccessTierCol int

	// DeleteMarkerCol is the column index for is_delete_marker (-1 if not
	// available). Only all-versions inventories carry it.
	DeleteMarkerCol int
}

// NewParquetReader creates a Parquet inventory reader from an io.ReaderAt.
// This is used when you already have a ReaderAt (e.g., a local file or memory-mapped data).
func NewParquetReader(r io.ReaderAt, size int64, cfg ParquetReaderConfig) (*ParquetReader, error) {
	file, err := parquet.OpenFile(r, size)
	if err != nil {
		return nil, fmt.Errorf("open parquet file: %w", err)
	}

	return newParquetReader(file, nil, cfg)
}

// NewParquetReaderFromReaderAt creates a Parquet inventory reader from an io.ReaderAt.
// This auto-detects the schema from the Parquet file and is more efficient than
// NewParquetReaderFromStream when you already have a ReaderAt (e.g., from temp file).
func NewParquetReaderFromReaderAt(r io.ReaderAt, size int64) (*ParquetReader, error) {
	file, err := parquet.OpenFile(r, size)
	if err != nil {
		return nil, fmt.Errorf("open parquet file: %w", err)
	}

	cfg, err := detectParquetSchema(file.Schema())
	if err != nil {
		return nil, err
	}

	return newParquetReader(file, nil, cfg)
}

// NewParquetReaderFromStream creates a Parquet inventory reader from a stream.
// Since Parquet requires random access, this buffers the entire stream to a temp file
// in tempDir (os.TempDir when empty).
// The size parameter is used for validation (if non-zero) but the full stream is read regardless.
func NewParquetReaderFromStream(r io.ReadCloser, size int64, tempDir string) (*ParquetReader, error) {
	tempFile, err := os.CreateTemp(tempDir, "parquet-inventory-*.parquet")
	if err != nil {
		r.Close()

		return nil, fmt.Errorf("create temp file: %w", err)
	}

	written, err := io.Copy(tempFile, r)
	r.Close()
	if err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())

		return nil, fmt.Errorf("buffer parquet data: %w", err)
	}

	// Validate size if provided
	if size > 0 && written != size {
		tempFile.Close()
		os.Remove(tempFile.Name())

		return nil, fmt.Errorf("%w: expected %d, got %d", ErrSizeMismatch, size, written)
	}

	if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())

		return nil, fmt.Errorf("seek temp file: %w", err)
	}

	file, err := parquet.OpenFile(tempFile, written)
	if err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())

		return nil, fmt.Errorf("open parquet file: %w", err)
	}

	cfg, err := detectParquetSchema(file.Schema())
	if err == nil {
		var r *ParquetReader
		if r, err = newParquetReader(file, tempFile, cfg); err == nil {
			return r, nil
		}
	}
	tempFile.Close()
	os.Remove(tempFile.Name())

	return nil, err
}

// detectParquetSchema detects column indices from the Parquet schema.
// Column names are matched case-insensitively after stripping underscores
// so AWS's PascalCase ("Key", "StorageClass") and the snake_case form
// ("key", "storage_class") that some other producers emit both work.
//
// Without normalisation, a Parquet inventory whose schema differs in
// case from the four hardcoded strings would silently drop the
// StorageClass / IntelligentTieringAccessTier columns (KeyCol/SizeCol
// failure is a hard error; tier columns silently default to -1), so
// every tier bucket would collapse to "unknown" with no error surfaced.
func detectParquetSchema(schema *parquet.Schema) (ParquetReaderConfig, error) {
	cfg := ParquetReaderConfig{
		KeyCol:          -1,
		SizeCol:         -1,
		StorageCol:      -1,
		AccessTierCol:   -1,
		DeleteMarkerCol: -1,
	}

	seen := make(map[string]string, len(schema.Fields()))
	for i, field := range schema.Fields() {
		canon := canonicalColumnName(field.Name())
		if prev, ok := seen[canon]; ok {
			return cfg, fmt.Errorf("%w: %q and %q both canonicalize to %q",
				ErrDuplicateColumn, prev, field.Name(), canon)
		}
		seen[canon] = field.Name()

		switch canon {
		case "key":
			cfg.KeyCol = i
		case "size":
			cfg.SizeCol = i
		case "storageclass":
			cfg.StorageCol = i
		case "intelligenttieringaccesstier":
			cfg.AccessTierCol = i
		case "isdeletemarker":
			cfg.DeleteMarkerCol = i
		}
	}

	if cfg.KeyCol < 0 {
		return cfg, ErrMissingKeyColumn
	}
	if cfg.SizeCol < 0 {
		return cfg, ErrMissingSizeColumn
	}

	return cfg, nil
}

// canonicalColumnName normalises a manifest/parquet column name to
// lowercase with underscores stripped. The CSV reader already does
// the same on its header row (pkg/inventory/reader.go); this keeps
// the Parquet path consistent so both formats accept any reasonable
// naming convention a producer publishes.
func canonicalColumnName(name string) string {
	return strings.ReplaceAll(strings.ToLower(strings.TrimSpace(name)), "_", "")
}

// newParquetReader creates a ParquetReader from an open file.
func newParquetReader(file *parquet.File, tempFile *os.File, cfg ParquetReaderConfig) (*ParquetReader, error) {
	r := &ParquetReader{
		file:         file,
		tempFile:     tempFile,
		rowGroups:    file.RowGroups(),
		currentRGIdx: -1,
	}
	r.key.init(plainStrings, valueString)
	r.size.init(plainUint64s, parquet.Value.Uint64)
	r.storage.init(plainStrings, valueString)
	r.accessTier.init(plainStrings, valueString)
	r.deleteMarker.init(plainBools, valueBool)
	r.cols = [numParquetColumns]batchColumn{&r.key, &r.size, &r.storage, &r.accessTier, &r.deleteMarker}

	schema := file.Schema()
	leaves := schema.Columns()
	indexes := [numParquetColumns]int{cfg.KeyCol, cfg.SizeCol, cfg.StorageCol, cfg.AccessTierCol, cfg.DeleteMarkerCol}
	for i, idx := range indexes {
		if idx >= len(leaves) {
			return nil, fmt.Errorf("%w: column %d of %d", ErrColumnOutOfRange, idx, len(leaves))
		}
		var leaf parquet.LeafColumn
		if idx >= 0 {
			leaf, _ = schema.Lookup(leaves[idx]...)
		}
		r.cols[i].setLeaf(idx, leaf)
	}

	return r, nil
}

// Next returns the next inventory row.
func (r *ParquetReader) Next() (Row, error) {
	for {
		if r.bufIdx < r.bufLen {
			i := r.bufIdx
			r.bufIdx++
			inv, deleteMarker, hasSize := r.rowAt(i)
			if deleteMarker {
				continue // a delete marker is not an object and stores no bytes
			}
			if inv.Key == "" {
				r.anomalies.MissingKeys++

				continue
			}
			if !hasSize {
				r.anomalies.BadSizes++
			}

			return inv, nil
		}
		if r.pendingErr != nil {
			err := r.pendingErr
			r.pendingErr = nil

			return Row{}, err
		}
		if !r.rgOpen {
			r.currentRGIdx++
			if r.currentRGIdx >= len(r.rowGroups) {
				return Row{}, io.EOF
			}
			r.openRowGroup(r.rowGroups[r.currentRGIdx])
		}

		n, err := r.readBatch()
		if n > 0 {
			r.bufIdx, r.bufLen = 0, n
			// Surface a read error after the rows read before it.
			if err != nil && !errors.Is(err, io.EOF) {
				r.pendingErr = err
			}

			continue
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return Row{}, err
		}
		if err := r.closeRowGroup(); err != nil {
			return Row{}, err
		}
	}
}

// openRowGroup starts reading the needed column chunks of rg.
func (r *ParquetReader) openRowGroup(rg parquet.RowGroup) {
	chunks := rg.ColumnChunks()
	for _, c := range r.cols {
		if leaf := c.leafIndex(); leaf >= 0 {
			c.open(chunks[leaf])
		}
	}
	r.rgOpen = true
}

// closeRowGroup releases the current row group's pages and page readers.
func (r *ParquetReader) closeRowGroup() error {
	if !r.rgOpen {
		return nil
	}
	var errs []error
	for _, c := range r.cols {
		if err := c.close(); err != nil {
			errs = append(errs, err)
		}
	}
	r.rgOpen = false

	return errors.Join(errs...)
}

// readBatch decodes the next batch of every read column and returns the
// row count, which every column must agree on. Every row of the previous
// batch has been returned, so the pages only it pointed into are
// released first.
func (r *ParquetReader) readBatch() (int, error) {
	rows, rowsLeaf := -1, -1
	var readErr error
	for _, c := range r.cols {
		leaf := c.leafIndex()
		if leaf < 0 {
			continue
		}
		c.releaseConsumed()
		n, err := c.fill()
		if err != nil && !errors.Is(err, io.EOF) {
			readErr = err
		}
		if rows >= 0 && n != rows {
			return 0, fmt.Errorf("%w: column %d has %d, column %d has %d",
				ErrColumnsMisaligned, rowsLeaf, rows, leaf, n)
		}
		rows, rowsLeaf = n, leaf
	}
	if readErr != nil {
		return rows, readErr
	}
	if rows == 0 {
		return 0, io.EOF
	}

	return rows, nil
}

// batchColumn is a parquetColumn of any value type.
type batchColumn interface {
	setLeaf(index int, leaf parquet.LeafColumn)
	leafIndex() int
	open(chunk parquet.ColumnChunk)
	releaseConsumed()
	fill() (int, error)
	close() error
}

// parquetColumn decodes one leaf column of the current row group a page
// at a time into a batch of per-row values. A plain page's values are
// decoded once per page and a dictionary's once per dictionary, so a row
// costs a copy or an index load. Byte arrays become strings pointing
// into their page or dictionary, so the pages a batch read from are held
// until the next batch.
type parquetColumn[T any] struct {
	pages parquet.Pages
	// held are the pages the batch may point into, oldest first; the
	// last is the page being read.
	held []parquet.Page

	// The page being read. Row i has a value when defs is nil or
	// defs[i] == present; the values are values, or dictValues looked
	// up by indexes when the page is dictionary-encoded.
	defs    []byte
	indexes []int32
	values  []T
	row     int // next row
	rows    int
	next    int // next value
	present byte

	dict       parquet.Dictionary
	dictValues []T

	out []T
	has []bool

	// decodePlain decodes a plain page's n values into dst, and reports
	// false for a physical type it has no fast path for.
	decodePlain func(data *encoding.Values, n int, dst []T) ([]T, bool)
	// convert turns one non-null value into T, for dictionary entries
	// and the pages decodePlain does not handle.
	convert func(parquet.Value) T

	valueBuf []T
	levelBuf []byte
	scratch  []parquet.Value

	index  int // leaf column index in the file; -1 when absent
	maxDef byte
	// byValue reads one row per value, as ReadValues gives them, for a
	// repeated column, whose rows span values.
	byValue bool
}

func (c *parquetColumn[T]) init(
	decodePlain func(*encoding.Values, int, []T) ([]T, bool),
	convert func(parquet.Value) T,
) {
	c.decodePlain, c.convert = decodePlain, convert
	c.out = make([]T, parquetBatchRows)
	c.has = make([]bool, parquetBatchRows)
}

func (c *parquetColumn[T]) setLeaf(index int, leaf parquet.LeafColumn) {
	c.index = index
	c.maxDef = byte(leaf.MaxDefinitionLevel)
	c.byValue = leaf.MaxRepetitionLevel > 0
}

func (c *parquetColumn[T]) leafIndex() int { return c.index }

func (c *parquetColumn[T]) open(chunk parquet.ColumnChunk) {
	c.pages = chunk.Pages()
	c.row, c.rows = 0, 0
}

// releaseConsumed releases the held pages no later row reads: all of
// them but the page being read, if it has rows left.
func (c *parquetColumn[T]) releaseConsumed() {
	keep := 0
	if c.row < c.rows {
		keep = 1
	}
	n := len(c.held) - keep
	for _, p := range c.held[:n] {
		parquet.Release(p)
	}
	copy(c.held, c.held[n:])
	clear(c.held[keep:])
	c.held = c.held[:keep]
}

func (c *parquetColumn[T]) close() error {
	c.row, c.rows = 0, 0
	c.releaseConsumed()
	c.defs, c.indexes, c.values, c.dict = nil, nil, nil, nil
	if c.pages == nil {
		return nil
	}
	err := c.pages.Close()
	c.pages = nil
	if err != nil {
		return fmt.Errorf("close parquet pages: %w", err)
	}

	return nil
}

// fill decodes the column's next rows into out, across pages, and
// returns how many it decoded; io.EOF ends the row group.
func (c *parquetColumn[T]) fill() (int, error) {
	n := 0
	for n < len(c.out) {
		if c.row == c.rows {
			if err := c.readPage(); err != nil {
				return n, err
			}

			continue
		}
		m := min(len(c.out)-n, c.rows-c.row)
		out, has := c.out[n:n+m], c.has[n:n+m]
		if c.defs == nil {
			c.fillDense(out, has)
		} else {
			c.fillSparse(out, has, c.defs[c.row:c.row+m])
		}
		c.row += m
		n += m
	}

	return n, nil
}

// fillDense fills rows that all have a value.
func (c *parquetColumn[T]) fillDense(out []T, has []bool) {
	if c.indexes != nil {
		for i, ix := range c.indexes[c.next : c.next+len(out)] {
			out[i] = c.dictValues[ix]
		}
	} else {
		copy(out, c.values[c.next:c.next+len(out)])
	}
	for i := range has {
		has[i] = true
	}
	c.next += len(out)
}

// fillSparse fills rows some of which are null.
func (c *parquetColumn[T]) fillSparse(out []T, has []bool, defs []byte) {
	var zero T
	next := c.next
	for i, d := range defs {
		if d != c.present {
			out[i], has[i] = zero, false

			continue
		}
		if c.indexes != nil {
			out[i] = c.dictValues[c.indexes[next]]
		} else {
			out[i] = c.values[next]
		}
		has[i] = true
		next++
	}
	c.next = next
}

// readPage makes the column chunk's next page the one being read.
func (c *parquetColumn[T]) readPage() error {
	page, err := c.pages.ReadPage()
	if errors.Is(err, io.EOF) {
		return io.EOF
	}
	if err != nil {
		return fmt.Errorf("read parquet page: %w", err)
	}
	c.held = append(c.held, page)
	c.row, c.rows, c.next = 0, 0, 0
	if c.byValue {
		return c.decodeValues(page)
	}

	return c.decodePage(page)
}

// decodePage decodes page's levels and its values or dictionary indexes.
func (c *parquetColumn[T]) decodePage(page parquet.Page) error {
	rows := int(page.NumValues())
	n := rows
	c.defs, c.present = nil, c.maxDef
	if c.maxDef > 0 {
		defs := page.DefinitionLevels()
		if len(defs) != rows {
			return fmt.Errorf("%w: column %d page has %d values and %d definition levels",
				ErrMalformedPage, c.index, rows, len(defs))
		}
		n = 0
		for _, d := range defs {
			if d == c.maxDef {
				n++
			}
		}
		if n < rows {
			c.defs = defs
		}
	}

	data := page.Data()
	if dict := page.Dictionary(); dict != nil && data.Kind() == encoding.Int32 {
		c.resolve(dict)
		indexes := data.Int32()
		if len(indexes) != n {
			return fmt.Errorf("%w: column %d page has %d dictionary indexes for %d values",
				ErrMalformedPage, c.index, len(indexes), n)
		}
		for _, ix := range indexes {
			if uint(ix) >= uint(len(c.dictValues)) {
				return fmt.Errorf("%w: column %d dictionary index %d of %d",
					ErrMalformedPage, c.index, ix, len(c.dictValues))
			}
		}
		c.indexes, c.values, c.rows = indexes, nil, rows

		return nil
	}
	values, ok := c.decodePlain(&data, n, c.valueBuf[:0])
	if !ok {
		return c.decodeValues(page)
	}
	c.valueBuf = values
	if len(values) != n {
		return fmt.Errorf("%w: column %d page has %d values for %d non-null rows",
			ErrMalformedPage, c.index, len(values), n)
	}
	c.indexes, c.values, c.rows = nil, values, rows

	return nil
}

// resolve converts each entry of dict once, when the first page of the
// column chunk that uses it is read.
func (c *parquetColumn[T]) resolve(dict parquet.Dictionary) {
	if dict == c.dict {
		return
	}
	n := dict.Len()
	c.dictValues = slices.Grow(c.dictValues[:0], n)[:n]
	for i := range n {
		c.dictValues[i] = c.convert(dict.Index(int32(i)))
	}
	c.dict = dict
}

// decodeValues decodes page one parquet.Value at a time, a row per
// value, for the pages decodePage has no fast path for.
func (c *parquetColumn[T]) decodeValues(page parquet.Page) error {
	n := int(page.NumValues())
	c.scratch = slices.Grow(c.scratch[:0], n)[:n]
	read, err := readValues(page.Values(), c.scratch)
	if err != nil {
		return err
	}
	values, defs := c.valueBuf[:0], c.levelBuf[:0]
	for _, v := range c.scratch[:read] {
		if v.IsNull() {
			defs = append(defs, 0)

			continue
		}
		defs = append(defs, 1)
		values = append(values, c.convert(v))
	}
	c.valueBuf, c.levelBuf = values, defs
	c.defs, c.present, c.indexes, c.values, c.rows = defs, 1, nil, values, read

	return nil
}

// readValues reads r's values into dst until dst is full or r ends.
func readValues(r parquet.ValueReader, dst []parquet.Value) (int, error) {
	n := 0
	for n < len(dst) {
		k, err := r.ReadValues(dst[n:])
		n += k
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return n, fmt.Errorf("read parquet values: %w", err)
		}
		if k == 0 {
			return n, fmt.Errorf("read parquet values: %w", io.ErrNoProgress)
		}
	}

	return n, nil
}

// plainStrings decodes a byte array page into strings pointing into it.
func plainStrings(data *encoding.Values, _ int, dst []string) ([]string, bool) {
	if data.Kind() != encoding.ByteArray {
		return dst, false
	}
	b, offsets := data.ByteArray()
	for i := 1; i < len(offsets); i++ {
		s := ""
		if v := b[offsets[i-1]:offsets[i]]; len(v) > 0 {
			s = unsafe.String(&v[0], len(v))
		}
		dst = append(dst, s)
	}

	return dst, true
}

// plainUint64s decodes an INT64 page as parquet.Value.Uint64 would.
func plainUint64s(data *encoding.Values, _ int, dst []uint64) ([]uint64, bool) {
	if data.Kind() != encoding.Int64 {
		return dst, false
	}

	return append(dst, data.Uint64()...), true
}

// plainBools decodes the first n bits of a BOOLEAN page.
func plainBools(data *encoding.Values, n int, dst []bool) ([]bool, bool) {
	if data.Kind() != encoding.Boolean {
		return dst, false
	}
	const bitsPerByte = 8
	bits := data.Boolean()
	for i := range min(n, len(bits)*bitsPerByte) {
		dst = append(dst, bits[i/bitsPerByte]>>(i%bitsPerByte)&1 != 0)
	}

	return dst, true
}

// rowAt assembles batch row i into an inventory row and reports
// whether the row is a delete marker and whether it has a size. A column
// the file lacks reads as its zero value.
func (r *ParquetReader) rowAt(i int) (Row, bool, bool) {
	return Row{
		Key:          r.key.out[i],
		Size:         r.size.out[i],
		StorageClass: r.storage.out[i],
		AccessTier:   r.accessTier.out[i],
	}, r.deleteMarker.out[i], r.size.has[i]
}

// valueString returns v as a string. A byte array points into its page
// rather than being copied: a Row's strings are valid only until the
// next call, and the page outlives that.
func valueString(v parquet.Value) string {
	if k := v.Kind(); k == parquet.ByteArray || k == parquet.FixedLenByteArray {
		b := v.ByteArray()

		return unsafe.String(unsafe.SliceData(b), len(b))
	}

	return v.String()
}

// valueBool reads a delete marker: a BOOLEAN, or the strings isTrue
// accepts.
func valueBool(v parquet.Value) bool {
	if v.Kind() == parquet.Boolean {
		return v.Boolean()
	}

	return isTrue(valueString(v))
}

// Anomalies counts the rows read so far that could not be read exactly.
func (r *ParquetReader) Anomalies() Anomalies { return r.anomalies }

// Close releases resources.
func (r *ParquetReader) Close() error {
	var errs []error
	if err := r.closeRowGroup(); err != nil {
		errs = append(errs, err)
	}

	if r.tempFile != nil {
		name := r.tempFile.Name()
		if err := r.tempFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close temp file: %w", err))
		}
		if err := os.Remove(name); err != nil {
			errs = append(errs, fmt.Errorf("remove temp file: %w", err))
		}
	}

	return errors.Join(errs...)
}
