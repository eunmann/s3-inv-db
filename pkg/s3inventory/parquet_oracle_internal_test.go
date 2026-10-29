package s3inventory

import (
	"errors"
	"fmt"
	"io"

	"github.com/parquet-go/parquet-go"
)

// oracleParquetReader is the generic-Value Parquet reader the page-level
// ParquetReader replaced: every column decodes into []parquet.Value and
// every row converts its values one by one. Tests hold the ParquetReader
// to its rows and anomalies.
type oracleParquetReader struct {
	rowGroups []parquet.RowGroup
	cols      [numParquetColumns]oracleColumn
	anomalies Anomalies
	rg        int
	rgOpen    bool
	bufIdx    int
	bufLen    int
}

type oracleColumn struct {
	pages  parquet.Pages
	values parquet.ValueReader
	buf    []parquet.Value
	index  int
}

func newOracleParquetReader(r io.ReaderAt, size int64) (*oracleParquetReader, error) {
	file, err := parquet.OpenFile(r, size)
	if err != nil {
		return nil, fmt.Errorf("open parquet file: %w", err)
	}
	cfg, err := detectParquetSchema(file.Schema())
	if err != nil {
		return nil, err
	}
	o := &oracleParquetReader{rowGroups: file.RowGroups(), rg: -1}
	indexes := [numParquetColumns]int{cfg.KeyCol, cfg.SizeCol, cfg.StorageCol, cfg.AccessTierCol, cfg.DeleteMarkerCol}
	for i, idx := range indexes {
		o.cols[i].index = idx
		if idx >= 0 {
			o.cols[i].buf = make([]parquet.Value, parquetBatchRows)
		}
	}

	return o, nil
}

func (o *oracleParquetReader) Next() (Row, error) {
	for {
		if o.bufIdx < o.bufLen {
			i := o.bufIdx
			o.bufIdx++
			inv, deleteMarker, hasSize := o.rowAt(i)
			if deleteMarker {
				continue
			}
			if inv.Key == "" {
				o.anomalies.MissingKeys++

				continue
			}
			if !hasSize {
				o.anomalies.BadSizes++
			}

			return inv, nil
		}
		if !o.rgOpen {
			o.rg++
			if o.rg >= len(o.rowGroups) {
				return Row{}, io.EOF
			}
			chunks := o.rowGroups[o.rg].ColumnChunks()
			for i := range o.cols {
				if c := &o.cols[i]; c.index >= 0 {
					c.pages = chunks[c.index].Pages()
				}
			}
			o.rgOpen = true
		}
		n, err := o.readBatch()
		if err != nil && !errors.Is(err, io.EOF) {
			return Row{}, err
		}
		if n > 0 {
			o.bufIdx, o.bufLen = 0, n

			continue
		}
		for i := range o.cols {
			if c := &o.cols[i]; c.pages != nil {
				if err := c.pages.Close(); err != nil {
					return Row{}, fmt.Errorf("close parquet pages: %w", err)
				}
				c.pages, c.values = nil, nil
			}
		}
		o.rgOpen = false
	}
}

func (o *oracleParquetReader) readBatch() (int, error) {
	rows := -1
	for i := range o.cols {
		c := &o.cols[i]
		if c.index < 0 {
			continue
		}
		n, err := c.read()
		if err != nil && !errors.Is(err, io.EOF) {
			return 0, err
		}
		if rows >= 0 && n != rows {
			return 0, ErrColumnsMisaligned
		}
		rows = n
	}
	if rows <= 0 {
		return 0, io.EOF
	}

	return rows, nil
}

func (c *oracleColumn) read() (int, error) {
	n := 0
	for n < len(c.buf) {
		if c.values == nil {
			page, err := c.pages.ReadPage()
			if errors.Is(err, io.EOF) {
				return n, io.EOF
			}
			if err != nil {
				return n, fmt.Errorf("read parquet page: %w", err)
			}
			c.values = page.Values()
		}
		k, err := c.values.ReadValues(c.buf[n:])
		n += k
		if errors.Is(err, io.EOF) {
			c.values = nil

			continue
		}
		if err != nil {
			return n, fmt.Errorf("read parquet values: %w", err)
		}
	}

	return n, nil
}

func (o *oracleParquetReader) rowAt(i int) (Row, bool, bool) {
	var inv Row
	if v, ok := o.value(pqKey, i); ok {
		inv.Key = v.String()
	}
	v, hasSize := o.value(pqSize, i)
	if hasSize {
		inv.Size = v.Uint64()
	}
	if v, ok := o.value(pqStorage, i); ok {
		inv.StorageClass = v.String()
	}
	if v, ok := o.value(pqAccessTier, i); ok {
		inv.AccessTier = v.String()
	}
	deleteMarker := false
	if v, ok := o.value(pqDeleteMarker, i); ok {
		if v.Kind() == parquet.Boolean {
			deleteMarker = v.Boolean()
		} else {
			deleteMarker = isTrue(v.String())
		}
	}

	return inv, deleteMarker, hasSize
}

func (o *oracleParquetReader) value(col, i int) (parquet.Value, bool) {
	c := &o.cols[col]
	if c.index < 0 || c.buf[i].IsNull() {
		return parquet.Value{}, false
	}

	return c.buf[i], true
}
