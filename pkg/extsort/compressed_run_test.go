package extsort_test

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// readAllRows reads every remaining row of reader, copying each.
func readAllRows(t *testing.T, reader *extsort.CompressedRunReader) []extsort.PrefixRow {
	t.Helper()
	var rows []extsort.PrefixRow
	for {
		var row extsort.PrefixRow
		err := reader.ReadInto(&row)
		if errors.Is(err, io.EOF) {
			return rows
		}
		if err != nil {
			t.Fatalf("read row %d: %v", len(rows), err)
		}
		rows = append(rows, row)
	}
}

func TestCompressedRunFile(t *testing.T) {
	t.Run("write and read", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "test.crun")

		// Create test rows
		rows := []*extsort.PrefixRow{
			{Prefix: "", Depth: 0, Count: 10, TotalBytes: 1000},
			{Prefix: "data/", Depth: 1, Count: 5, TotalBytes: 500},
			{Prefix: "data/2024/", Depth: 2, Count: 3, TotalBytes: 300},
		}
		rows[0].SetTier(tiers.Standard, 7, 700)
		rows[0].SetTier(tiers.GlacierFR, 3, 300)

		// Write compressed
		writer, err := extsort.NewCompressedRunWriter(path, extsort.CompressedRunWriterOptions{})
		if err != nil {
			t.Fatalf("create writer: %v", err)
		}
		for _, row := range rows {
			if err := writer.Write(row); err != nil {
				t.Fatalf("write row: %v", err)
			}
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("close writer: %v", err)
		}

		// Read compressed
		reader, err := extsort.OpenCompressedRunFile(path, 0)
		if err != nil {
			t.Fatalf("open reader: %v", err)
		}
		defer reader.Close()

		if reader.Count() != 3 {
			t.Errorf("expected count 3, got %d", reader.Count())
		}

		got := readAllRows(t, reader)
		if len(got) != len(rows) {
			t.Fatalf("read %d rows, want %d", len(got), len(rows))
		}
		for i, row := range got {
			if row.Prefix != rows[i].Prefix {
				t.Errorf("row %d: expected prefix %q, got %q", i, rows[i].Prefix, row.Prefix)
			}
			if row.Count != rows[i].Count {
				t.Errorf("row %d: expected count %d, got %d", i, rows[i].Count, row.Count)
			}
			if row.TotalBytes != rows[i].TotalBytes {
				t.Errorf("row %d: expected bytes %d, got %d", i, rows[i].TotalBytes, row.TotalBytes)
			}
			if row.Depth != rows[i].Depth {
				t.Errorf("row %d: expected depth %d, got %d", i, rows[i].Depth, row.Depth)
			}
		}

		if got[0].TierCount(tiers.Standard) != 7 {
			t.Errorf("expected 7 Standard tier count, got %d", got[0].TierCount(tiers.Standard))
		}
		if got[0].TierBytes(tiers.GlacierFR) != 300 {
			t.Errorf("expected 300 GlacierFR bytes, got %d", got[0].TierBytes(tiers.GlacierFR))
		}

		var row extsort.PrefixRow
		if err := reader.ReadInto(&row); !errors.Is(err, io.EOF) {
			t.Errorf("expected EOF after the last row, got %v", err)
		}
	})

	t.Run("sorted write", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "test.crun")

		// Create unsorted rows
		rows := []*extsort.PrefixRow{
			{Prefix: "z/", Depth: 1, Count: 1, TotalBytes: 100},
			{Prefix: "a/", Depth: 1, Count: 2, TotalBytes: 200},
			{Prefix: "m/", Depth: 1, Count: 3, TotalBytes: 300},
		}

		writer, err := extsort.NewCompressedRunWriter(path, extsort.CompressedRunWriterOptions{})
		if err != nil {
			t.Fatalf("create writer: %v", err)
		}
		if err := writer.WriteSorted(rows); err != nil {
			t.Fatalf("write sorted: %v", err)
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("close writer: %v", err)
		}

		// Read back and verify sorted order
		reader, err := extsort.OpenCompressedRunFile(path, 0)
		if err != nil {
			t.Fatalf("open reader: %v", err)
		}
		defer reader.Close()

		expectedOrder := []string{"a/", "m/", "z/"}
		got := readAllRows(t, reader)
		if len(got) != len(expectedOrder) {
			t.Fatalf("read %d rows, want %d", len(got), len(expectedOrder))
		}
		for i, row := range got {
			if row.Prefix != expectedOrder[i] {
				t.Errorf("row %d: expected prefix %q, got %q", i, expectedOrder[i], row.Prefix)
			}
		}
	})

	t.Run("large file with many rows", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "large.crun")

		const numRows = 10000

		// Write many rows
		writer, err := extsort.NewCompressedRunWriter(path, extsort.CompressedRunWriterOptions{})
		if err != nil {
			t.Fatalf("create writer: %v", err)
		}
		for i := range numRows {
			row := &extsort.PrefixRow{
				Prefix:     fmt.Sprintf("prefix/%05d/", i),
				Depth:      2,
				Count:      uint64(i + 1),
				TotalBytes: uint64((i + 1) * 100),
			}
			if err := writer.Write(row); err != nil {
				t.Fatalf("write row %d: %v", i, err)
			}
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("close writer: %v", err)
		}

		// Verify count
		reader, err := extsort.OpenCompressedRunFile(path, 0)
		if err != nil {
			t.Fatalf("open reader: %v", err)
		}
		defer reader.Close()

		if reader.Count() != numRows {
			t.Errorf("expected count %d, got %d", numRows, reader.Count())
		}

		got := readAllRows(t, reader)
		if len(got) != numRows {
			t.Fatalf("read %d rows, want %d", len(got), numRows)
		}
		for i, row := range got {
			expectedPrefix := fmt.Sprintf("prefix/%05d/", i)
			if row.Prefix != expectedPrefix {
				t.Errorf("row %d: expected prefix %q, got %q", i, expectedPrefix, row.Prefix)
			}
			if row.Count != uint64(i+1) {
				t.Errorf("row %d: expected count %d, got %d", i, i+1, row.Count)
			}
		}
	})

	t.Run("compression levels", func(t *testing.T) {
		tmpDir := t.TempDir()

		levels := []extsort.CompressionLevel{
			extsort.CompressionFastest,
			extsort.CompressionDefault,
			extsort.CompressionBetter,
		}

		for _, level := range levels {
			path := filepath.Join(tmpDir, fmt.Sprintf("level%d.crun", level))

			writer, err := extsort.NewCompressedRunWriter(path, extsort.CompressedRunWriterOptions{
				CompressionLevel: level,
			})
			if err != nil {
				t.Fatalf("level %d: create writer: %v", level, err)
			}
			for i := range 100 {
				row := &extsort.PrefixRow{
					Prefix:     fmt.Sprintf("data/%d/", i),
					Depth:      2,
					Count:      uint64(i),
					TotalBytes: uint64(i * 100),
				}
				if err := writer.Write(row); err != nil {
					t.Fatalf("level %d: write: %v", level, err)
				}
			}
			if err := writer.Close(); err != nil {
				t.Fatalf("level %d: close: %v", level, err)
			}

			// Verify readable
			reader, err := extsort.OpenCompressedRunFile(path, 0)
			if err != nil {
				t.Fatalf("level %d: open: %v", level, err)
			}
			if reader.Count() != 100 {
				t.Errorf("level %d: expected count 100, got %d", level, reader.Count())
			}
			reader.Close()
		}
	})

	t.Run("remove file", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "test.crun")

		writer, err := extsort.NewCompressedRunWriter(path, extsort.CompressedRunWriterOptions{})
		if err != nil {
			t.Fatalf("create writer: %v", err)
		}
		writer.Write(&extsort.PrefixRow{Prefix: "test/"})
		writer.Close()

		reader, err := extsort.OpenCompressedRunFile(path, 0)
		if err != nil {
			t.Fatalf("open reader: %v", err)
		}

		if err := reader.Remove(); err != nil {
			t.Fatalf("remove: %v", err)
		}

		// Verify file is gone
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Error("expected file to be removed")
		}
	})
}

// TestCompressedRunSize checks that the run body compresses at least 2x
// against its raw record encoding.
func TestCompressedRunSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "compressed.crun")

	const numRows = 5000

	// Realistic prefix data (highly compressible).
	rows := make([]*extsort.PrefixRow, numRows)
	var rawSize int64
	for i := range numRows {
		rows[i] = &extsort.PrefixRow{
			Prefix:     fmt.Sprintf("s3://my-bucket/data/year=2024/month=%02d/day=%02d/file_%08d.parquet", i%12+1, i%28+1, i),
			Depth:      6,
			Count:      uint64(i + 1),
			TotalBytes: uint64((i + 1) * 1024),
		}
		rows[i].SetTier(tiers.Standard, uint64(i), uint64(i*1024))
		// Length, prefix, depth, count, bytes, then both tier arrays.
		rawSize += int64(4 + len(rows[i].Prefix) + 2 + 8 + 8 + extsort.MaxTiers*16)
	}

	writer, err := extsort.NewCompressedRunWriter(path, extsort.CompressedRunWriterOptions{})
	if err != nil {
		t.Fatalf("create compressed writer: %v", err)
	}
	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			t.Fatalf("write compressed: %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close compressed: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	ratio := float64(rawSize) / float64(info.Size())
	t.Logf("raw %d bytes, compressed %d bytes, ratio %.2fx", rawSize, info.Size(), ratio)

	if ratio < 2.0 {
		t.Errorf("expected at least 2x compression, got %.2fx", ratio)
	}
}

func TestCompressedRunEmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "empty.crun")

	writer, err := extsort.NewCompressedRunWriter(path, extsort.CompressedRunWriterOptions{})
	if err != nil {
		t.Fatalf("create writer: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	reader, err := extsort.OpenCompressedRunFile(path, 0)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()

	if reader.Count() != 0 {
		t.Errorf("expected count 0, got %d", reader.Count())
	}

	var row extsort.PrefixRow
	if err := reader.ReadInto(&row); !errors.Is(err, io.EOF) {
		t.Errorf("expected EOF on empty file, got %v", err)
	}
}
