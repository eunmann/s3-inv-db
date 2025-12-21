package extsort

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

//nolint:gocyclo // Test coverage over multiple scenarios requires high cyclomatic complexity
func TestCompressedRunFile(t *testing.T) {
	t.Run("write and read", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "test.crun")

		// Create test rows
		rows := []*PrefixRow{
			{Prefix: "", Depth: 0, Count: 10, TotalBytes: 1000},
			{Prefix: "data/", Depth: 1, Count: 5, TotalBytes: 500},
			{Prefix: "data/2024/", Depth: 2, Count: 3, TotalBytes: 300},
		}
		rows[0].TierCounts[tiers.Standard] = 7
		rows[0].TierCounts[tiers.GlacierFR] = 3
		rows[0].TierBytes[tiers.Standard] = 700
		rows[0].TierBytes[tiers.GlacierFR] = 300

		// Write compressed
		writer, err := NewCompressedRunWriter(path, CompressedRunWriterOptions{})
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
		reader, err := OpenCompressedRunFile(path, 0)
		if err != nil {
			t.Fatalf("open reader: %v", err)
		}
		defer reader.Close()

		if reader.Count() != 3 {
			t.Errorf("expected count 3, got %d", reader.Count())
		}

		// Read all rows
		for i := range 3 {
			row, err := reader.Read()
			if err != nil {
				t.Fatalf("read row %d: %v", i, err)
			}
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

		// Verify tier data on first row
		row, _ := OpenCompressedRunFile(path, 0)
		firstRow, _ := row.Read()
		row.Close()
		if firstRow.TierCounts[tiers.Standard] != 7 {
			t.Errorf("expected 7 Standard tier count, got %d", firstRow.TierCounts[tiers.Standard])
		}
		if firstRow.TierBytes[tiers.GlacierFR] != 300 {
			t.Errorf("expected 300 GlacierFR bytes, got %d", firstRow.TierBytes[tiers.GlacierFR])
		}

		// EOF
		reader2, _ := OpenCompressedRunFile(path, 0)
		for range 3 {
			reader2.Read()
		}
		_, err = reader2.Read()
		reader2.Close()
		if !errors.Is(err, io.EOF) {
			t.Errorf("expected EOF, got %v", err)
		}
	})

	t.Run("sorted write", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "test.crun")

		// Create unsorted rows
		rows := []*PrefixRow{
			{Prefix: "z/", Depth: 1, Count: 1, TotalBytes: 100},
			{Prefix: "a/", Depth: 1, Count: 2, TotalBytes: 200},
			{Prefix: "m/", Depth: 1, Count: 3, TotalBytes: 300},
		}

		writer, err := NewCompressedRunWriter(path, CompressedRunWriterOptions{})
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
		reader, err := OpenCompressedRunFile(path, 0)
		if err != nil {
			t.Fatalf("open reader: %v", err)
		}
		defer reader.Close()

		expectedOrder := []string{"a/", "m/", "z/"}
		for i := range 3 {
			row, err := reader.Read()
			if err != nil {
				t.Fatalf("read row %d: %v", i, err)
			}
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
		writer, err := NewCompressedRunWriter(path, CompressedRunWriterOptions{})
		if err != nil {
			t.Fatalf("create writer: %v", err)
		}
		for i := range numRows {
			row := &PrefixRow{
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
		reader, err := OpenCompressedRunFile(path, 0)
		if err != nil {
			t.Fatalf("open reader: %v", err)
		}
		defer reader.Close()

		if reader.Count() != numRows {
			t.Errorf("expected count %d, got %d", numRows, reader.Count())
		}

		// Read all and verify
		for i := range numRows {
			row, err := reader.Read()
			if err != nil {
				t.Fatalf("read row %d: %v", i, err)
			}
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

		levels := []CompressionLevel{
			CompressionFastest,
			CompressionDefault,
			CompressionBetter,
		}

		for _, level := range levels {
			path := filepath.Join(tmpDir, fmt.Sprintf("level%d.crun", level))

			writer, err := NewCompressedRunWriter(path, CompressedRunWriterOptions{
				CompressionLevel: level,
			})
			if err != nil {
				t.Fatalf("level %d: create writer: %v", level, err)
			}
			for i := range 100 {
				row := &PrefixRow{
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
			reader, err := OpenCompressedRunFile(path, 0)
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

		writer, err := NewCompressedRunWriter(path, CompressedRunWriterOptions{})
		if err != nil {
			t.Fatalf("create writer: %v", err)
		}
		writer.Write(&PrefixRow{Prefix: "test/"})
		writer.Close()

		reader, err := OpenCompressedRunFile(path, 0)
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

func TestCompressedVsUncompressedSize(t *testing.T) {
	tmpDir := t.TempDir()
	uncompressedPath := filepath.Join(tmpDir, "uncompressed.run")
	compressedPath := filepath.Join(tmpDir, "compressed.crun")

	const numRows = 5000

	// Create test rows with realistic prefix data (highly compressible)
	rows := make([]*PrefixRow, numRows)
	for i := range numRows {
		rows[i] = &PrefixRow{
			Prefix:     fmt.Sprintf("s3://my-bucket/data/year=2024/month=%02d/day=%02d/file_%08d.parquet", i%12+1, i%28+1, i),
			Depth:      6,
			Count:      uint64(i + 1),
			TotalBytes: uint64((i + 1) * 1024),
		}
		rows[i].TierCounts[tiers.Standard] = uint64(i)
		rows[i].TierBytes[tiers.Standard] = uint64(i * 1024)
	}

	// Write uncompressed
	uWriter, err := NewRunFileWriter(uncompressedPath, 0)
	if err != nil {
		t.Fatalf("create uncompressed writer: %v", err)
	}
	for _, row := range rows {
		if err := uWriter.Write(row); err != nil {
			t.Fatalf("write uncompressed: %v", err)
		}
	}
	if err := uWriter.Close(); err != nil {
		t.Fatalf("close uncompressed: %v", err)
	}

	// Write compressed
	cWriter, err := NewCompressedRunWriter(compressedPath, CompressedRunWriterOptions{})
	if err != nil {
		t.Fatalf("create compressed writer: %v", err)
	}
	for _, row := range rows {
		if err := cWriter.Write(row); err != nil {
			t.Fatalf("write compressed: %v", err)
		}
	}
	if err := cWriter.Close(); err != nil {
		t.Fatalf("close compressed: %v", err)
	}

	// Compare file sizes
	uInfo, _ := os.Stat(uncompressedPath)
	cInfo, _ := os.Stat(compressedPath)

	uSize := uInfo.Size()
	cSize := cInfo.Size()
	ratio := float64(uSize) / float64(cSize)

	t.Logf("Uncompressed: %d bytes", uSize)
	t.Logf("Compressed:   %d bytes", cSize)
	t.Logf("Ratio:        %.2fx", ratio)

	// Expect at least 2x compression for this type of data
	if ratio < 2.0 {
		t.Errorf("expected at least 2x compression, got %.2fx", ratio)
	}
}

func TestRunReaderInterface(t *testing.T) {
	tmpDir := t.TempDir()

	t.Run("auto-detect uncompressed", func(t *testing.T) {
		path := filepath.Join(tmpDir, "uncompressed.run")

		writer, _ := NewRunFileWriter(path, 0)
		writer.Write(&PrefixRow{Prefix: "test/", Count: 42})
		writer.Close()

		reader, err := OpenRunFileAuto(path, 0)
		if err != nil {
			t.Fatalf("open auto: %v", err)
		}
		defer reader.Close()

		if reader.Count() != 1 {
			t.Errorf("expected count 1, got %d", reader.Count())
		}

		row, _ := reader.Read()
		if row.Prefix != "test/" {
			t.Errorf("expected prefix 'test/', got %q", row.Prefix)
		}
		if row.Count != 42 {
			t.Errorf("expected count 42, got %d", row.Count)
		}
	})

	t.Run("auto-detect compressed", func(t *testing.T) {
		path := filepath.Join(tmpDir, "compressed.crun")

		writer, _ := NewCompressedRunWriter(path, CompressedRunWriterOptions{})
		writer.Write(&PrefixRow{Prefix: "test/", Count: 42})
		writer.Close()

		reader, err := OpenRunFileAuto(path, 0)
		if err != nil {
			t.Fatalf("open auto: %v", err)
		}
		defer reader.Close()

		if reader.Count() != 1 {
			t.Errorf("expected count 1, got %d", reader.Count())
		}

		row, _ := reader.Read()
		if row.Prefix != "test/" {
			t.Errorf("expected prefix 'test/', got %q", row.Prefix)
		}
		if row.Count != 42 {
			t.Errorf("expected count 42, got %d", row.Count)
		}
	})
}

func TestCompressedRunEmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "empty.crun")

	writer, err := NewCompressedRunWriter(path, CompressedRunWriterOptions{})
	if err != nil {
		t.Fatalf("create writer: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	reader, err := OpenCompressedRunFile(path, 0)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()

	if reader.Count() != 0 {
		t.Errorf("expected count 0, got %d", reader.Count())
	}

	_, err = reader.Read()
	if !errors.Is(err, io.EOF) {
		t.Errorf("expected EOF on empty file, got %v", err)
	}
}
