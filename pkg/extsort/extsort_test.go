package extsort

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

func TestAggregator(t *testing.T) {
	t.Run("basic aggregation", func(t *testing.T) {
		agg := NewAggregator(100, 0)

		// Add some objects
		agg.AddObject("data/2024/01/file1.txt", 100, tiers.Standard)
		agg.AddObject("data/2024/01/file2.txt", 200, tiers.Standard)
		agg.AddObject("data/2024/02/file3.txt", 300, tiers.GlacierFR)

		if agg.ObjectCount() != 3 {
			t.Errorf("expected 3 objects, got %d", agg.ObjectCount())
		}

		if agg.BytesProcessed() != 600 {
			t.Errorf("expected 600 bytes, got %d", agg.BytesProcessed())
		}

		// Drain and check prefixes
		rows := agg.Drain()
		if len(rows) != 5 { // root, data/, data/2024/, data/2024/01/, data/2024/02/
			t.Errorf("expected 5 prefixes, got %d", len(rows))
		}
	})

	t.Run("max depth limit", func(t *testing.T) {
		agg := NewAggregator(100, 2)

		agg.AddObject("a/b/c/d/e/file.txt", 100, tiers.Standard)

		rows := agg.Drain()
		// Should only have root, a/, a/b/ (depth 0, 1, 2)
		if len(rows) != 3 {
			t.Errorf("expected 3 prefixes with maxDepth=2, got %d", len(rows))
		}
	})

	t.Run("tier tracking", func(t *testing.T) {
		agg := NewAggregator(100, 0)

		agg.AddObject("data/file1.txt", 100, tiers.Standard)
		agg.AddObject("data/file2.txt", 200, tiers.GlacierFR)
		agg.AddObject("data/file3.txt", 300, tiers.GlacierIR)

		rows := agg.Drain()

		// Find the data/ prefix
		var dataRow *PrefixRow
		for _, row := range rows {
			if row.Prefix == "data/" {
				dataRow = row
				break
			}
		}

		if dataRow == nil {
			t.Fatal("data/ prefix not found")
		}

		if dataRow.TierCounts[tiers.Standard] != 1 {
			t.Errorf("expected 1 Standard object, got %d", dataRow.TierCounts[tiers.Standard])
		}
		if dataRow.TierCounts[tiers.GlacierFR] != 1 {
			t.Errorf("expected 1 Glacier object, got %d", dataRow.TierCounts[tiers.GlacierFR])
		}
		if dataRow.TierBytes[tiers.GlacierIR] != 300 {
			t.Errorf("expected 300 GlacierIR bytes, got %d", dataRow.TierBytes[tiers.GlacierIR])
		}
	})
}

func TestRunFile(t *testing.T) {
	t.Run("write and read", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "test.run")

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

		// Write
		writer, err := NewRunFileWriter(path, 0)
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

		// Read
		reader, err := OpenRunFile(path, 0)
		if err != nil {
			t.Fatalf("open reader: %v", err)
		}
		defer reader.Close()

		if reader.Count() != 3 {
			t.Errorf("expected count 3, got %d", reader.Count())
		}

		// Read all rows
		for i := 0; i < 3; i++ {
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
		}

		// EOF
		_, err = reader.Read()
		if err != io.EOF {
			t.Errorf("expected EOF, got %v", err)
		}
	})

	t.Run("sorted write", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "sorted.run")

		// Create unsorted rows
		rows := []*PrefixRow{
			{Prefix: "data/", Depth: 1, Count: 5, TotalBytes: 500},
			{Prefix: "", Depth: 0, Count: 10, TotalBytes: 1000},
			{Prefix: "zebra/", Depth: 1, Count: 2, TotalBytes: 200},
			{Prefix: "alpha/", Depth: 1, Count: 3, TotalBytes: 300},
		}

		// Write sorted
		writer, err := NewRunFileWriter(path, 0)
		if err != nil {
			t.Fatalf("create writer: %v", err)
		}
		if err := writer.WriteSorted(rows); err != nil {
			t.Fatalf("write sorted: %v", err)
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("close writer: %v", err)
		}

		// Read and verify order
		reader, err := OpenRunFile(path, 0)
		if err != nil {
			t.Fatalf("open reader: %v", err)
		}
		defer reader.Close()

		expected := []string{"", "alpha/", "data/", "zebra/"}
		for i, exp := range expected {
			row, err := reader.Read()
			if err != nil {
				t.Fatalf("read row %d: %v", i, err)
			}
			if row.Prefix != exp {
				t.Errorf("row %d: expected prefix %q, got %q", i, exp, row.Prefix)
			}
		}
	})
}

func TestMerger(t *testing.T) {
	t.Run("merge multiple run files", func(t *testing.T) {
		tmpDir := t.TempDir()

		// Create run file 1
		path1 := filepath.Join(tmpDir, "run1.bin")
		w1, _ := NewRunFileWriter(path1, 0)
		w1.Write(&PrefixRow{Prefix: "", Depth: 0, Count: 5, TotalBytes: 500})
		w1.Write(&PrefixRow{Prefix: "alpha/", Depth: 1, Count: 2, TotalBytes: 200})
		w1.Write(&PrefixRow{Prefix: "gamma/", Depth: 1, Count: 3, TotalBytes: 300})
		w1.Close()

		// Create run file 2
		path2 := filepath.Join(tmpDir, "run2.bin")
		w2, _ := NewRunFileWriter(path2, 0)
		w2.Write(&PrefixRow{Prefix: "", Depth: 0, Count: 7, TotalBytes: 700})
		w2.Write(&PrefixRow{Prefix: "beta/", Depth: 1, Count: 4, TotalBytes: 400})
		w2.Write(&PrefixRow{Prefix: "gamma/", Depth: 1, Count: 5, TotalBytes: 500})
		w2.Close()

		// Merge
		merger, err := NewMergeIterator([]string{path1, path2}, 0)
		if err != nil {
			t.Fatalf("create merger: %v", err)
		}
		defer merger.Close()

		// Expected merged order with duplicates combined
		expected := []struct {
			prefix string
			count  uint64
			bytes  uint64
		}{
			{"", 12, 1200},           // merged
			{"alpha/", 2, 200},       // from run1 only
			{"beta/", 4, 400},        // from run2 only
			{"gamma/", 8, 800},       // merged
		}

		for i, exp := range expected {
			row, err := merger.Next()
			if err != nil {
				t.Fatalf("next %d: %v", i, err)
			}
			if row.Prefix != exp.prefix {
				t.Errorf("row %d: expected prefix %q, got %q", i, exp.prefix, row.Prefix)
			}
			if row.Count != exp.count {
				t.Errorf("row %d (%s): expected count %d, got %d", i, exp.prefix, exp.count, row.Count)
			}
			if row.TotalBytes != exp.bytes {
				t.Errorf("row %d (%s): expected bytes %d, got %d", i, exp.prefix, exp.bytes, row.TotalBytes)
			}
		}

		// EOF
		_, err = merger.Next()
		if err != io.EOF {
			t.Errorf("expected EOF, got %v", err)
		}
	})
}

func TestIndexBuilder(t *testing.T) {
	t.Run("build from sorted rows", func(t *testing.T) {
		tmpDir := t.TempDir()
		outDir := filepath.Join(tmpDir, "index")

		// Create builder
		builder, err := NewIndexBuilder(outDir)
		if err != nil {
			t.Fatalf("create builder: %v", err)
		}

		// Add sorted rows
		rows := []*PrefixRow{
			{Prefix: "", Depth: 0, Count: 10, TotalBytes: 1000},
			{Prefix: "data/", Depth: 1, Count: 7, TotalBytes: 700},
			{Prefix: "data/2024/", Depth: 2, Count: 5, TotalBytes: 500},
			{Prefix: "data/2024/01/", Depth: 3, Count: 3, TotalBytes: 300},
			{Prefix: "data/2024/02/", Depth: 3, Count: 2, TotalBytes: 200},
			{Prefix: "logs/", Depth: 1, Count: 3, TotalBytes: 300},
		}

		for _, row := range rows {
			if err := builder.Add(row); err != nil {
				t.Fatalf("add row: %v", err)
			}
		}

		// Finalize
		if err := builder.Finalize(); err != nil {
			t.Fatalf("finalize: %v", err)
		}

		if builder.Count() != 6 {
			t.Errorf("expected 6 prefixes, got %d", builder.Count())
		}

		// Open and verify the built index
		idx, err := indexread.Open(outDir)
		if err != nil {
			t.Fatalf("open index: %v", err)
		}
		defer idx.Close()

		// Verify root lookup
		pos, ok := idx.Lookup("")
		if !ok {
			t.Fatal("root prefix not found")
		}
		stats := idx.Stats(pos)
		if stats.ObjectCount != 10 {
			t.Errorf("root: expected 10 objects, got %d", stats.ObjectCount)
		}
		if stats.TotalBytes != 1000 {
			t.Errorf("root: expected 1000 bytes, got %d", stats.TotalBytes)
		}

		// Verify data/ lookup
		pos, ok = idx.Lookup("data/")
		if !ok {
			t.Fatal("data/ prefix not found")
		}
		stats = idx.Stats(pos)
		if stats.ObjectCount != 7 {
			t.Errorf("data/: expected 7 objects, got %d", stats.ObjectCount)
		}

		// Verify subtree navigation
		pos, ok = idx.Lookup("data/2024/")
		if !ok {
			t.Fatal("data/2024/ prefix not found")
		}
		descendants, err := idx.DescendantsAtDepth(pos, 1)
		if err != nil {
			t.Fatalf("get descendants: %v", err)
		}
		if len(descendants) != 2 {
			t.Errorf("expected 2 descendants at depth 1, got %d", len(descendants))
		}
	})
}

func TestEndToEndWithAggregator(t *testing.T) {
	t.Run("full pipeline simulation", func(t *testing.T) {
		tmpDir := t.TempDir()

		// Simulate pipeline: aggregate -> flush -> merge -> build
		agg := NewAggregator(100, 0)

		// Add objects (simulating first chunk)
		agg.AddObject("data/2024/01/file1.txt", 100, tiers.Standard)
		agg.AddObject("data/2024/01/file2.txt", 200, tiers.Standard)
		agg.AddObject("data/2024/02/file3.txt", 300, tiers.GlacierFR)

		// Flush to run file 1
		rows1 := agg.Drain()
		path1 := filepath.Join(tmpDir, "run1.bin")
		w1, _ := NewRunFileWriter(path1, 0)
		w1.WriteSorted(rows1)
		w1.Close()

		// Add more objects (simulating second chunk)
		agg.AddObject("logs/app.log", 400, tiers.Standard)
		agg.AddObject("data/2024/01/file4.txt", 500, tiers.Standard)

		// Flush to run file 2
		rows2 := agg.Drain()
		path2 := filepath.Join(tmpDir, "run2.bin")
		w2, _ := NewRunFileWriter(path2, 0)
		w2.WriteSorted(rows2)
		w2.Close()

		// Merge run files
		merger, err := NewMergeIterator([]string{path1, path2}, 0)
		if err != nil {
			t.Fatalf("create merger: %v", err)
		}

		// Build index
		outDir := filepath.Join(tmpDir, "index")
		builder, err := NewIndexBuilder(outDir)
		if err != nil {
			t.Fatalf("create builder: %v", err)
		}

		if err := builder.AddAll(merger); err != nil {
			t.Fatalf("add all: %v", err)
		}
		merger.Close()

		if err := builder.Finalize(); err != nil {
			t.Fatalf("finalize: %v", err)
		}

		// Verify index
		idx, err := indexread.Open(outDir)
		if err != nil {
			t.Fatalf("open index: %v", err)
		}
		defer idx.Close()

		// Root should have 5 objects
		pos, ok := idx.Lookup("")
		if !ok {
			t.Fatal("root not found")
		}
		stats := idx.Stats(pos)
		if stats.ObjectCount != 5 {
			t.Errorf("root: expected 5 objects, got %d", stats.ObjectCount)
		}
		if stats.TotalBytes != 1500 {
			t.Errorf("root: expected 1500 bytes, got %d", stats.TotalBytes)
		}

		// data/ should have 4 objects (merged from both runs)
		pos, ok = idx.Lookup("data/")
		if !ok {
			t.Fatal("data/ not found")
		}
		stats = idx.Stats(pos)
		if stats.ObjectCount != 4 {
			t.Errorf("data/: expected 4 objects, got %d", stats.ObjectCount)
		}

		// logs/ should have 1 object
		pos, ok = idx.Lookup("logs/")
		if !ok {
			t.Fatal("logs/ not found")
		}
		stats = idx.Stats(pos)
		if stats.ObjectCount != 1 {
			t.Errorf("logs/: expected 1 object, got %d", stats.ObjectCount)
		}
	})
}

func BenchmarkAggregator(b *testing.B) {
	b.Run("AddObject", func(b *testing.B) {
		agg := NewAggregator(100000, 0)
		key := "data/2024/01/02/03/file.txt"

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			agg.AddObject(key, 1000, tiers.Standard)
		}
	})
}

func BenchmarkRunFile(b *testing.B) {
	tmpDir := b.TempDir()
	path := filepath.Join(tmpDir, "bench.run")

	b.Run("Write", func(b *testing.B) {
		row := &PrefixRow{
			Prefix:     "data/2024/01/02/03/",
			Depth:      5,
			Count:      1000,
			TotalBytes: 100000,
		}
		row.TierCounts[tiers.Standard] = 800
		row.TierCounts[tiers.GlacierFR] = 200
		row.TierBytes[tiers.Standard] = 80000
		row.TierBytes[tiers.GlacierFR] = 20000

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			writer, _ := NewRunFileWriter(path, 0)
			for j := 0; j < 1000; j++ {
				writer.Write(row)
			}
			writer.Close()
		}
	})

	b.Run("Read", func(b *testing.B) {
		// Create test file
		row := &PrefixRow{
			Prefix:     "data/2024/01/02/03/",
			Depth:      5,
			Count:      1000,
			TotalBytes: 100000,
		}
		writer, _ := NewRunFileWriter(path, 0)
		for j := 0; j < 1000; j++ {
			writer.Write(row)
		}
		writer.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			reader, _ := OpenRunFile(path, 0)
			for {
				_, err := reader.Read()
				if err == io.EOF {
					break
				}
			}
			reader.Close()
		}
	})
}

func init() {
	// Ensure test temp dirs are cleaned up
	os.MkdirAll(os.TempDir(), 0755)
}
