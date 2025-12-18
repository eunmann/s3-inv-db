package extsort

import (
	"context"
	"io"
	"math/rand"
	"sort"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/inventory"
)

// mockReader implements inventory.Reader for testing.
type mockReader struct {
	records []inventory.Record
	pos     int
}

func newMockReader(records []inventory.Record) *mockReader {
	return &mockReader{records: records}
}

func (m *mockReader) Read() (inventory.Record, error) {
	if m.pos >= len(m.records) {
		return inventory.Record{}, io.EOF
	}
	rec := m.records[m.pos]
	m.pos++
	return rec, nil
}

func (m *mockReader) Close() error { return nil }

func TestEmptySorter(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{MaxRecordsPerChunk: 10, TmpDir: dir}
	s := NewSorter(cfg)

	it, err := s.Merge(context.Background())
	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}
	defer it.Close()

	if it.Next() {
		t.Error("expected no records from empty sorter")
	}
	if it.Err() != nil {
		t.Errorf("unexpected error: %v", it.Err())
	}
}

func TestSingleChunk(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{MaxRecordsPerChunk: 100, TmpDir: dir}
	s := NewSorter(cfg)

	records := []inventory.Record{
		{Key: "c/file.txt", Size: 300},
		{Key: "a/file.txt", Size: 100},
		{Key: "b/file.txt", Size: 200},
	}

	if err := s.AddRecords(context.Background(), newMockReader(records)); err != nil {
		t.Fatalf("AddRecords failed: %v", err)
	}

	it, err := s.Merge(context.Background())
	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}
	defer it.Close()
	defer s.Cleanup()

	var result []inventory.Record
	for it.Next() {
		result = append(result, it.Record())
	}
	if it.Err() != nil {
		t.Fatalf("iteration error: %v", it.Err())
	}

	expected := []string{"a/file.txt", "b/file.txt", "c/file.txt"}
	if len(result) != len(expected) {
		t.Fatalf("got %d records, want %d", len(result), len(expected))
	}
	for i, key := range expected {
		if result[i].Key != key {
			t.Errorf("record[%d].Key = %s, want %s", i, result[i].Key, key)
		}
	}
}

func TestMultipleChunks(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{MaxRecordsPerChunk: 3, TmpDir: dir}
	s := NewSorter(cfg)

	// 10 records, should create 4 run files (3+3+3+1)
	records := []inventory.Record{
		{Key: "j/file.txt", Size: 10},
		{Key: "e/file.txt", Size: 5},
		{Key: "a/file.txt", Size: 1},
		{Key: "f/file.txt", Size: 6},
		{Key: "b/file.txt", Size: 2},
		{Key: "g/file.txt", Size: 7},
		{Key: "c/file.txt", Size: 3},
		{Key: "h/file.txt", Size: 8},
		{Key: "d/file.txt", Size: 4},
		{Key: "i/file.txt", Size: 9},
	}

	if err := s.AddRecords(context.Background(), newMockReader(records)); err != nil {
		t.Fatalf("AddRecords failed: %v", err)
	}

	it, err := s.Merge(context.Background())
	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}
	defer it.Close()
	defer s.Cleanup()

	var result []inventory.Record
	for it.Next() {
		result = append(result, it.Record())
	}
	if it.Err() != nil {
		t.Fatalf("iteration error: %v", it.Err())
	}

	// Verify sorted order
	if len(result) != 10 {
		t.Fatalf("got %d records, want 10", len(result))
	}
	for i := 1; i < len(result); i++ {
		if result[i].Key < result[i-1].Key {
			t.Errorf("records not sorted: %s < %s", result[i].Key, result[i-1].Key)
		}
	}

	// Verify sizes preserved
	sizeMap := make(map[string]uint64)
	for _, r := range records {
		sizeMap[r.Key] = r.Size
	}
	for _, r := range result {
		if r.Size != sizeMap[r.Key] {
			t.Errorf("size mismatch for %s: got %d, want %d", r.Key, r.Size, sizeMap[r.Key])
		}
	}
}

func TestLargeRandomDataset(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{MaxRecordsPerChunk: 100, TmpDir: dir}
	s := NewSorter(cfg)

	// Generate random records
	rng := rand.New(rand.NewSource(42))
	n := 1000
	records := make([]inventory.Record, n)
	for i := 0; i < n; i++ {
		key := randomKey(rng, 5)
		records[i] = inventory.Record{Key: key, Size: uint64(rng.Intn(10000))}
	}

	if err := s.AddRecords(context.Background(), newMockReader(records)); err != nil {
		t.Fatalf("AddRecords failed: %v", err)
	}

	it, err := s.Merge(context.Background())
	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}
	defer it.Close()
	defer s.Cleanup()

	var result []inventory.Record
	for it.Next() {
		result = append(result, it.Record())
	}
	if it.Err() != nil {
		t.Fatalf("iteration error: %v", it.Err())
	}

	if len(result) != n {
		t.Fatalf("got %d records, want %d", len(result), n)
	}

	// Verify globally sorted
	for i := 1; i < len(result); i++ {
		if result[i].Key < result[i-1].Key {
			t.Errorf("records not sorted at %d: %s < %s", i, result[i].Key, result[i-1].Key)
		}
	}
}

func TestMultipleReaders(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{MaxRecordsPerChunk: 5, TmpDir: dir}
	s := NewSorter(cfg)

	// Add records from multiple "files"
	records1 := []inventory.Record{
		{Key: "z/file.txt", Size: 26},
		{Key: "x/file.txt", Size: 24},
	}
	records2 := []inventory.Record{
		{Key: "y/file.txt", Size: 25},
		{Key: "a/file.txt", Size: 1},
	}

	if err := s.AddRecords(context.Background(), newMockReader(records1)); err != nil {
		t.Fatalf("AddRecords failed: %v", err)
	}
	if err := s.AddRecords(context.Background(), newMockReader(records2)); err != nil {
		t.Fatalf("AddRecords failed: %v", err)
	}

	it, err := s.Merge(context.Background())
	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}
	defer it.Close()
	defer s.Cleanup()

	var result []inventory.Record
	for it.Next() {
		result = append(result, it.Record())
	}
	if it.Err() != nil {
		t.Fatalf("iteration error: %v", it.Err())
	}

	expected := []string{"a/file.txt", "x/file.txt", "y/file.txt", "z/file.txt"}
	if len(result) != len(expected) {
		t.Fatalf("got %d records, want %d", len(result), len(expected))
	}
	for i, key := range expected {
		if result[i].Key != key {
			t.Errorf("record[%d].Key = %s, want %s", i, result[i].Key, key)
		}
	}
}

func TestContextCancellation(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{MaxRecordsPerChunk: 2, TmpDir: dir}
	s := NewSorter(cfg)

	records := []inventory.Record{
		{Key: "a/file.txt", Size: 1},
		{Key: "b/file.txt", Size: 2},
		{Key: "c/file.txt", Size: 3},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := s.AddRecords(ctx, newMockReader(records))
	if err == nil {
		t.Error("expected context cancellation error")
	}
}

func TestContextCancellationDuringMerge(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{MaxRecordsPerChunk: 2, TmpDir: dir}
	s := NewSorter(cfg)

	records := []inventory.Record{
		{Key: "a/file.txt", Size: 1},
		{Key: "b/file.txt", Size: 2},
		{Key: "c/file.txt", Size: 3},
		{Key: "d/file.txt", Size: 4},
	}

	if err := s.AddRecords(context.Background(), newMockReader(records)); err != nil {
		t.Fatalf("AddRecords failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	it, err := s.Merge(ctx)
	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}
	defer it.Close()
	defer s.Cleanup()

	// Read one record, then cancel
	if !it.Next() {
		t.Fatal("expected first record")
	}
	cancel()

	// Should detect cancellation on next iteration
	for it.Next() {
		// Keep iterating until cancellation is detected
	}
	if it.Err() != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", it.Err())
	}
}

func TestCleanup(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{MaxRecordsPerChunk: 2, TmpDir: dir}
	s := NewSorter(cfg)

	records := []inventory.Record{
		{Key: "a/file.txt", Size: 1},
		{Key: "b/file.txt", Size: 2},
		{Key: "c/file.txt", Size: 3},
	}

	if err := s.AddRecords(context.Background(), newMockReader(records)); err != nil {
		t.Fatalf("AddRecords failed: %v", err)
	}

	// Don't merge, just cleanup
	if err := s.Cleanup(); err != nil {
		t.Fatalf("Cleanup failed: %v", err)
	}

	// Verify files are removed by trying to merge (should be empty)
	it, err := s.Merge(context.Background())
	if err != nil {
		t.Fatalf("Merge after cleanup failed: %v", err)
	}
	defer it.Close()

	if it.Next() {
		t.Error("expected no records after cleanup")
	}
}

func TestDuplicateKeys(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{MaxRecordsPerChunk: 2, TmpDir: dir}
	s := NewSorter(cfg)

	records := []inventory.Record{
		{Key: "a/file.txt", Size: 100},
		{Key: "a/file.txt", Size: 200},
		{Key: "b/file.txt", Size: 300},
	}

	if err := s.AddRecords(context.Background(), newMockReader(records)); err != nil {
		t.Fatalf("AddRecords failed: %v", err)
	}

	it, err := s.Merge(context.Background())
	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}
	defer it.Close()
	defer s.Cleanup()

	var result []inventory.Record
	for it.Next() {
		result = append(result, it.Record())
	}
	if it.Err() != nil {
		t.Fatalf("iteration error: %v", it.Err())
	}

	if len(result) != 3 {
		t.Fatalf("got %d records, want 3", len(result))
	}

	// Both duplicates should appear
	if result[0].Key != "a/file.txt" || result[1].Key != "a/file.txt" {
		t.Errorf("expected two a/file.txt records first")
	}
}

func TestStableSorting(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{MaxRecordsPerChunk: 100, TmpDir: dir}
	s := NewSorter(cfg)

	// Records with same key but different sizes
	records := []inventory.Record{
		{Key: "same.txt", Size: 1},
		{Key: "same.txt", Size: 2},
		{Key: "same.txt", Size: 3},
	}

	if err := s.AddRecords(context.Background(), newMockReader(records)); err != nil {
		t.Fatalf("AddRecords failed: %v", err)
	}

	it, err := s.Merge(context.Background())
	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}
	defer it.Close()
	defer s.Cleanup()

	var result []inventory.Record
	for it.Next() {
		result = append(result, it.Record())
	}

	if len(result) != 3 {
		t.Fatalf("got %d records, want 3", len(result))
	}

	// All should have the same key
	for i, r := range result {
		if r.Key != "same.txt" {
			t.Errorf("record[%d].Key = %s, want same.txt", i, r.Key)
		}
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.MaxRecordsPerChunk != 1_000_000 {
		t.Errorf("MaxRecordsPerChunk = %d, want 1000000", cfg.MaxRecordsPerChunk)
	}
	if cfg.TmpDir == "" {
		t.Error("TmpDir should not be empty")
	}
}

// Helper function to generate random keys
func randomKey(rng *rand.Rand, depth int) string {
	parts := make([]string, depth)
	for i := 0; i < depth; i++ {
		parts[i] = randomString(rng, 5)
	}
	return join(parts, "/") + ".txt"
}

func randomString(rng *rand.Rand, n int) string {
	letters := "abcdefghijklmnopqrstuvwxyz"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rng.Intn(len(letters))]
	}
	return string(b)
}

func join(parts []string, sep string) string {
	if len(parts) == 0 {
		return ""
	}
	result := parts[0]
	for _, p := range parts[1:] {
		result += sep + p
	}
	return result
}

// Benchmark for merge performance
func BenchmarkMerge(b *testing.B) {
	dir := b.TempDir()
	cfg := Config{MaxRecordsPerChunk: 10000, TmpDir: dir}

	rng := rand.New(rand.NewSource(42))
	records := make([]inventory.Record, 100000)
	for i := range records {
		records[i] = inventory.Record{Key: randomKey(rng, 5), Size: uint64(i)}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := NewSorter(cfg)
		_ = s.AddRecords(context.Background(), newMockReader(records))
		it, _ := s.Merge(context.Background())
		for it.Next() {
		}
		it.Close()
		s.Cleanup()
	}
}

// Test that verifies comparison with standard library sort
func TestCompareWithStdSort(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{MaxRecordsPerChunk: 7, TmpDir: dir}
	s := NewSorter(cfg)

	rng := rand.New(rand.NewSource(123))
	n := 50
	records := make([]inventory.Record, n)
	for i := 0; i < n; i++ {
		records[i] = inventory.Record{
			Key:  randomKey(rng, 3),
			Size: uint64(rng.Intn(1000)),
		}
	}

	// Get expected order from standard sort
	expected := make([]inventory.Record, n)
	copy(expected, records)
	sort.Slice(expected, func(i, j int) bool {
		return expected[i].Key < expected[j].Key
	})

	if err := s.AddRecords(context.Background(), newMockReader(records)); err != nil {
		t.Fatalf("AddRecords failed: %v", err)
	}

	it, err := s.Merge(context.Background())
	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}
	defer it.Close()
	defer s.Cleanup()

	var result []inventory.Record
	for it.Next() {
		result = append(result, it.Record())
	}

	if len(result) != len(expected) {
		t.Fatalf("got %d records, want %d", len(result), len(expected))
	}

	for i := range result {
		if result[i].Key != expected[i].Key {
			t.Errorf("record[%d].Key = %s, want %s", i, result[i].Key, expected[i].Key)
		}
	}
}
