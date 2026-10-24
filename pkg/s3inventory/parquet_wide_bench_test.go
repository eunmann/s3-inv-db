package s3inventory_test

import (
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/s3inventory"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
	"github.com/parquet-go/parquet-go"
)

// awsInventoryRecord carries every field S3 Inventory can emit in
// Parquet, in AWS's own column names. The reader needs five of them.
type awsInventoryRecord struct {
	Bucket                       string    `parquet:"bucket"`
	Key                          string    `parquet:"key"`
	VersionID                    string    `parquet:"version_id,optional"`
	IsLatest                     bool      `parquet:"is_latest"`
	IsDeleteMarker               bool      `parquet:"is_delete_marker"`
	Size                         int64     `parquet:"size,optional"`
	LastModifiedDate             time.Time `parquet:"last_modified_date"`
	ETag                         string    `parquet:"e_tag,optional"`
	StorageClass                 string    `parquet:"storage_class,optional"`
	IsMultipartUploaded          bool      `parquet:"is_multipart_uploaded,optional"`
	ReplicationStatus            string    `parquet:"replication_status,optional"`
	EncryptionStatus             string    `parquet:"encryption_status,optional"`
	ObjectLockRetainUntilDate    time.Time `parquet:"object_lock_retain_until_date,optional"`
	ObjectLockMode               string    `parquet:"object_lock_mode,optional"`
	ObjectLockLegalHoldStatus    string    `parquet:"object_lock_legal_hold_status,optional"`
	IntelligentTieringAccessTier string    `parquet:"intelligent_tiering_access_tier,optional"`
	BucketKeyStatus              string    `parquet:"bucket_key_status,optional"`
	ChecksumAlgorithm            string    `parquet:"checksum_algorithm,optional"`
	ObjectAccessControlList      string    `parquet:"object_access_control_list,optional"`
	ObjectOwner                  string    `parquet:"object_owner,optional"`
}

// writeWideInventory writes n records with high-entropy ETags, owners
// and ACLs, as real inventories have, and returns the file path.
func writeWideInventory(tb testing.TB, n int) string {
	tb.Helper()
	tm := tiers.NewMapping()
	r := rand.New(rand.NewPCG(uint64(n), 1))
	hex := func(chars int) string {
		const digits = "0123456789abcdef"
		b := make([]byte, chars)
		for i := range b {
			b[i] = digits[r.IntN(len(digits))]
		}

		return string(b)
	}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := make([]awsInventoryRecord, 0, n)
	for _, o := range benchInventoryRows(n) {
		rows = append(rows, awsInventoryRecord{
			Bucket:                  "example-bucket",
			Key:                     o.Key,
			VersionID:               hex(32),
			IsLatest:                true,
			Size:                    int64(o.Size),
			LastModifiedDate:        base.Add(time.Duration(r.IntN(1<<30)) * time.Second),
			ETag:                    hex(32),
			StorageClass:            tierName(tm, o.TierID),
			ReplicationStatus:       "COMPLETED",
			EncryptionStatus:        "SSE-S3",
			BucketKeyStatus:         "DISABLED",
			ChecksumAlgorithm:       "CRC64NVME",
			ObjectAccessControlList: "eyJncmFudHMiOlt7InBlcm1pc3Npb24iOiJGVUxMX0NPTlRST0wifV19" + hex(64),
			ObjectOwner:             hex(64),
		})
	}
	path := filepath.Join(tb.TempDir(), "wide.parquet")
	if err := parquet.WriteFile(path, rows); err != nil {
		tb.Fatalf("WriteFile: %v", err)
	}

	return path
}

// BenchmarkParquetReaderWide reads an inventory with every AWS column,
// so it measures what reading only the needed columns saves.
func BenchmarkParquetReaderWide(b *testing.B) {
	const n = 500_000
	path := writeWideInventory(b, n)
	f, err := os.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		b.Fatal(err)
	}

	b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			reader, err := s3inventory.NewParquetReaderFromReaderAt(f, info.Size())
			if err != nil {
				b.Fatal(err)
			}
			count := 0
			for {
				_, err := reader.Next()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					b.Fatal(err)
				}
				count++
			}
			_ = reader.Close()
			if count != n {
				b.Fatalf("got %d rows, want %d", count, n)
			}
		}
	})
}
