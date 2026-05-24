package seeder

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/eunmann/s3-inv-db/internal/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// Test-only re-exports of unexported sentinel errors so the external
// seeder_test package can assert against them via errors.Is.
var (
	ErrEmptyBucket    = errEmptyBucket
	ErrEmptySrcBucket = errEmptySrcBucket
	ErrPrefixNotSlash = errPrefixNotSlash
)

// MD5Hex exposes md5Hex for tests.
func MD5Hex(b []byte) string { return md5Hex(b) }

// RunStampUUID exposes runStampUUID for tests.
func RunStampUUID(stamp time.Time, index int) string { return runStampUUID(stamp, index) }

// TierToS3Columns exposes tierToS3Columns for tests.
func TierToS3Columns(m *tiers.Mapping, id tiers.ID) (S3InventoryColumns, error) {
	return tierToS3Columns(m, id)
}

// CsvGzPayload re-exports the unexported csvGzPayload as a type alias
// so tests in the seeder_test package can read Body and Size by name.
type CsvGzPayload = csvGzPayload

// EncodeCSVGz exposes encodeCSVGz for tests.
func EncodeCSVGz(srcBucket string, objects []benchutil.FakeObject) (CsvGzPayload, error) {
	return encodeCSVGz(srcBucket, objects)
}

// NewS3ClientForTest exposes newS3Client for tests.
func NewS3ClientForTest(ctx context.Context) (*s3.Client, error) { return newS3Client(ctx) }

// GenerateInventory exposes generateInventory for tests.
func GenerateInventory(cfg Config, index int, seed int64) (InventoryInfo, error) {
	return generateInventory(cfg, index, seed)
}

// GetGeneratorConfig exposes getGeneratorConfig for tests.
func GetGeneratorConfig(preset string, numObjects int) benchutil.GeneratorConfig {
	return getGeneratorConfig(preset, numObjects)
}
