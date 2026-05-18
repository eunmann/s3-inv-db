package seeder

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/md5" // MD5 is mandated by the AWS S3 Inventory manifest spec.
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/eunmann/s3-inv-db/internal/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// S3Config configures the S3 upload target.
type S3Config struct {
	Bucket    string // destination bucket (e.g., "s3-inv")
	Prefix    string // prefix under bucket (e.g., "inventory-data/"); must end with / if non-empty
	SrcBucket string // simulated source bucket name written into manifest + key path
}

// Validate returns an error if cfg is missing required fields.
func (c S3Config) Validate() error {
	if c.Bucket == "" {
		return errEmptyBucket
	}
	if c.SrcBucket == "" {
		return errEmptySrcBucket
	}
	if c.Prefix != "" && c.Prefix[len(c.Prefix)-1] != '/' {
		return errPrefixNotSlash
	}

	return nil
}

var (
	errEmptyBucket    = errors.New("s3 bucket is required when target is s3")
	errEmptySrcBucket = errors.New("s3 src bucket is required when target is s3")
	errPrefixNotSlash = errors.New("s3 prefix must end with /")
)

// newS3Client builds an aws-sdk-go-v2 S3 client. When AWS_ENDPOINT_URL_S3
// is set (the MinIO case) it forces path-style addressing.
func newS3Client(ctx context.Context) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	opts := []func(*s3.Options){}
	if os.Getenv(s3fetch.EnvEndpointURL) != "" {
		opts = append(opts, func(o *s3.Options) { o.UsePathStyle = true })
	}

	return s3.NewFromConfig(cfg, opts...), nil
}

// UploadInventory generates one synthetic S3 Inventory run and uploads
// the manifest + manifest.checksum + a single CSV.gz data file to MinIO/S3.
// The layout exactly mirrors what AWS S3 produces for a real inventory:
//
//	<bucket>/<prefix><src-bucket>/<inv-id>/<YYYY-MM-DDTHH-MMZ>/manifest.json
//	<bucket>/<prefix><src-bucket>/<inv-id>/<YYYY-MM-DDTHH-MMZ>/manifest.checksum
//	<bucket>/<prefix><src-bucket>/<inv-id>/data/<uuid>.csv.gz
func UploadInventory(ctx context.Context, client *s3.Client, cfg Config, s3cfg S3Config, index int, seed int64, runStamp time.Time) (InventoryInfo, error) {
	invID := fmt.Sprintf("inv-%03d", index)
	name := fmt.Sprintf("Seed Inventory %d", index)

	genCfg := getGeneratorConfig(cfg.Preset, cfg.Objects)
	genCfg.Seed = seed
	gen := benchutil.NewGenerator(genCfg)
	objects := gen.Generate()

	payload, err := encodeCSVGz(s3cfg.SrcBucket, objects)
	if err != nil {
		return InventoryInfo{}, fmt.Errorf("encode csv.gz: %w", err)
	}

	// AWS S3 Inventory uses minute-granularity run folders (YYYY-MM-DDTHH-MMZ).
	stamp := runStamp.UTC().Format("2006-01-02T15-04Z")
	dataKey := fmt.Sprintf("%s%s/%s/data/%s.csv.gz", s3cfg.Prefix, s3cfg.SrcBucket, invID, runStampUUID(runStamp, index))
	manifestKey := fmt.Sprintf("%s%s/%s/%s/manifest.json", s3cfg.Prefix, s3cfg.SrcBucket, invID, stamp)
	checksumKey := fmt.Sprintf("%s%s/%s/%s/manifest.checksum", s3cfg.Prefix, s3cfg.SrcBucket, invID, stamp)

	manifest := map[string]any{
		"sourceBucket":      s3cfg.SrcBucket,
		"destinationBucket": "arn:aws:s3:::" + s3cfg.Bucket,
		"version":           "2016-11-30",
		"creationTimestamp": strconv.FormatInt(runStamp.UnixMilli(), 10),
		"fileFormat":        "CSV",
		"fileSchema":        "Bucket, Key, Size, LastModifiedDate, ETag, StorageClass, IntelligentTieringAccessTier",
		"files": []map[string]any{
			{"key": dataKey, "size": payload.Size, "MD5checksum": md5Hex(payload.Body)},
		},
	}
	manifestJSON, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return InventoryInfo{}, fmt.Errorf("marshal manifest: %w", err)
	}

	if err := putObject(ctx, client, s3cfg.Bucket, dataKey, payload.Body, "application/gzip"); err != nil {
		return InventoryInfo{}, fmt.Errorf("upload data: %w", err)
	}
	if err := putObject(ctx, client, s3cfg.Bucket, manifestKey, manifestJSON, "application/json"); err != nil {
		return InventoryInfo{}, fmt.Errorf("upload manifest: %w", err)
	}
	if err := putObject(ctx, client, s3cfg.Bucket, checksumKey, []byte(md5Hex(manifestJSON)), "text/plain"); err != nil {
		return InventoryInfo{}, fmt.Errorf("upload checksum: %w", err)
	}

	return InventoryInfo{
		ID:       invID,
		Name:     name,
		Path:     fmt.Sprintf("s3://%s/%s", s3cfg.Bucket, manifestKey),
		Objects:  cfg.Objects,
		Prefixes: 0, // not built; the server builds the index on demand
	}, nil
}

// UploadMultiChunkInventory generates `cfg.Objects` synthetic objects,
// splits them into numChunks equal-ish slices, uploads each slice as
// its own .csv.gz data file, and writes a manifest pointing at all of
// them. Useful for benchmarks that need to exercise the build pipeline's
// chunk-level parallelism. NumChunks must be >= 1; values >= the
// generated object count fall back to one file per object.
func UploadMultiChunkInventory(ctx context.Context, client *s3.Client, cfg Config, s3cfg S3Config, index int, seed int64, runStamp time.Time, numChunks int) (InventoryInfo, error) {
	if numChunks < 1 {
		numChunks = 1
	}
	if numChunks > cfg.Objects {
		numChunks = cfg.Objects
	}
	invID := fmt.Sprintf("inv-%03d", index)
	name := fmt.Sprintf("Seed Inventory %d (multi-chunk)", index)

	genCfg := getGeneratorConfig(cfg.Preset, cfg.Objects)
	genCfg.Seed = seed
	gen := benchutil.NewGenerator(genCfg)

	stamp := runStamp.UTC().Format("2006-01-02T15-04Z")
	manifestKey := fmt.Sprintf("%s%s/%s/%s/manifest.json", s3cfg.Prefix, s3cfg.SrcBucket, invID, stamp)
	checksumKey := fmt.Sprintf("%s%s/%s/%s/manifest.checksum", s3cfg.Prefix, s3cfg.SrcBucket, invID, stamp)

	files := make([]map[string]any, 0, numChunks)
	baseChunkSize := cfg.Objects / numChunks
	remainder := cfg.Objects % numChunks
	chunkBuf := make([]benchutil.FakeObject, 0, baseChunkSize+1)
	for chunk := range numChunks {
		// Distribute the remainder across the first `remainder` chunks
		// so chunks differ by at most 1 object — matches the prior
		// (len+n-1)/n slicing without materialising the full slice.
		thisChunkSize := baseChunkSize
		if chunk < remainder {
			thisChunkSize++
		}
		chunkBuf = chunkBuf[:0]
		for range thisChunkSize {
			// Hot loop — drive the generator directly per object so
			// we never accumulate more than one chunk's objects in
			// memory at a time.
			chunkBuf = append(chunkBuf, gen.Next())
		}
		payload, err := encodeCSVGz(s3cfg.SrcBucket, chunkBuf)
		if err != nil {
			return InventoryInfo{}, fmt.Errorf("encode chunk %d: %w", chunk, err)
		}
		dataKey := fmt.Sprintf("%s%s/%s/data/%s-chunk-%04d.csv.gz", s3cfg.Prefix, s3cfg.SrcBucket, invID, runStampUUID(runStamp, index), chunk)
		if err := putObject(ctx, client, s3cfg.Bucket, dataKey, payload.Body, "application/gzip"); err != nil {
			return InventoryInfo{}, fmt.Errorf("upload chunk %d: %w", chunk, err)
		}
		files = append(files, map[string]any{
			"key":         dataKey,
			"size":        payload.Size,
			"MD5checksum": md5Hex(payload.Body),
		})
	}

	manifest := map[string]any{
		"sourceBucket":      s3cfg.SrcBucket,
		"destinationBucket": "arn:aws:s3:::" + s3cfg.Bucket,
		"version":           "2016-11-30",
		"creationTimestamp": strconv.FormatInt(runStamp.UnixMilli(), 10),
		"fileFormat":        "CSV",
		"fileSchema":        "Bucket, Key, Size, LastModifiedDate, ETag, StorageClass, IntelligentTieringAccessTier",
		"files":             files,
	}
	manifestJSON, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return InventoryInfo{}, fmt.Errorf("marshal manifest: %w", err)
	}
	if err := putObject(ctx, client, s3cfg.Bucket, manifestKey, manifestJSON, "application/json"); err != nil {
		return InventoryInfo{}, fmt.Errorf("upload manifest: %w", err)
	}
	if err := putObject(ctx, client, s3cfg.Bucket, checksumKey, []byte(md5Hex(manifestJSON)), "text/plain"); err != nil {
		return InventoryInfo{}, fmt.Errorf("upload checksum: %w", err)
	}

	return InventoryInfo{
		ID:       invID,
		Name:     name,
		Path:     fmt.Sprintf("s3://%s/%s", s3cfg.Bucket, manifestKey),
		Objects:  cfg.Objects,
		Prefixes: 0,
	}, nil
}

// csvGzPayload is the gzipped CSV body plus its on-the-wire byte size
// (used to populate the manifest's `size` field). The two values are
// always produced together so a struct is more honest than a tuple.
type csvGzPayload struct {
	Body []byte
	Size int64
}

// encodeCSVGz writes synthetic objects as a gzipped, URL-encoded CSV body
// matching the fileSchema declared in the manifest. AWS S3 Inventory uses
// URL-encoded field values and CSV without a header row.
func encodeCSVGz(srcBucket string, objects []benchutil.FakeObject) (csvGzPayload, error) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	w := csv.NewWriter(gz)

	mapping := tiers.NewMapping()
	now := time.Now().UTC().Format(time.RFC3339)
	etag := `"0000000000000000000000000000000000"`

	for _, o := range objects {
		cols := tierToS3Columns(mapping, o.TierID)
		// Real S3 Inventory URL-encodes Key values, but our consumer
		// (pkg/inventory/reader.go) does not URL-decode them, so keep the
		// keys raw to match what the build pipeline actually expects.
		row := []string{
			srcBucket,
			o.Key,
			strconv.FormatUint(o.Size, 10),
			now,
			etag,
			cols.StorageClass,
			cols.AccessTier,
		}
		if err := w.Write(row); err != nil {
			return csvGzPayload{}, fmt.Errorf("write csv row: %w", err)
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return csvGzPayload{}, fmt.Errorf("flush csv: %w", err)
	}
	if err := gz.Close(); err != nil {
		return csvGzPayload{}, fmt.Errorf("close gzip: %w", err)
	}

	body := buf.Bytes()

	return csvGzPayload{Body: body, Size: int64(len(body))}, nil
}

// storageClassIntelligentTiering is the S3 Inventory StorageClass column
// value AWS writes for every Intelligent-Tiering object regardless of
// the per-object access tier sub-state.
const storageClassIntelligentTiering = "INTELLIGENT_TIERING"

// S3InventoryColumns holds the pair of columns AWS writes per row in
// the inventory CSV: the StorageClass and the per-object access tier
// (the latter only populated for Intelligent-Tiering objects).
type S3InventoryColumns struct {
	StorageClass string
	AccessTier   string
}

// tierToS3Columns inverts tiers.Mapping.FromS3: split a tier ID back into
// the (StorageClass, IntelligentTieringAccessTier) inventory columns AWS
// would produce.
func tierToS3Columns(m *tiers.Mapping, id tiers.ID) S3InventoryColumns {
	switch id {
	case tiers.ITFrequent, tiers.ITFrequentSmall:
		// Small IT_FREQUENT objects round-trip through the same S3 columns;
		// the size-aware classifier on the read side reclassifies them.
		return S3InventoryColumns{StorageClass: storageClassIntelligentTiering, AccessTier: "FREQUENT_ACCESS"}
	case tiers.ITInfrequent:
		return S3InventoryColumns{StorageClass: storageClassIntelligentTiering, AccessTier: "INFREQUENT_ACCESS"}
	case tiers.ITArchiveInstant:
		return S3InventoryColumns{StorageClass: storageClassIntelligentTiering, AccessTier: "ARCHIVE_INSTANT_ACCESS"}
	case tiers.ITArchive:
		return S3InventoryColumns{StorageClass: storageClassIntelligentTiering, AccessTier: "ARCHIVE_ACCESS"}
	case tiers.ITDeepArchive:
		return S3InventoryColumns{StorageClass: storageClassIntelligentTiering, AccessTier: "DEEP_ARCHIVE_ACCESS"}
	case tiers.Standard, tiers.StandardIA, tiers.OneZoneIA,
		tiers.GlacierIR, tiers.GlacierFR, tiers.DeepArchive,
		tiers.ReducedRedundancy, tiers.NumTiers:
		// Plain S3 storage classes (no access-tier column). NumTiers is
		// the sentinel and shouldn't appear at this layer, but handle it
		// defensively rather than letting exhaustive fall through.
	}

	return S3InventoryColumns{StorageClass: m.ByID(id).Name}
}

func putObject(ctx context.Context, client *s3.Client, bucket, key string, body []byte, contentType string) error {
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(body),
		ContentType: aws.String(contentType),
	})
	if err != nil {
		return fmt.Errorf("put s3://%s/%s: %w", bucket, key, err)
	}

	return nil
}

func md5Hex(b []byte) string {
	sum := md5.Sum(b)

	return hex.EncodeToString(sum[:])
}

// runStampUUID returns a deterministic synthetic UUID per (run, index).
func runStampUUID(stamp time.Time, index int) string {
	return fmt.Sprintf("%010x-%04x", stamp.Unix(), index)
}
