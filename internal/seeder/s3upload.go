package seeder

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/md5" // MD5 is mandated by the AWS S3 Inventory manifest spec.
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
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
// is set (a local S3 server) it forces path-style addressing.
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
// the manifest + manifest.checksum + a single CSV.gz data file to S3.
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

	payload, err := encodeCSVGz(s3cfg.SrcBucket, seed, objects)
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
		"fileSchema":        csvFileSchema,
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

// autoChunkCount picks a chunk count that gives synthetic inventories
// the same multi-file shape real S3 inventories have (S3 caps each
// .csv.gz data file around 1 GiB, so a billion-object bucket has 100+
// chunks). One chunk per ~100K objects, capped at 16 so small runs
// stay single-file and 2.5M-object runs land at ~16 chunks.
func autoChunkCount(objects int) int {
	const objectsPerChunk = 100_000
	const maxChunks = 16
	n := objects / objectsPerChunk
	if n < 1 {
		return 1
	}
	if n > maxChunks {
		return maxChunks
	}

	return n
}

// UploadMultiChunkInventory generates `cfg.Objects` synthetic objects,
// splits them into numChunks equal-ish slices, uploads each slice as
// its own .csv.gz data file, and writes a manifest pointing at all of
// them. Useful for benchmarks that need to exercise the build pipeline's
// chunk-level parallelism. NumChunks must be >= 1; values >= the
// generated object count fall back to one file per object.
func UploadMultiChunkInventory(ctx context.Context, client *s3.Client, cfg Config, s3cfg S3Config, index int, seed int64, runStamp time.Time, numChunks int) (InventoryInfo, error) {
	put := func(ctx context.Context, key string, body []byte, contentType string) error {
		return putObject(ctx, client, s3cfg.Bucket, key, body, contentType)
	}

	return writeMultiChunkInventory(ctx, put, cfg, s3cfg, index, seed, runStamp, numChunks)
}

// The sample inventory FixtureFingerprint hashes: enough rows over
// enough chunks to cover the row encoding, the key order and the chunk
// split.
const (
	fingerprintObjects = 1000
	fingerprintChunks  = 3
	fingerprintHexLen  = 6
)

// FixtureFingerprint returns a short digest of what
// UploadMultiChunkInventory writes for cfg's Preset and SortedKeys and
// seed, taken over a small sample inventory at a fixed run time. A
// benchmark that caches a seeded fixture puts it in the cache key, so
// a seeder that writes different bytes seeds a new fixture instead of
// reusing one an older seeder wrote.
func FixtureFingerprint(ctx context.Context, cfg Config, seed int64) (string, error) {
	h := sha256.New()
	put := func(_ context.Context, key string, body []byte, _ string) error {
		h.Write([]byte(key))
		h.Write(body)

		return nil
	}
	cfg.Objects = fingerprintObjects
	s3cfg := S3Config{Bucket: "fingerprint", SrcBucket: "fingerprint"}
	if _, err := writeMultiChunkInventory(ctx, put, cfg, s3cfg, 1, seed, time.Unix(0, 0), fingerprintChunks); err != nil {
		return "", fmt.Errorf("write sample inventory: %w", err)
	}

	return hex.EncodeToString(h.Sum(nil))[:fingerprintHexLen], nil
}

// putFunc stores one object of a seeded inventory.
type putFunc func(ctx context.Context, key string, body []byte, contentType string) error

// writeMultiChunkInventory is UploadMultiChunkInventory with its
// objects handed to put.
func writeMultiChunkInventory(ctx context.Context, put putFunc, cfg Config, s3cfg S3Config, index int, seed int64, runStamp time.Time, numChunks int) (InventoryInfo, error) {
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

	var sorted []benchutil.FakeObject
	if cfg.SortedKeys {
		sorted = make([]benchutil.FakeObject, cfg.Objects)
		for i := range sorted {
			sorted[i] = gen.Next()
		}
		slices.SortStableFunc(sorted, func(a, b benchutil.FakeObject) int { return strings.Compare(a.Key, b.Key) })
	}
	files := make([]map[string]any, 0, numChunks)
	baseChunkSize := cfg.Objects / numChunks
	remainder := cfg.Objects % numChunks
	chunkBuf := make([]benchutil.FakeObject, 0, baseChunkSize+1)
	for chunk := range numChunks {
		// Spread the remainder across the first `remainder` chunks so
		// chunks differ by at most 1 object.
		thisChunkSize := baseChunkSize
		if chunk < remainder {
			thisChunkSize++
		}
		chunkBuf = chunkBuf[:0]
		for range thisChunkSize {
			if sorted != nil {
				chunkBuf = append(chunkBuf, sorted[0])
				sorted = sorted[1:]

				continue
			}
			chunkBuf = append(chunkBuf, gen.Next())
		}
		payload, err := encodeCSVGz(s3cfg.SrcBucket, seed, chunkBuf)
		if err != nil {
			return InventoryInfo{}, fmt.Errorf("encode chunk %d: %w", chunk, err)
		}
		dataKey := fmt.Sprintf("%s%s/%s/data/%s-chunk-%04d.csv.gz", s3cfg.Prefix, s3cfg.SrcBucket, invID, runStampUUID(runStamp, index), chunk)
		if err := put(ctx, dataKey, payload.Body, "application/gzip"); err != nil {
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
		"fileSchema":        csvFileSchema,
		"files":             files,
	}
	manifestJSON, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return InventoryInfo{}, fmt.Errorf("marshal manifest: %w", err)
	}
	if err := put(ctx, manifestKey, manifestJSON, "application/json"); err != nil {
		return InventoryInfo{}, fmt.Errorf("upload manifest: %w", err)
	}
	if err := put(ctx, checksumKey, []byte(md5Hex(manifestJSON)), "text/plain"); err != nil {
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

// csvFileSchema is the manifest's fileSchema for the seeded CSV: a
// subset of the AWS S3 Inventory fields, in the order AWS writes them.
const csvFileSchema = "Bucket, Key, Size, LastModifiedDate, ETag, StorageClass, IntelligentTieringAccessTier"

// csvFlushBytes is how much encoded CSV encodeCSVGz gathers before
// handing it to the gzip writer.
const csvFlushBytes = 64 << 10

// encodeCSVGz writes synthetic objects as a gzipped CSV body matching
// csvFileSchema, the way AWS S3 Inventory writes CSV: no header row,
// every field quoted, keys URL-encoded. Each object's ETag and
// LastModifiedDate derive from seed and its key (objectMetadata).
func encodeCSVGz(srcBucket string, seed int64, objects []benchutil.FakeObject) (csvGzPayload, error) {
	var out bytes.Buffer
	gz := gzip.NewWriter(&out)
	mapping := tiers.NewMapping()

	var rows bytes.Buffer
	for _, o := range objects {
		cols, err := tierToS3Columns(mapping, o.TierID)
		if err != nil {
			return csvGzPayload{}, fmt.Errorf("tier columns: %w", err)
		}
		meta := objectMetadata(seed, o.Key, o.Size)
		appendQuotedRow(&rows,
			srcBucket,
			csvKey(o.Key),
			strconv.FormatUint(o.Size, 10),
			meta.LastModified,
			meta.ETag,
			cols.StorageClass,
			cols.AccessTier,
		)
		if rows.Len() >= csvFlushBytes {
			if _, err := gz.Write(rows.Bytes()); err != nil {
				return csvGzPayload{}, fmt.Errorf("write csv rows: %w", err)
			}
			rows.Reset()
		}
	}
	if _, err := gz.Write(rows.Bytes()); err != nil {
		return csvGzPayload{}, fmt.Errorf("write csv rows: %w", err)
	}
	if err := gz.Close(); err != nil {
		return csvGzPayload{}, fmt.Errorf("close gzip: %w", err)
	}

	body := out.Bytes()

	return csvGzPayload{Body: body, Size: int64(len(body))}, nil
}

// appendQuotedRow appends one CSV record with every field quoted, empty
// ones included, as AWS S3 Inventory writes them.
func appendQuotedRow(buf *bytes.Buffer, fields ...string) {
	for i, f := range fields {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteByte('"')
		if strings.Contains(f, `"`) {
			f = strings.ReplaceAll(f, `"`, `""`)
		}
		buf.WriteString(f)
		buf.WriteByte('"')
	}
	buf.WriteByte('\n')
}

// The synthetic LastModifiedDate falls in a fixed window, so a fixture's
// bytes don't depend on when it was seeded: up to four years from
// 2021-01-01T00:00:00Z.
const (
	lastModifiedEpoch = 1_609_459_200
	lastModifiedSpan  = 4 * 365 * 24 * 60 * 60
)

// lastModifiedLayout is how AWS S3 Inventory writes LastModifiedDate in
// CSV: UTC with milliseconds, which are always zero.
const lastModifiedLayout = "2006-01-02T15:04:05.000Z"

// multipartPartSize is the AWS CLI's default multipart threshold and
// part size: a synthetic object above it gets a multipart ETag,
// "<hex>-<parts>".
const multipartPartSize = 8 << 20

// objectColumns holds one object's ETag and LastModifiedDate columns.
type objectColumns struct {
	ETag         string
	LastModified string
}

// objectMetadata returns one object's ETag and LastModifiedDate: a
// random-looking 128-bit ETag and a time to the second spread over the
// window, so the gzip ratio and inflate cost resemble a real
// inventory's. Both come from seed and key alone, so every seeding
// writes the same bytes whatever the row order.
func objectMetadata(seed int64, key string, size uint64) objectColumns {
	h := fnv.New64a()
	h.Write([]byte(key))
	rng := rand.New(rand.NewPCG(uint64(seed), h.Sum64()))

	var sum [16]byte
	binary.BigEndian.PutUint64(sum[:8], rng.Uint64())
	binary.BigEndian.PutUint64(sum[8:], rng.Uint64())
	etag := hex.EncodeToString(sum[:])
	if size > multipartPartSize {
		etag += "-" + strconv.FormatUint((size+multipartPartSize-1)/multipartPartSize, 10)
	}

	return objectColumns{
		ETag:         etag,
		LastModified: time.Unix(lastModifiedEpoch+rng.Int64N(lastModifiedSpan), 0).UTC().Format(lastModifiedLayout),
	}
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
func tierToS3Columns(m *tiers.Mapping, id tiers.ID) (S3InventoryColumns, error) {
	switch id {
	case tiers.ITFrequent, tiers.ITFrequentSmall:
		// Small IT_FREQUENT objects round-trip through the same S3 columns;
		// the size-aware classifier on the read side reclassifies them.
		return S3InventoryColumns{StorageClass: storageClassIntelligentTiering, AccessTier: "FREQUENT_ACCESS"}, nil
	case tiers.ITInfrequent:
		return S3InventoryColumns{StorageClass: storageClassIntelligentTiering, AccessTier: "INFREQUENT_ACCESS"}, nil
	case tiers.ITArchiveInstant:
		return S3InventoryColumns{StorageClass: storageClassIntelligentTiering, AccessTier: "ARCHIVE_INSTANT_ACCESS"}, nil
	case tiers.ITArchive:
		return S3InventoryColumns{StorageClass: storageClassIntelligentTiering, AccessTier: "ARCHIVE_ACCESS"}, nil
	case tiers.ITDeepArchive:
		return S3InventoryColumns{StorageClass: storageClassIntelligentTiering, AccessTier: "DEEP_ARCHIVE_ACCESS"}, nil
	case tiers.Standard, tiers.StandardIA, tiers.OneZoneIA,
		tiers.GlacierIR, tiers.GlacierFR, tiers.DeepArchive,
		tiers.ReducedRedundancy, tiers.NumTiers:
		// Plain S3 storage classes (no access-tier column). NumTiers is
		// the sentinel and shouldn't appear at this layer, but handle it
		// defensively rather than letting exhaustive fall through.
	}

	info, ok := m.ByID(id)
	if !ok {
		return S3InventoryColumns{}, fmt.Errorf("%w: %d", tiers.ErrUnknownTierID, id)
	}

	return S3InventoryColumns{StorageClass: info.Name}, nil
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

// csvKey URL-encodes a key the way S3 Inventory does in CSV reports, so
// seeded CSV inventories exercise the build's decode path. Slashes are
// left readable; the decoder accepts either form.
func csvKey(key string) string {
	return strings.ReplaceAll(url.QueryEscape(key), "%2F", "/")
}
