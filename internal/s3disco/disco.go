// Package s3disco discovers AWS S3 Inventory layouts under a configured
// bucket+prefix and surfaces the latest manifest for each inventory.
//
// AWS S3 writes inventories under
//
//	<bucket>/<dest-prefix>/<src-bucket>/<inv-id>/<YYYY-MM-DDTHH-MMZ>/manifest.json
//	<bucket>/<dest-prefix>/<src-bucket>/<inv-id>/<YYYY-MM-DDTHH-MMZ>/manifest.checksum
//	<bucket>/<dest-prefix>/<src-bucket>/<inv-id>/data/<uuid>.<ext>
//
// There is no "latest" pointer — consumers list the timestamp folders and
// pick the lexicographically greatest one (ISO timestamps sort right).
package s3disco

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/indexstore"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
	"github.com/rs/zerolog"
)

// Discoverer wraps an S3 client + bucket/prefix root and lists inventories.
type Discoverer struct {
	client          *s3.Client
	bucket          string
	prefix          string
	manifestFetches int
	// indexes is set when the root holds published indexes
	// (pkg/indexstore) rather than inventory reports: each run's
	// manifest.json is an index manifest.
	indexes bool

	// manifests holds the summary of every inventory manifest read so
	// far, by key. S3 Inventory writes a run's manifest.json once, when
	// the run completes, so a summary never goes stale; only successful
	// reads are kept, and List drops keys whose run is gone. Index
	// manifests are not cached: republishing a run rewrites its
	// manifest at the same key, and deletes it while the new files
	// upload.
	manifestsMu sync.Mutex
	manifests   map[string]manifestSummary
}

// manifestSummary is what discovery shows of one manifest.
type manifestSummary struct {
	fileFormat        string
	fileCount         int
	totalBytes        int64
	creationTimestamp string
}

// listConcurrency bounds the configurations List describes at once.
const listConcurrency = 8

// DefaultManifestFetches is the per-configuration cap on how many of
// the newest run manifests are fetched per discovery pass. Older runs
// surface with only their run timestamp.
const DefaultManifestFetches = 10

// New constructs a Discoverer from an s3.Client and a parsed bucket/prefix.
// Prefix may be empty or end with "/"; callers should normalize.
func New(client *s3.Client, bucket, prefix string) *Discoverer {
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	return &Discoverer{
		client: client, bucket: bucket, prefix: prefix, manifestFetches: DefaultManifestFetches,
		manifests: make(map[string]manifestSummary),
	}
}

// runFolderRE matches the two timestamp folder shapes S3 Inventory uses
// (we also accept second-granularity output from our seeder). Any other
// directory under the inventory prefix (e.g. data/) is filtered out so it
// can't be misread as a run.
var runFolderRE = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}-\d{2}(-\d{2})?Z$`)

// Source and inventory names come verbatim from S3 key segments that
// anyone with write access to the inventory bucket controls. They end
// up as cache-directory path segments and inside htmx attributes (where
// "[...]" in hx-trigger is evaluated as JavaScript), so only the
// charsets AWS itself allows are accepted: S3 bucket naming for the
// source and the inventory-configuration ID charset for the inventory.
var (
	sourceNameRE  = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
	inventoryIDRE = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
)

func validSourceName(name string) bool { return sourceNameRE.MatchString(name) }

// validInventoryID additionally refuses "." and "..", which the AWS
// charset admits but which are path navigation, not names.
func validInventoryID(id string) bool {
	return id != "." && id != ".." && inventoryIDRE.MatchString(id)
}

// NewFromS3URI builds a Discoverer from an s3:// URI like
// "s3://bucket/optional/prefix/".
func NewFromS3URI(client *s3.Client, uri string) (*Discoverer, error) {
	parsed, err := s3fetch.ParseS3URI(uri)
	if err != nil {
		return nil, fmt.Errorf("parse source URI %q: %w", uri, err)
	}

	return New(client, parsed.Bucket, parsed.Key), nil
}

// ListIndexes makes the discoverer read each run's manifest.json as a
// published index's manifest (pkg/indexstore) instead of an inventory
// report's: the root is an index source, laid out like an inventory
// destination. A run shows its index size; one without a manifest isn't
// committed yet.
func (d *Discoverer) ListIndexes() *Discoverer {
	d.indexes = true

	return d
}

// Bucket returns the destination bucket name being discovered.
func (d *Discoverer) Bucket() string { return d.bucket }

// Prefix returns the destination-prefix being discovered (trailing slash).
func (d *Discoverer) Prefix() string { return d.prefix }

// List walks the configured root and returns one Inventory entry per
// (<src-bucket>, <inv-id>, <run>) triple — every run S3 has published.
// Configurations with no completed runs still appear as a single
// placeholder entry (Run empty) so the UI can show "no runs yet".
// Runs come back newest-first within each configuration so the UI can
// surface the most recent at the top of each group without re-sorting.
func (d *Discoverer) List(ctx context.Context) ([]inventory.Inventory, error) {
	srcBuckets, err := d.listCommonPrefixes(ctx, d.prefix)
	if err != nil {
		return nil, fmt.Errorf("list source buckets: %w", err)
	}

	logger := zerolog.Ctx(ctx)
	// One slot per source or configuration, in walk order; the
	// configurations are described concurrently and fill their slots.
	var slots []*[]inventory.Inventory
	var partial atomic.Bool
	var wg sync.WaitGroup
	sem := make(chan struct{}, listConcurrency)
	for _, src := range srcBuckets {
		srcName := trimPrefix(src, d.prefix)
		if !validSourceName(srcName) {
			logger.Warn().Str("src", srcName).Msg("skip source with invalid S3 bucket name")

			continue
		}
		invs, err := d.listCommonPrefixes(ctx, src)
		if err != nil {
			logger.Warn().Err(err).Str("src", srcName).Msg("list inventories under src")
			slots = append(slots, &[]inventory.Inventory{{SourceBucket: srcName, Error: "failed to list inventories under source bucket"}})
			partial.Store(true)

			continue
		}
		for _, inv := range invs {
			invName := trimPrefix(inv, src)
			if !validInventoryID(invName) {
				logger.Warn().Str("src", srcName).Str("inv", invName).Msg("skip inventory with invalid configuration ID")

				continue
			}
			slot := new([]inventory.Inventory)
			slots = append(slots, slot)
			sem <- struct{}{}
			wg.Go(func() {
				defer func() { <-sem }()
				runs, err := d.describeRuns(ctx, srcName, invName, inv)
				if err != nil {
					logger.Warn().Err(err).Str("src", srcName).Str("inv", invName).Msg("describe runs")
					runs = []inventory.Inventory{{SourceBucket: srcName, Name: invName, Error: "failed to describe inventory"}}
					partial.Store(true)
				}
				*slot = runs
			})
		}
	}
	wg.Wait()

	var out []inventory.Inventory
	for _, runs := range slots {
		out = append(out, *runs...)
	}
	if !partial.Load() {
		d.keepManifests(out)
	}

	return out, nil
}

// keepManifests drops cached summaries of runs a complete listing no
// longer shows. List skips it when a source or configuration failed to
// list, so a transient error doesn't cost those runs' manifests again.
func (d *Discoverer) keepManifests(listed []inventory.Inventory) {
	keep := make(map[string]struct{}, len(listed))
	for i := range listed {
		if listed[i].ManifestKey != "" {
			keep[listed[i].ManifestKey] = struct{}{}
		}
	}
	d.manifestsMu.Lock()
	defer d.manifestsMu.Unlock()
	for key := range d.manifests {
		if _, ok := keep[key]; !ok {
			delete(d.manifests, key)
		}
	}
}

// Find returns one Inventory by (srcBucket, invID, run) with at most
// one manifest GET, none when an inventory run's manifest was already
// read; an
// empty run costs one extra LIST to pick the most recent run for the
// configuration.
func (d *Discoverer) Find(ctx context.Context, srcBucket, invID, run string) (inventory.Inventory, error) {
	if srcBucket == "" || invID == "" {
		return inventory.Inventory{}, errEmptyID
	}
	if !validSourceName(srcBucket) || !validInventoryID(invID) {
		return inventory.Inventory{}, fmt.Errorf("%w: %q/%q", inventory.ErrInvalidName, srcBucket, invID)
	}
	invPrefix := d.prefix + srcBucket + "/" + invID + "/"
	switch {
	case run == "":
		runs, err := d.listRuns(ctx, invPrefix)
		if err != nil {
			return inventory.Inventory{}, err
		}
		if len(runs) == 0 {
			return inventory.Inventory{SourceBucket: srcBucket, Name: invID}, nil
		}
		run = runs[0]
	case !runFolderRE.MatchString(run):
		return inventory.Inventory{}, fmt.Errorf("%w: %s/%s/%s", inventory.ErrRunNotFound, srcBucket, invID, run)
	}
	entry := runEntry(srcBucket, invID, invPrefix, run)
	err := d.describeRun(ctx, &entry)
	if nsk := (*types.NoSuchKey)(nil); errors.As(err, &nsk) {
		return inventory.Inventory{}, fmt.Errorf("%w: %s/%s/%s", inventory.ErrRunNotFound, srcBucket, invID, run)
	}

	return entry, nil
}

var errEmptyID = errors.New("source bucket and inventory id are required")

// describeRuns walks <prefix><src>/<inv>/ and returns one Inventory per
// run folder, newest-first. The newest manifestFetches runs have their
// manifest fetched. A configuration with no run folders returns a
// single placeholder.
func (d *Discoverer) describeRuns(ctx context.Context, src, inv, invPrefix string) ([]inventory.Inventory, error) {
	runs, err := d.listRuns(ctx, invPrefix)
	if err != nil {
		return nil, err
	}
	if len(runs) == 0 {
		return []inventory.Inventory{{SourceBucket: src, Name: inv}}, nil
	}

	out := make([]inventory.Inventory, 0, len(runs))
	limit := d.manifestFetches
	for i, name := range runs {
		entry := runEntry(src, inv, invPrefix, name)
		if limit <= 0 || i < limit {
			err := d.describeRun(ctx, &entry)
			// An index run without its manifest is still uploading (the
			// manifest is written last): not a run yet.
			if _, missing := errors.AsType[*types.NoSuchKey](err); missing && d.indexes {
				continue
			}
		}
		out = append(out, entry)
	}

	return out, nil
}

// listRuns returns the run folder names under invPrefix, newest first.
// Other directories (e.g. data/) are filtered out so they can't be
// misread as runs.
func (d *Discoverer) listRuns(ctx context.Context, invPrefix string) ([]string, error) {
	folders, err := d.listCommonPrefixes(ctx, invPrefix)
	if err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	var runs []string
	for _, r := range folders {
		name := trimPrefix(r, invPrefix)
		if runFolderRE.MatchString(name) {
			runs = append(runs, name)
		}
	}
	slices.SortFunc(runs, func(a, b string) int { return strings.Compare(b, a) })

	return runs, nil
}

func runEntry(src, inv, invPrefix, run string) inventory.Inventory {
	return inventory.Inventory{
		SourceBucket: src,
		Name:         inv,
		Run:          run,
		ManifestKey:  invPrefix + run + "/manifest.json",
	}
}

// describeRun fills entry's manifest stats. A fetch failure is logged
// and recorded on the entry so the UI shows it; the error is returned
// so callers can tell a missing run apart.
func (d *Discoverer) describeRun(ctx context.Context, entry *inventory.Inventory) error {
	if !d.indexes {
		d.manifestsMu.Lock()
		sum, ok := d.manifests[entry.ManifestKey]
		d.manifestsMu.Unlock()
		if ok {
			sum.fill(entry)

			return nil
		}
	}
	sum, err := d.summarize(ctx, entry.ManifestKey)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).
			Str("src", entry.SourceBucket).Str("inv", entry.Name).Str("run", entry.Run).
			Msg("fetch manifest")
		entry.FileFormat = "unknown"
		entry.Error = "failed to read manifest"

		return err
	}
	if !d.indexes {
		d.manifestsMu.Lock()
		d.manifests[entry.ManifestKey] = sum
		d.manifestsMu.Unlock()
	}
	sum.fill(entry)

	return nil
}

// summarize reads the manifest at key: an inventory report's, or a
// published index's when the discoverer lists indexes.
func (d *Discoverer) summarize(ctx context.Context, key string) (manifestSummary, error) {
	if d.indexes {
		return d.indexSummary(ctx, key)
	}
	manifest, err := d.fetchManifest(ctx, key)
	if err != nil {
		return manifestSummary{}, err
	}

	return manifestSummary{
		fileFormat:        manifest.FileFormat,
		fileCount:         len(manifest.Files),
		totalBytes:        manifestTotalBytes(manifest),
		creationTimestamp: manifest.CreationTimestamp,
	}, nil
}

// indexFormat is the file format discovery shows for a published index.
const indexFormat = "index"

// indexSummary reads a published index's manifest: its file count and
// on-disk size, which is what loading it costs.
func (d *Discoverer) indexSummary(ctx context.Context, key string) (manifestSummary, error) {
	resp, err := d.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(d.bucket), Key: aws.String(key)})
	if err != nil {
		return manifestSummary{}, fmt.Errorf("get s3://%s/%s: %w", d.bucket, key, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return manifestSummary{}, fmt.Errorf("read s3://%s/%s: %w", d.bucket, key, err)
	}
	m, err := indexstore.ParseManifest(data)
	if err != nil {
		return manifestSummary{}, fmt.Errorf("parse s3://%s/%s: %w", d.bucket, key, err)
	}

	return manifestSummary{
		fileFormat:        indexFormat,
		fileCount:         len(m.Files),
		totalBytes:        int64(m.TotalBytes()),
		creationTimestamp: m.CreatedAt.UTC().Format(time.RFC3339),
	}, nil
}

func (s manifestSummary) fill(entry *inventory.Inventory) {
	entry.FileFormat = s.fileFormat
	entry.FileCount = s.fileCount
	entry.TotalBytes = s.totalBytes
	entry.CreationTimestamp = s.creationTimestamp
}

// manifestTotalBytes sums the compressed sizes of every data file in
// the manifest. AWS records the size as int64 already; we keep the
// same width so big inventories don't roll over.
func manifestTotalBytes(m *s3fetch.Manifest) int64 {
	var total int64
	for _, f := range m.Files {
		total += f.Size
	}

	return total
}

// fetchManifest GETs and parses a manifest.json using the discoverer's s3
// client — same endpoint, credentials, and path-style settings as List.
func (d *Discoverer) fetchManifest(ctx context.Context, key string) (*s3fetch.Manifest, error) {
	resp, err := d.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(d.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("get s3://%s/%s: %w", d.bucket, key, err)
	}
	defer resp.Body.Close()
	manifest, err := s3fetch.ParseManifest(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("parse manifest s3://%s/%s: %w", d.bucket, key, err)
	}

	return manifest, nil
}

// listCommonPrefixes does a single ListObjectsV2 with delimiter=/ to return
// immediate subdirectories under prefix (each ending in "/").
func (d *Discoverer) listCommonPrefixes(ctx context.Context, prefix string) ([]string, error) {
	var out []string
	paginator := s3.NewListObjectsV2Paginator(d.client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(d.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list s3://%s/%s: %w", d.bucket, prefix, err)
		}
		for _, cp := range page.CommonPrefixes {
			if cp.Prefix != nil {
				out = append(out, *cp.Prefix)
			}
		}
	}

	return out, nil
}

func trimPrefix(s, prefix string) string {
	return strings.TrimSuffix(strings.TrimPrefix(s, prefix), "/")
}
