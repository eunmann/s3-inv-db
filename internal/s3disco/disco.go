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
	"regexp"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
	"github.com/rs/zerolog"
)

// Inventory represents one discovered S3 Inventory run. The Discoverer
// emits one Inventory per (SourceBucket, InventoryID, Run) so the UI
// can show every snapshot, not just the most recent. Group by the
// (SourceBucket, InventoryID) pair to render runs under the same
// inventory configuration.
type Inventory struct {
	// SourceBucket is the bucket the inventory is *describing* (the layer
	// of the S3 prefix that S3 inserts between the dest-prefix and the
	// inventory-id).
	SourceBucket string `json:"source_bucket"`

	// InventoryID is the inventory configuration ID.
	InventoryID string `json:"inventory_id"`

	// Run is the timestamp folder name (e.g., "2026-05-13T03-02Z"). Empty
	// when the configuration has been discovered but has no completed
	// runs yet — in that case the entry is returned as a placeholder so
	// the UI can surface "no runs yet".
	Run string `json:"run"`

	// ManifestKey is the S3 key of this run's manifest.json. Empty when
	// Run is empty.
	ManifestKey string `json:"manifest_key"`

	// FileFormat reported by the manifest ("CSV", "Parquet").
	FileFormat string `json:"file_format,omitempty"`

	// FileCount is the number of data files referenced by the manifest.
	// A coarse "size" signal for the UI before download.
	FileCount int `json:"file_count,omitempty"`

	// CreationTimestamp is the manifest's reported creation time
	// (UnixMilli as a decimal string, exactly as S3 writes it).
	CreationTimestamp string `json:"creation_timestamp,omitempty"`

	// Error captures a non-fatal per-run failure (e.g. unreadable
	// manifest). Empty on success.
	Error string `json:"error,omitempty"`
}

// CompositeID returns "<src-bucket>/<inv-id>/<run>" — the unique
// identifier we surface through the HTTP API and use as the inventory
// Manager key. URL-encoding is the caller's job. When Run is empty
// (no completed runs for the configuration), only "<src>/<inv>" is
// returned and the entry is not independently loadable.
func (i Inventory) CompositeID() string {
	if i.Run == "" {
		return i.SourceBucket + "/" + i.InventoryID
	}
	return i.SourceBucket + "/" + i.InventoryID + "/" + i.Run
}

// ConfigID returns "<src-bucket>/<inv-id>" — the identifier of the
// inventory configuration, shared across all runs. Used by the UI to
// group runs.
func (i Inventory) ConfigID() string {
	return i.SourceBucket + "/" + i.InventoryID
}

// Discoverer wraps an S3 client + bucket/prefix root and lists inventories.
// Per-call logging uses zerolog.Ctx(ctx) so the request-scoped
// logger (with request_id) attaches automatically.
type Discoverer struct {
	client *s3.Client
	bucket string
	prefix string // ends with "/" when non-empty
}

// New constructs a Discoverer from an s3.Client and a parsed bucket/prefix.
// Prefix may be empty or end with "/"; callers should normalize.
func New(client *s3.Client, bucket, prefix string) *Discoverer {
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return &Discoverer{client: client, bucket: bucket, prefix: prefix}
}

// runFolderRE matches the two timestamp folder shapes S3 Inventory uses
// (we also accept second-granularity output from our seeder). Any other
// directory under the inventory prefix (e.g. data/) is filtered out so it
// can't be misread as a run.
var runFolderRE = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}-\d{2}(-\d{2})?Z$`)

// NewFromS3URI builds a Discoverer from an s3:// URI like
// "s3://bucket/optional/prefix/".
func NewFromS3URI(client *s3.Client, uri string) (*Discoverer, error) {
	bucket, key, err := s3fetch.ParseS3URI(uri)
	if err != nil {
		return nil, fmt.Errorf("parse source URI %q: %w", uri, err)
	}
	return New(client, bucket, key), nil
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
func (d *Discoverer) List(ctx context.Context) ([]Inventory, error) {
	srcBuckets, err := d.listCommonPrefixes(ctx, d.prefix)
	if err != nil {
		return nil, fmt.Errorf("list source buckets: %w", err)
	}

	logger := zerolog.Ctx(ctx)
	var out []Inventory
	for _, src := range srcBuckets {
		srcName := trimPrefix(src, d.prefix)
		invs, err := d.listCommonPrefixes(ctx, src)
		if err != nil {
			logger.Warn().Err(err).Str("src", srcName).Msg("list inventories under src")
			out = append(out, Inventory{SourceBucket: srcName, Error: "failed to list inventories under source bucket"})
			continue
		}
		for _, inv := range invs {
			invName := trimPrefix(inv, src)
			runs, err := d.describeRuns(ctx, srcName, invName, inv)
			if err != nil {
				logger.Warn().Err(err).Str("src", srcName).Str("inv", invName).Msg("describe runs")
				out = append(out, Inventory{SourceBucket: srcName, InventoryID: invName, Error: "failed to describe inventory"})
				continue
			}
			out = append(out, runs...)
		}
	}
	return out, nil
}

// Find returns one Inventory by (srcBucket, invID, run). When run is
// empty, returns the most recent run for the configuration — preserves
// the old caller contract for code paths that just want "the latest".
func (d *Discoverer) Find(ctx context.Context, srcBucket, invID, run string) (Inventory, error) {
	if srcBucket == "" || invID == "" {
		return Inventory{}, errEmptyID
	}
	invPrefix := d.prefix + srcBucket + "/" + invID + "/"
	runs, err := d.describeRuns(ctx, srcBucket, invID, invPrefix)
	if err != nil {
		return Inventory{}, err
	}
	if len(runs) == 0 {
		return Inventory{SourceBucket: srcBucket, InventoryID: invID}, nil
	}
	if run == "" {
		// Newest first — caller wants "the latest".
		return runs[0], nil
	}
	for i := range runs {
		if runs[i].Run == run {
			return runs[i], nil
		}
	}
	return Inventory{}, fmt.Errorf("%w: %s/%s/%s", ErrRunNotFound, srcBucket, invID, run)
}

// ErrRunNotFound is returned by Find when the requested run isn't
// present under the (srcBucket, invID) configuration.
var ErrRunNotFound = errors.New("run not found")

var errEmptyID = errors.New("source bucket and inventory id are required")

// describeRuns walks <prefix><src>/<inv>/ and returns one Inventory per
// run folder, newest-first. Each entry has its own manifest fetched. A
// configuration with no run folders returns a single placeholder.
func (d *Discoverer) describeRuns(ctx context.Context, src, inv, invPrefix string) ([]Inventory, error) {
	folders, err := d.listCommonPrefixes(ctx, invPrefix)
	if err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}

	// Filter to legitimate run folders; sort newest first.
	var runs []string
	for _, r := range folders {
		name := trimPrefix(r, invPrefix)
		if runFolderRE.MatchString(name) {
			runs = append(runs, name)
		}
	}
	if len(runs) == 0 {
		return []Inventory{{SourceBucket: src, InventoryID: inv}}, nil
	}
	sort.Sort(sort.Reverse(sort.StringSlice(runs)))

	out := make([]Inventory, 0, len(runs))
	logger := zerolog.Ctx(ctx)
	for _, name := range runs {
		entry := Inventory{
			SourceBucket: src,
			InventoryID:  inv,
			Run:          name,
			ManifestKey:  invPrefix + name + "/manifest.json",
		}
		manifest, err := d.fetchManifest(ctx, entry.ManifestKey)
		if err != nil {
			logger.Warn().Err(err).Str("src", src).Str("inv", inv).Str("run", name).Msg("fetch manifest")
			entry.FileFormat = "unknown"
			entry.Error = "failed to read manifest"
			out = append(out, entry)
			continue
		}
		entry.FileFormat = manifest.FileFormat
		entry.FileCount = len(manifest.Files)
		entry.CreationTimestamp = manifest.CreationTimestamp
		out = append(out, entry)
	}
	return out, nil
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
