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
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
)

// Inventory represents one discovered S3 Inventory configuration with its
// most recent run.
type Inventory struct {
	// SourceBucket is the bucket the inventory is *describing* (the layer
	// of the S3 prefix that S3 inserts between the dest-prefix and the
	// inventory-id).
	SourceBucket string `json:"source_bucket"`

	// InventoryID is the inventory configuration ID.
	InventoryID string `json:"inventory_id"`

	// LatestRun is the timestamp folder name of the most recent run
	// (e.g., "2026-05-13T03-02Z"). Empty if the inventory exists but has
	// no completed run yet.
	LatestRun string `json:"latest_run"`

	// ManifestKey is the S3 key of the latest manifest.json relative to
	// DestinationBucket. Empty when LatestRun is empty.
	ManifestKey string `json:"manifest_key"`

	// FileFormat reported by the latest manifest ("CSV", "Parquet").
	FileFormat string `json:"file_format,omitempty"`

	// FileCount is the number of data files referenced by the latest
	// manifest. A coarse "size" signal for the UI before download.
	FileCount int `json:"file_count,omitempty"`

	// CreationTimestamp is the manifest's reported creation time (UnixMilli
	// as a decimal string, exactly as S3 writes it).
	CreationTimestamp string `json:"creation_timestamp,omitempty"`
}

// CompositeID returns "<src-bucket>/<inv-id>" — the unique identifier we
// surface through the HTTP API. URL-encoding is the caller's job.
func (i Inventory) CompositeID() string {
	return i.SourceBucket + "/" + i.InventoryID
}

// Discoverer wraps an S3 client + bucket/prefix root and lists inventories.
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
// discovered <src-bucket>/<inv-id> pair, populated with the latest run.
// Inventories with no completed runs are still returned (LatestRun empty)
// so the UI can show them.
func (d *Discoverer) List(ctx context.Context) ([]Inventory, error) {
	srcBuckets, err := d.listCommonPrefixes(ctx, d.prefix)
	if err != nil {
		return nil, fmt.Errorf("list source buckets: %w", err)
	}

	var out []Inventory
	for _, src := range srcBuckets {
		srcName := trimPrefix(src, d.prefix)
		invs, err := d.listCommonPrefixes(ctx, src)
		if err != nil {
			return nil, fmt.Errorf("list inventories under %s: %w", src, err)
		}
		for _, inv := range invs {
			invName := trimPrefix(inv, src)
			entry, err := d.describeInventory(ctx, srcName, invName, inv)
			if err != nil {
				return nil, fmt.Errorf("describe %s/%s: %w", srcName, invName, err)
			}
			out = append(out, entry)
		}
	}
	return out, nil
}

// Find returns a single Inventory by composite ID, or an error.
func (d *Discoverer) Find(ctx context.Context, srcBucket, invID string) (Inventory, error) {
	if srcBucket == "" || invID == "" {
		return Inventory{}, errEmptyID
	}
	invPrefix := d.prefix + srcBucket + "/" + invID + "/"
	return d.describeInventory(ctx, srcBucket, invID, invPrefix)
}

var errEmptyID = errors.New("source bucket and inventory id are required")

// describeInventory walks <prefix><src>/<inv>/ to find the latest manifest.
func (d *Discoverer) describeInventory(ctx context.Context, src, inv, invPrefix string) (Inventory, error) {
	runs, err := d.listCommonPrefixes(ctx, invPrefix)
	if err != nil {
		return Inventory{}, fmt.Errorf("list runs: %w", err)
	}

	entry := Inventory{SourceBucket: src, InventoryID: inv}

	// Pick the lex-greatest timestamp folder. ISO timestamps sort right.
	// data/ is also a "common prefix" — filter it out by requiring the
	// timestamp shape (must start with a digit).
	latest := ""
	for _, r := range runs {
		name := trimPrefix(r, invPrefix)
		name = strings.TrimSuffix(name, "/")
		if name == "" || (name[0] < '0' || name[0] > '9') {
			continue
		}
		if name > latest {
			latest = name
		}
	}
	if latest == "" {
		return entry, nil
	}
	entry.LatestRun = latest
	entry.ManifestKey = invPrefix + latest + "/manifest.json"

	// Soft-fail on parse error so one broken manifest doesn't break List.
	manifest, err := d.fetchManifest(ctx, entry.ManifestKey)
	if err != nil {
		entry.FileFormat = "unknown"
		return entry, nil //nolint:nilerr // surface a partial entry; List shouldn't fail for one broken manifest
	}
	entry.FileFormat = manifest.FileFormat
	entry.FileCount = len(manifest.Files)
	entry.CreationTimestamp = manifest.CreationTimestamp
	return entry, nil
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
