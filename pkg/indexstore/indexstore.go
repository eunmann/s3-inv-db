// Package indexstore publishes built indexes to S3 and fetches them
// back, so an index can be built in one place and served in another.
//
// A run's index lives under <base>/<source-bucket>/<inventory-id>/<run>/,
// the same layout S3 Inventory uses for its reports. Its manifest.json,
// which lists every file with its size and SHA-256, is written last:
// its presence commits the run, so a reader never sees a partial index,
// and a fetch verifies every file against it before the index is used.
package indexstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/eunmann/s3-inv-db/pkg/format"
)

// ErrNotPublished is returned when a run has no committed index: no
// manifest.json under its prefix.
var ErrNotPublished = errors.New("no published index")

// ErrNotRunManifest is returned for an inventory manifest key that
// isn't <source-bucket>/<inventory-id>/<run>/manifest.json.
var ErrNotRunManifest = errors.New("not a run's manifest key")

// ErrBadManifest is returned for a manifest that names a file outside
// the run's directory or none at all.
var ErrBadManifest = errors.New("invalid index manifest")

// Run identifies one inventory run.
type Run struct {
	Source    string // source bucket
	Inventory string // inventory configuration ID
	Run       string // run timestamp folder, YYYY-MM-DDTHH-MMZ
}

// RunFromManifestKey returns the run an S3 Inventory manifest key names:
// .../<source-bucket>/<inventory-id>/<run>/manifest.json.
func RunFromManifestKey(key string) (Run, error) {
	parts := strings.Split(strings.Trim(key, "/"), "/")
	if len(parts) < 4 || parts[len(parts)-1] != format.ManifestFile {
		return Run{}, fmt.Errorf("%w: %q is not <source-bucket>/<inventory-id>/<run>/manifest.json", ErrNotRunManifest, key)
	}
	n := len(parts)
	r := Run{Source: parts[n-4], Inventory: parts[n-3], Run: parts[n-2]}
	for _, seg := range []string{r.Source, r.Inventory, r.Run} {
		if seg == "" || seg == "." || seg == ".." {
			return Run{}, fmt.Errorf("%w: %q has an empty or relative path segment", ErrNotRunManifest, key)
		}
	}

	return r, nil
}

// Prefix returns the run's key prefix under base, ending in a slash.
func (r Run) Prefix(base string) string {
	base = strings.TrimSuffix(base, "/")
	if base != "" {
		base += "/"
	}

	return base + r.Source + "/" + r.Inventory + "/" + r.Run + "/"
}

// Store reads and writes published indexes in one S3 bucket.
type Store struct {
	client *s3.Client
	tm     *transfermanager.Client
	bucket string
}

// New returns a Store for bucket.
func New(client *s3.Client, bucket string) *Store {
	return &Store{client: client, tm: transfermanager.New(client), bucket: bucket}
}

// Bucket is the store's bucket.
func (s *Store) Bucket() string { return s.bucket }

// Publish uploads the index in dir under prefix: every file its
// manifest lists, then the manifest. A manifest already at prefix is
// deleted first, so a reader never sees the old manifest over new
// files; until the new one lands the run reads as unpublished.
func (s *Store) Publish(ctx context.Context, dir, prefix string) error {
	m, err := format.ReadManifest(dir)
	if err != nil {
		return fmt.Errorf("read index manifest: %w", err)
	}
	if err := checkNames(m); err != nil {
		return err
	}
	if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(prefix + format.ManifestFile),
	}); err != nil {
		return fmt.Errorf("unpublish previous index at s3://%s/%s: %w", s.bucket, prefix, err)
	}
	for _, name := range sortedNames(m) {
		if err := s.upload(ctx, filepath.Join(dir, filepath.FromSlash(name)), prefix+name); err != nil {
			return err
		}
	}

	return s.upload(ctx, filepath.Join(dir, format.ManifestFile), prefix+format.ManifestFile)
}

func (s *Store) upload(ctx context.Context, local, key string) error {
	f, err := os.Open(local)
	if err != nil {
		return fmt.Errorf("open %s: %w", local, err)
	}
	defer f.Close()
	if _, err := s.tm.UploadObject(ctx, &transfermanager.UploadObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key), Body: f,
	}); err != nil {
		return fmt.Errorf("upload s3://%s/%s: %w", s.bucket, key, err)
	}

	return nil
}

// ReadManifest returns the committed manifest under prefix, or
// ErrNotPublished when there is none.
func (s *Store) ReadManifest(ctx context.Context, prefix string) (*format.Manifest, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(prefix + format.ManifestFile),
	})
	if err != nil {
		if _, ok := errors.AsType[*types.NoSuchKey](err); ok {
			return nil, fmt.Errorf("%w at s3://%s/%s", ErrNotPublished, s.bucket, prefix)
		}

		return nil, fmt.Errorf("read s3://%s/%s%s: %w", s.bucket, prefix, format.ManifestFile, err)
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("read s3://%s/%s%s: %w", s.bucket, prefix, format.ManifestFile, err)
	}

	return ParseManifest(data)
}

// ParseManifest decodes an index manifest and checks its file names.
func ParseManifest(data []byte) (*format.Manifest, error) {
	var m format.Manifest
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBadManifest, err)
	}
	if err := checkNames(&m); err != nil {
		return nil, err
	}

	return &m, nil
}

// Fetch downloads the committed index under prefix into dir, which
// must be empty or not exist yet, and verifies every file against the
// manifest. On any error dir is removed, so it holds a verified index
// or nothing. The onFile callback, if non-nil, gets the bytes fetched
// so far and the total after each file.
func (s *Store) Fetch(ctx context.Context, prefix, dir string, onFile func(done, total uint64)) (err error) {
	m, err := s.ReadManifest(ctx, prefix)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, format.DirPerm); err != nil {
		return fmt.Errorf("create %s: %w", dir, err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, os.RemoveAll(dir))
		}
	}()
	total, done := m.TotalBytes(), uint64(0)
	for _, name := range sortedNames(m) {
		if err := s.download(ctx, prefix+name, filepath.Join(dir, filepath.FromSlash(name))); err != nil {
			return err
		}
		done += uint64(m.Files[name].Size)
		if onFile != nil {
			onFile(done, total)
		}
	}
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("encode manifest: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, format.ManifestFile), data, format.FilePerm); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}
	if err := format.VerifyManifest(dir, m); err != nil {
		return fmt.Errorf("verify s3://%s/%s: %w", s.bucket, prefix, err)
	}

	return nil
}

func (s *Store) download(ctx context.Context, key, local string) error {
	if err := os.MkdirAll(filepath.Dir(local), format.DirPerm); err != nil {
		return fmt.Errorf("create %s: %w", filepath.Dir(local), err)
	}
	f, err := os.Create(local)
	if err != nil {
		return fmt.Errorf("create %s: %w", local, err)
	}
	if _, err := s.tm.DownloadObject(ctx, &transfermanager.DownloadObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key), WriterAt: f,
	}); err != nil {
		return errors.Join(fmt.Errorf("download s3://%s/%s: %w", s.bucket, key, err), f.Close())
	}

	return f.Close() //nolint:wrapcheck // the path is in the caller's context
}

// checkNames refuses a manifest whose file names would land outside the
// run's directory: it comes from S3, so it is input, not trusted.
func checkNames(m *format.Manifest) error {
	if len(m.Files) == 0 {
		return fmt.Errorf("%w: no files", ErrBadManifest)
	}
	for name := range m.Files {
		clean := path.Clean(name)
		if name == "" || clean != name || path.IsAbs(name) || clean == ".." || strings.HasPrefix(clean, "../") ||
			strings.Contains(name, `\`) || name == format.ManifestFile {
			return fmt.Errorf("%w: file %q", ErrBadManifest, name)
		}
	}

	return nil
}

func sortedNames(m *format.Manifest) []string {
	names := make([]string, 0, len(m.Files))
	for name := range m.Files {
		names = append(names, name)
	}
	slices.Sort(names)

	return names
}
