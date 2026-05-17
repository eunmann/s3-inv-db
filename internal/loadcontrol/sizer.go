package loadcontrol

import (
	"context"
	"fmt"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
)

// ManifestSizer is the inventory.ManifestSizer concrete implementation
// backed by an s3fetch.Client. Caches nothing — the manifest is small
// JSON and fetched once per load attempt.
type ManifestSizer struct {
	client *s3fetch.Client
}

// NewManifestSizer returns a ManifestSizer that fetches via client.
func NewManifestSizer(client *s3fetch.Client) *ManifestSizer {
	return &ManifestSizer{client: client}
}

// ManifestSize returns the sum of the manifest's data-file sizes — the
// total compressed bytes the load pipeline will pull from S3. The gate
// multiplies this by indexRatio to estimate final IndexBytes.
func (s *ManifestSizer) ManifestSize(ctx context.Context, bucket, key string) (uint64, error) {
	m, err := s.client.FetchManifest(ctx, bucket, key)
	if err != nil {
		return 0, fmt.Errorf("fetch manifest: %w", err)
	}
	var total uint64
	for _, f := range m.Files {
		if f.Size > 0 {
			total += uint64(f.Size)
		}
	}

	return total, nil
}

// Assert that *ManifestSizer satisfies the inventory.ManifestSizer
// interface — keeps the discovery service's contract honest.
var _ inventory.ManifestSizer = (*ManifestSizer)(nil)
