package tiers

// FromS3Normalized is exported for testing only, so FromS3's fast path
// can be checked against the normalizing path it short-circuits.
func FromS3Normalized(m *Mapping, storageClass, accessTier string) ID {
	return m.fromS3Normalized(storageClass, accessTier)
}

// CanonicalID is exported for testing only.
var CanonicalID = canonicalID
