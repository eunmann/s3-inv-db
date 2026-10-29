package tiers

// FromS3Normalized is exported for testing only, so FromS3's fast path
// can be checked against the normalizing path it short-circuits.
var FromS3Normalized = fromS3Normalized

// CanonicalID is exported for testing only.
var CanonicalID = canonicalID
