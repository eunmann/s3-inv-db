package format

// On-disk filenames for an index directory. Single source of truth so
// builders and readers stay in sync; manifest verification also reads
// from this list.
const (
	DepthOffsetsFile   = "depth_offsets.u64"
	DepthPositionsFile = "depth_positions.u64"
	MPHFile            = "mph.bin"
	ManifestFile       = "manifest.json"
	TierStatsDir       = "tier_stats"
	DepthBucketNameFmt = "depth_%02d"
)
