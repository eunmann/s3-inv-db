// Package benchutil provides synthetic data generation for benchmarks and testing.
package benchutil

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// defaultBenchSeed is the deterministic seed used when callers don't
// specify one. Stable so benchmark runs reproduce.
const defaultBenchSeed int64 = 42

// Default tier mix probabilities used by DefaultConfig. Together they
// sum to 1.0.
const (
	defaultProbStandard   = 0.60
	defaultProbStandardIA = 0.15
	defaultProbGlacierIR  = 0.10
	defaultProbITFrequent = 0.10
	defaultProbITArchive  = 0.05
)

// S3-realistic tier mix probabilities used by S3RealisticConfig. All
// 11 storage classes populated, modelling a typical enterprise bucket
// that uses both legacy lifecycle tiers (Standard / IA / Glacier IR /
// Glacier FR / Deep Archive) and the Intelligent-Tiering family
// (split across its access sub-tiers, plus the small-object subset
// which is billed at the Frequent rate). Probabilities sum to 1.0.
const (
	s3ProbStandard         = 0.25
	s3ProbStandardIA       = 0.15
	s3ProbGlacierIR        = 0.07
	s3ProbGlacierFR        = 0.05
	s3ProbDeepArchive      = 0.08
	s3ProbITFrequent       = 0.12
	s3ProbITInfrequent     = 0.10
	s3ProbITArchiveInstant = 0.06
	s3ProbITArchive        = 0.06
	s3ProbITDeepArchive    = 0.04
	s3ProbITFrequentSmall  = 0.02
)

// Synthetic directory depth used by DefaultConfig.
const defaultMaxDepth = 6

// Fanout and depth used by S3RealisticConfig, modelling
// bucket/type/year/month/day/user/file paths.
const (
	s3PrefixFanout = 15
	s3MaxDepth     = 7
)

// FakeObject represents a synthetic S3 object for benchmarks.
type FakeObject struct {
	Key    string
	Size   uint64
	TierID tiers.ID
}

// GeneratorConfig configures synthetic data generation.
type GeneratorConfig struct {
	// NumObjects is the total number of objects to generate.
	NumObjects int
	// PrefixFanout is the average number of children per directory.
	PrefixFanout int
	// MaxDepth is the maximum directory depth.
	MaxDepth int
	// TierDistribution maps tier IDs to their probability (0.0-1.0).
	// If nil, all objects use Standard tier.
	TierDistribution map[tiers.ID]float64
	// Seed for reproducible generation. 0 = use default seed.
	Seed int64
}

// DefaultConfig returns a reasonable default configuration.
func DefaultConfig(numObjects int) GeneratorConfig {
	return GeneratorConfig{
		NumObjects:   numObjects,
		PrefixFanout: 10,
		MaxDepth:     defaultMaxDepth,
		TierDistribution: map[tiers.ID]float64{
			tiers.Standard:   defaultProbStandard,
			tiers.StandardIA: defaultProbStandardIA,
			tiers.GlacierIR:  defaultProbGlacierIR,
			tiers.ITFrequent: defaultProbITFrequent,
			tiers.ITArchive:  defaultProbITArchive,
		},
		Seed: defaultBenchSeed,
	}
}

// S3RealisticConfig returns a config that generates S3-like paths and
// a tier distribution that exercises all 11 storage classes. Most
// real enterprise buckets have objects across nearly every class — a
// few in legacy Standard, some auto-classed by Intelligent-Tiering
// into all of its access sub-tiers, plus deliberate lifecycle rules
// pushing cold data to Glacier/Deep Archive. Aggregated prefixes
// (toward the root) end up with non-zero counts in every tier
// column, which matches the on-disk reality.
func S3RealisticConfig(numObjects int) GeneratorConfig {
	return GeneratorConfig{
		NumObjects:   numObjects,
		PrefixFanout: s3PrefixFanout,
		MaxDepth:     s3MaxDepth,
		TierDistribution: map[tiers.ID]float64{
			tiers.Standard:         s3ProbStandard,
			tiers.StandardIA:       s3ProbStandardIA,
			tiers.GlacierIR:        s3ProbGlacierIR,
			tiers.GlacierFR:        s3ProbGlacierFR,
			tiers.DeepArchive:      s3ProbDeepArchive,
			tiers.ITFrequent:       s3ProbITFrequent,
			tiers.ITInfrequent:     s3ProbITInfrequent,
			tiers.ITArchiveInstant: s3ProbITArchiveInstant,
			tiers.ITArchive:        s3ProbITArchive,
			tiers.ITDeepArchive:    s3ProbITDeepArchive,
			tiers.ITFrequentSmall:  s3ProbITFrequentSmall,
		},
		Seed: defaultBenchSeed,
	}
}

// Generator generates synthetic S3 inventory data.
type Generator struct {
	cfg GeneratorConfig
	rng *rand.Rand
}

// NewGenerator creates a new data generator.
func NewGenerator(cfg GeneratorConfig) *Generator {
	seed := cfg.Seed
	if seed == 0 {
		seed = defaultBenchSeed
	}

	return &Generator{
		cfg: cfg,
		rng: rand.New(rand.NewSource(seed)),
	}
}

// Generate returns a slice of synthetic objects.
func (g *Generator) Generate() []FakeObject {
	objects := make([]FakeObject, g.cfg.NumObjects)

	for i := range g.cfg.NumObjects {
		objects[i] = g.generateObject()
	}

	return objects
}

// GenerateChannel returns objects via a channel for streaming processing.
func (g *Generator) GenerateChannel() <-chan FakeObject {
	ch := make(chan FakeObject, 1000)

	go func() {
		defer close(ch)
		for range g.cfg.NumObjects {
			ch <- g.generateObject()
		}
	}()

	return ch
}

func (g *Generator) generateObject() FakeObject {
	return FakeObject{
		Key:    g.generateKey(),
		Size:   g.generateSize(),
		TierID: g.generateTier(),
	}
}

func (g *Generator) generateKey() string {
	// Determine depth (1 to MaxDepth)
	depth := 1 + g.rng.Intn(g.cfg.MaxDepth)

	// Build path
	path := ""
	var pathBuilder strings.Builder
	for range depth {
		segment := g.generateSegment()
		pathBuilder.WriteString(segment + "/")
	}
	path += pathBuilder.String()

	// Add filename
	path += g.generateFilename()

	return path
}

func (g *Generator) generateSegment() string {
	const (
		yearBase    = 2020
		yearSpan    = 5
		monthsCount = 12
		daysCount   = 28
	)
	// Mix of different segment types to create realistic structure
	segmentType := g.rng.Intn(4)

	switch segmentType {
	case 0: // Date-like: 2024, 01, 15
		formats := []string{
			strconv.Itoa(yearBase + g.rng.Intn(yearSpan)),  // year
			fmt.Sprintf("%02d", 1+g.rng.Intn(monthsCount)), // month
			fmt.Sprintf("%02d", 1+g.rng.Intn(daysCount)),   // day
			fmt.Sprintf("hour=%02d", g.rng.Intn(24)),       // hour partition
			fmt.Sprintf("dt=%d-%02d-%02d", yearBase+g.rng.Intn(yearSpan), 1+g.rng.Intn(monthsCount), 1+g.rng.Intn(daysCount)),
		}

		return formats[g.rng.Intn(len(formats))]

	case 1: // ID-like: user_12345, account_abc
		prefixes := []string{"user", "account", "tenant", "org", "project"}
		prefix := prefixes[g.rng.Intn(len(prefixes))]
		id := g.rng.Intn(g.cfg.PrefixFanout * 100)

		return fmt.Sprintf("%s_%05d", prefix, id)

	case 2: // Category: logs, data, exports, backups
		categories := []string{"logs", "data", "exports", "backups", "raw", "processed", "archive", "tmp"}

		return categories[g.rng.Intn(len(categories))]

	default: // Simple alphabetic: a, b, ..., z, aa, ab, ...
		return g.generateAlphaSegment()
	}
}

func (g *Generator) generateAlphaSegment() string {
	const lettersInAlphabet = 26
	// Generate segments like: a, b, ..., z, aa, ab, ..., zz
	n := g.rng.Intn(g.cfg.PrefixFanout)
	if n < lettersInAlphabet {
		return string(rune('a' + n))
	}

	return string(rune('a'+n/lettersInAlphabet-1)) + string(rune('a'+n%lettersInAlphabet))
}

func (g *Generator) generateFilename() string {
	extensions := []string{".json", ".csv", ".parquet", ".txt", ".gz", ".log", ".dat"}
	ext := extensions[g.rng.Intn(len(extensions))]

	return fmt.Sprintf("file_%08x%s", g.rng.Uint32(), ext)
}

func (g *Generator) generateSize() uint64 {
	const (
		sizeBuckets       = 10
		smallFileSpan     = 1024 * 1024
		mediumFileSpan    = 100 * 1024 * 1024
		largeFileSpan     = 900 * 1024 * 1024
		veryLargeFileSpan = int64(4 * 1024 * 1024 * 1024)
	)
	// Log-normal-ish distribution: mostly small files, some large.
	// Threshold layout (g.rng.Intn(sizeBuckets) < threshold) by bucket:
	//   tiny [<1KB)            : 10%
	//   small [1KB,1MB)        : 30%
	//   medium [1MB,100MB)     : 40%
	//   large [100MB,1GB)      : 10%
	//   very large [1GB,5GB]   : 10%
	const (
		thresholdTiny   = 1
		thresholdSmall  = thresholdTiny + 3  // 30% small
		thresholdMedium = thresholdSmall + 4 // 40% medium
		thresholdLarge  = thresholdMedium + 1
	)
	bucket := g.rng.Intn(sizeBuckets)
	switch {
	case bucket < thresholdTiny:
		return uint64(g.rng.Intn(1024))
	case bucket < thresholdSmall:
		return uint64(1024 + g.rng.Intn(smallFileSpan))
	case bucket < thresholdMedium:
		return uint64(1024*1024 + g.rng.Intn(mediumFileSpan))
	case bucket < thresholdLarge:
		return uint64(100*1024*1024 + g.rng.Intn(largeFileSpan))
	default:
		return uint64(1024*1024*1024 + g.rng.Int63n(veryLargeFileSpan))
	}
}

func (g *Generator) generateTier() tiers.ID {
	if len(g.cfg.TierDistribution) == 0 {
		return tiers.Standard
	}

	r := g.rng.Float64()
	cumulative := 0.0

	for tierID, prob := range g.cfg.TierDistribution {
		cumulative += prob
		if r < cumulative {
			return tierID
		}
	}

	// Default fallback
	return tiers.Standard
}

// GenerateKeys returns just the keys for trie-building benchmarks.
func GenerateKeys(numObjects int, shape string) []string {
	switch shape {
	case "deep_narrow":
		return generateDeepNarrowKeys(numObjects)
	case "wide_shallow":
		return generateWideShallowKeys(numObjects)
	case "balanced":
		return generateBalancedKeys(numObjects)
	case "s3_realistic":
		return generateS3RealisticKeys(numObjects)
	case "wide_single_level":
		return generateWideSingleLevelKeys(numObjects)
	default:
		return generateS3RealisticKeys(numObjects)
	}
}

// Tree shape generators (moved from triebuild benchmarks for reuse)

func generateDeepNarrowKeys(size int) []string {
	keys := make([]string, size)
	depth := 20
	numBranches := 26
	filesPerLeaf := max(size/numBranches, 1)

	idx := 0
	for branch := 0; idx < size && branch < numBranches; branch++ {
		prefix := ""
		var prefixBuilder strings.Builder
		for range depth {
			prefixBuilder.WriteString(fmt.Sprintf("%c/", 'a'+byte(branch)))
		}
		prefix += prefixBuilder.String()
		for f := 0; idx < size && f < filesPerLeaf; f++ {
			keys[idx] = fmt.Sprintf("%sfile%d.txt", prefix, f)
			idx++
		}
	}

	return keys[:idx]
}

func generateWideShallowKeys(size int) []string {
	keys := make([]string, size)
	filesPerPrefix := 5
	numPrefixes := max(size/filesPerPrefix, 1)

	idx := 0
	for p := 0; idx < size && p < numPrefixes; p++ {
		prefix := fmt.Sprintf("prefix%05d/", p)
		for f := 0; idx < size && f < filesPerPrefix; f++ {
			keys[idx] = fmt.Sprintf("%sfile%d.txt", prefix, f)
			idx++
		}
	}

	return keys[:idx]
}

func generateBalancedKeys(size int) []string {
	keys := make([]string, size)
	branchFactor := 26
	depth := 3

	idx := 0
	var generate func(prefix string, level int)
	generate = func(prefix string, level int) {
		if idx >= size {
			return
		}
		if level >= depth {
			for f := 0; f < 5 && idx < size; f++ {
				keys[idx] = fmt.Sprintf("%sfile%d.txt", prefix, f)
				idx++
			}

			return
		}
		for c := 0; c < branchFactor && idx < size; c++ {
			generate(fmt.Sprintf("%s%c/", prefix, 'a'+byte(c)), level+1)
		}
	}
	generate("", 0)

	return keys[:idx]
}

func generateS3RealisticKeys(size int) []string {
	const daysInMonth = 28
	rng := rand.New(rand.NewSource(defaultBenchSeed))
	keys := make([]string, size)

	prefixes := []string{"data", "logs", "backups", "exports", "uploads"}
	years := []string{"2022", "2023", "2024"}
	months := []string{"01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"}
	extensions := []string{".json", ".csv", ".parquet", ".txt", ".gz"}

	for i := range size {
		prefix := prefixes[rng.Intn(len(prefixes))]
		year := years[rng.Intn(len(years))]
		month := months[rng.Intn(len(months))]
		day := fmt.Sprintf("%02d", rng.Intn(daysInMonth)+1)
		userID := fmt.Sprintf("user%05d", rng.Intn(1000))
		fileID := fmt.Sprintf("file_%08x", rng.Uint32())
		ext := extensions[rng.Intn(len(extensions))]

		keys[i] = fmt.Sprintf("%s/%s/%s/%s/%s/%s%s", prefix, year, month, day, userID, fileID, ext)
	}

	return keys
}

func generateWideSingleLevelKeys(size int) []string {
	keys := make([]string, size)
	for i := range size {
		keys[i] = fmt.Sprintf("root/child%07d/file.txt", i)
	}

	return keys
}
