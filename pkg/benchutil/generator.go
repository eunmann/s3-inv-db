// Package benchutil provides synthetic data generation for benchmarks and testing.
package benchutil

import (
	"fmt"
	"math/rand"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
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
		MaxDepth:     6,
		TierDistribution: map[tiers.ID]float64{
			tiers.Standard:   0.60,
			tiers.StandardIA: 0.15,
			tiers.GlacierIR:  0.10,
			tiers.ITFrequent: 0.10,
			tiers.ITArchive:  0.05,
		},
		Seed: 42,
	}
}

// S3RealisticConfig returns a config that generates S3-like paths.
func S3RealisticConfig(numObjects int) GeneratorConfig {
	return GeneratorConfig{
		NumObjects:   numObjects,
		PrefixFanout: 15, // date-based structures
		MaxDepth:     7,  // bucket/type/year/month/day/user/file
		TierDistribution: map[tiers.ID]float64{
			tiers.Standard:   0.50,
			tiers.StandardIA: 0.20,
			tiers.GlacierIR:  0.15,
			tiers.ITFrequent: 0.10,
			tiers.ITArchive:  0.05,
		},
		Seed: 42,
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
		seed = 42
	}
	return &Generator{
		cfg: cfg,
		rng: rand.New(rand.NewSource(seed)),
	}
}

// Generate returns a slice of synthetic objects.
func (g *Generator) Generate() []FakeObject {
	objects := make([]FakeObject, g.cfg.NumObjects)

	for i := 0; i < g.cfg.NumObjects; i++ {
		objects[i] = g.generateObject()
	}

	return objects
}

// GenerateChannel returns objects via a channel for streaming processing.
func (g *Generator) GenerateChannel() <-chan FakeObject {
	ch := make(chan FakeObject, 1000)

	go func() {
		defer close(ch)
		for i := 0; i < g.cfg.NumObjects; i++ {
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
	for d := 0; d < depth; d++ {
		segment := g.generateSegment()
		path += segment + "/"
	}

	// Add filename
	path += g.generateFilename()
	return path
}

func (g *Generator) generateSegment() string {
	// Mix of different segment types to create realistic structure
	segmentType := g.rng.Intn(4)

	switch segmentType {
	case 0: // Date-like: 2024, 01, 15
		formats := []string{
			fmt.Sprintf("%d", 2020+g.rng.Intn(5)),    // year
			fmt.Sprintf("%02d", 1+g.rng.Intn(12)),    // month
			fmt.Sprintf("%02d", 1+g.rng.Intn(28)),    // day
			fmt.Sprintf("hour=%02d", g.rng.Intn(24)), // hour partition
			fmt.Sprintf("dt=%d-%02d-%02d", 2020+g.rng.Intn(5), 1+g.rng.Intn(12), 1+g.rng.Intn(28)),
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
	// Generate segments like: a, b, ..., z, aa, ab, ..., zz
	n := g.rng.Intn(g.cfg.PrefixFanout)
	if n < 26 {
		return string(rune('a' + n))
	}
	return string(rune('a'+n/26-1)) + string(rune('a'+n%26))
}

func (g *Generator) generateFilename() string {
	extensions := []string{".json", ".csv", ".parquet", ".txt", ".gz", ".log", ".dat"}
	ext := extensions[g.rng.Intn(len(extensions))]
	return fmt.Sprintf("file_%08x%s", g.rng.Uint32(), ext)
}

func (g *Generator) generateSize() uint64 {
	// Log-normal-ish distribution: mostly small files, some large
	switch g.rng.Intn(10) {
	case 0: // 10% tiny files (< 1KB)
		return uint64(g.rng.Intn(1024))
	case 1, 2, 3: // 30% small files (1KB - 1MB)
		return uint64(1024 + g.rng.Intn(1024*1024))
	case 4, 5, 6, 7: // 40% medium files (1MB - 100MB)
		return uint64(1024*1024 + g.rng.Intn(100*1024*1024))
	case 8: // 10% large files (100MB - 1GB)
		return uint64(100*1024*1024 + g.rng.Intn(900*1024*1024))
	default: // 10% very large files (1GB - 5GB)
		return uint64(1024*1024*1024 + g.rng.Int63n(4*1024*1024*1024))
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
	filesPerLeaf := size / numBranches
	if filesPerLeaf < 1 {
		filesPerLeaf = 1
	}

	idx := 0
	for branch := 0; idx < size && branch < numBranches; branch++ {
		prefix := ""
		for d := 0; d < depth; d++ {
			prefix += fmt.Sprintf("%c/", 'a'+byte(branch))
		}
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
	numPrefixes := size / filesPerPrefix
	if numPrefixes < 1 {
		numPrefixes = 1
	}

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
	rng := rand.New(rand.NewSource(42))
	keys := make([]string, size)

	prefixes := []string{"data", "logs", "backups", "exports", "uploads"}
	years := []string{"2022", "2023", "2024"}
	months := []string{"01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"}
	extensions := []string{".json", ".csv", ".parquet", ".txt", ".gz"}

	for i := 0; i < size; i++ {
		prefix := prefixes[rng.Intn(len(prefixes))]
		year := years[rng.Intn(len(years))]
		month := months[rng.Intn(len(months))]
		day := fmt.Sprintf("%02d", rng.Intn(28)+1)
		userID := fmt.Sprintf("user%05d", rng.Intn(1000))
		fileID := fmt.Sprintf("file_%08x", rng.Uint32())
		ext := extensions[rng.Intn(len(extensions))]

		keys[i] = fmt.Sprintf("%s/%s/%s/%s/%s/%s%s", prefix, year, month, day, userID, fileID, ext)
	}
	return keys
}

func generateWideSingleLevelKeys(size int) []string {
	keys := make([]string, size)
	for i := 0; i < size; i++ {
		keys[i] = fmt.Sprintf("root/child%07d/file.txt", i)
	}
	return keys
}
