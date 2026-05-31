package benchutil

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// GridSpec is one cell of the (shape × tier × size) grid.
type GridSpec struct {
	Tier  TierDistribution
	Size  SizeDistribution
	Shape string
	N     int
	Seed  int64
}

// Name returns a stable cell identifier used in NDJSON output.
func (s GridSpec) Name() string {
	return fmt.Sprintf("shape=%s/tier=%s/size=%s/n=%d", s.Shape, s.Tier.Name, s.Size.Name, s.N)
}

// GridGenerator emits FakeObject instances for a grid cell. Unlike
// the original Generator it picks Key from a named shape preset and
// pulls Size + TierID from the requested distributions.
type GridGenerator struct {
	rng        *rand.Rand
	tierProbs  []tierProb
	sizeSample func(rng *rand.Rand) uint64
	shape      string
	keyState   any
}

type tierProb struct {
	cum float64
	id  uint64 // tiers.ID
}

// NewGridGenerator builds a generator for spec. The seed defaults to
// BenchmarkSeed when zero.
func NewGridGenerator(spec GridSpec) *GridGenerator {
	seed := spec.Seed
	if seed == 0 {
		seed = BenchmarkSeed
	}
	rng := rand.New(rand.NewSource(seed))
	probs := make([]tierProb, 0, len(spec.Tier.Probs))
	cum := 0.0
	for id, p := range spec.Tier.Probs {
		cum += p
		probs = append(probs, tierProb{cum: cum, id: uint64(id)})
	}
	g := &GridGenerator{
		rng:        rng,
		tierProbs:  probs,
		sizeSample: spec.Size.Sample,
		shape:      spec.Shape,
	}
	g.initKeyState(spec.N)
	return g
}

// Stream emits N synthetic objects through visit without ever
// materialising the slice. N is the spec.N captured at construction
// time.
func (g *GridGenerator) Stream(n int, visit func(FakeObject)) {
	for i := range n {
		visit(FakeObject{
			Key:    g.nextKey(i, n),
			Size:   g.sizeSample(g.rng),
			TierID: g.sampleTier(),
		})
	}
}

func (g *GridGenerator) sampleTier() tiers.ID {
	r := g.rng.Float64()
	for _, tp := range g.tierProbs {
		if r < tp.cum {
			return tiers.ID(tp.id)
		}
	}
	if len(g.tierProbs) == 0 {
		return tiers.ID(0)
	}
	return tiers.ID(g.tierProbs[len(g.tierProbs)-1].id)
}

type deepNarrowState struct {
	depth        int
	numBranches  int
	filesPerLeaf int
}

type wideShallowState struct {
	filesPerPrefix int
	numPrefixes    int
}

type balancedState struct {
	branchFactor int
	depth        int
}

type wideSingleLevelState struct{}

type s3RealisticState struct{}

func (g *GridGenerator) initKeyState(n int) {
	switch g.shape {
	case "deep_narrow":
		const depth = 20
		const numBranches = 26
		g.keyState = &deepNarrowState{
			depth:        depth,
			numBranches:  numBranches,
			filesPerLeaf: max(n/numBranches, 1),
		}
	case "wide_shallow":
		const filesPerPrefix = 5
		g.keyState = &wideShallowState{
			filesPerPrefix: filesPerPrefix,
			numPrefixes:    max(n/filesPerPrefix, 1),
		}
	case "balanced":
		const alphabetSize = 26
		const balancedDepth = 3
		g.keyState = &balancedState{branchFactor: alphabetSize, depth: balancedDepth}
	case "wide_single_level":
		g.keyState = &wideSingleLevelState{}
	case "s3_realistic":
		g.keyState = &s3RealisticState{}
	default:
		g.keyState = &s3RealisticState{}
	}
}

func (g *GridGenerator) nextKey(i, n int) string {
	switch s := g.keyState.(type) {
	case *deepNarrowState:
		branch := i / s.filesPerLeaf
		if branch >= s.numBranches {
			branch = s.numBranches - 1
		}
		f := i % s.filesPerLeaf
		var b strings.Builder
		for range s.depth {
			b.WriteByte(byte('a' + branch))
			b.WriteByte('/')
		}
		fmt.Fprintf(&b, "file%d.txt", f)
		return b.String()
	case *wideShallowState:
		p := i / s.filesPerPrefix
		if p >= s.numPrefixes {
			p = s.numPrefixes - 1
		}
		f := i % s.filesPerPrefix
		return fmt.Sprintf("prefix%05d/file%d.txt", p, f)
	case *balancedState:
		// Compute a deterministic key by interpreting i in base 26 over depth digits.
		var b strings.Builder
		idx := i
		for range s.depth {
			b.WriteByte(byte('a' + idx%s.branchFactor))
			b.WriteByte('/')
			idx /= s.branchFactor
		}
		const filesPerLeaf = 5
		fmt.Fprintf(&b, "file%d.txt", i%filesPerLeaf)
		return b.String()
	case *wideSingleLevelState:
		return fmt.Sprintf("root/child%07d/file.txt", i)
	case *s3RealisticState:
		return s3RealisticKey(g.rng)
	}
	_ = n // reserved for shapes that want total count
	return s3RealisticKey(g.rng)
}

func s3RealisticKey(rng *rand.Rand) string {
	prefixes := []string{"data", "logs", "backups", "exports", "uploads"}
	years := []string{"2022", "2023", "2024"}
	months := []string{"01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"}
	exts := []string{".json", ".csv", ".parquet", ".txt", ".gz"}
	const daysInMonth = 28
	prefix := prefixes[rng.Intn(len(prefixes))]
	year := years[rng.Intn(len(years))]
	month := months[rng.Intn(len(months))]
	day := fmt.Sprintf("%02d", rng.Intn(daysInMonth)+1)
	user := fmt.Sprintf("user%05d", rng.Intn(1000))
	file := fmt.Sprintf("file_%08x", rng.Uint32())
	ext := exts[rng.Intn(len(exts))]
	return fmt.Sprintf("%s/%s/%s/%s/%s/%s%s", prefix, year, month, day, user, file, ext)
}
