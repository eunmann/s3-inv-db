package cli

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

var (
	errVerifyIntegrity = errors.New("verify: integrity issues detected")
	errMPHFLostPrefix  = errors.New("MPHF lost prefix")
	errMPHFMismatch    = errors.New("MPHF roundtrip mismatch")
	// ErrNegativeSample is returned when --sample is negative; the int→uint64
	// cast would otherwise wrap to a huge positive bound.
	ErrNegativeSample = errors.New("--sample must be >= 0")
)

type verifyOutput struct {
	Index            string `json:"index"`
	NodeCount        uint64 `json:"node_count"`
	MaxDepth         uint32 `json:"max_depth"`
	FilesChecked     int    `json:"files_checked"`
	MPHFRoundtripOK  bool   `json:"mphf_roundtrip_ok"`
	MPHFRoundtripErr string `json:"mphf_roundtrip_err,omitempty"`
	ManifestOK       bool   `json:"manifest_ok"`
	ManifestErr      string `json:"manifest_err,omitempty"`
}

func runVerify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	indexDir := fs.String("index", "", "index directory to verify")
	sample := fs.Int("sample", 0, "MPHF roundtrip sample size (0 = check every prefix)")
	outputFlag := addOutputFlag(fs)

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}

	out, err := parseOutputFormat(*outputFlag)
	if err != nil {
		return err
	}

	if *indexDir == "" {
		return ErrIndexRequired
	}
	if *sample < 0 {
		return ErrNegativeSample
	}

	result := verifyOutput{Index: *indexDir}

	manifest, err := format.ReadManifest(*indexDir)
	if err != nil {
		result.ManifestErr = err.Error()
	} else {
		result.NodeCount = manifest.NodeCount
		result.MaxDepth = manifest.MaxDepth
		result.FilesChecked = len(manifest.Files)
		if vErr := format.VerifyManifest(*indexDir, manifest); vErr != nil {
			result.ManifestErr = vErr.Error()
		} else {
			result.ManifestOK = true
		}
	}

	if rtErr := mphfRoundtrip(*indexDir, *sample); rtErr != nil {
		result.MPHFRoundtripErr = rtErr.Error()
	} else {
		result.MPHFRoundtripOK = true
	}

	if out == OutputJSON {
		return writeJSON(os.Stdout, result)
	}

	return printVerifyText(result)
}

func printVerifyText(r verifyOutput) error {
	fmt.Fprintf(os.Stdout, "Index: %s\n", r.Index)
	fmt.Fprintf(os.Stdout, "Nodes: %d, MaxDepth: %d, Files checked: %d\n", r.NodeCount, r.MaxDepth, r.FilesChecked)
	manifestStatus := "OK"
	if !r.ManifestOK {
		manifestStatus = "FAIL: " + r.ManifestErr
	}
	fmt.Fprintf(os.Stdout, "Manifest: %s\n", manifestStatus)
	mphStatus := "OK"
	if !r.MPHFRoundtripOK {
		mphStatus = "FAIL: " + r.MPHFRoundtripErr
	}
	fmt.Fprintf(os.Stdout, "MPHF roundtrip: %s\n", mphStatus)
	if !r.ManifestOK || !r.MPHFRoundtripOK {
		return errVerifyIntegrity
	}

	return nil
}

// mphfRoundtrip iterates positions, reconstructs the prefix from the
// dictionary, and confirms MPHF.Lookup(prefix) returns the same position.
// If sample > 0, only the first `sample` positions are checked.
func mphfRoundtrip(dir string, sample int) error {
	idx, err := indexread.Open(dir)
	if err != nil {
		return fmt.Errorf("open index: %w", err)
	}
	defer idx.Close()

	count := idx.Count()
	limit := count
	if sample > 0 && uint64(sample) < limit {
		limit = uint64(sample)
	}

	for pos := range limit {
		name, err := idx.PrefixString(pos)
		if err != nil {
			return fmt.Errorf("prefix at pos %d: %w", pos, err)
		}
		got, ok := idx.Lookup(name)
		if !ok {
			return fmt.Errorf("%w: pos %d (%q)", errMPHFLostPrefix, pos, name)
		}
		if got != pos {
			return fmt.Errorf("%w: pos %d → %q → pos %d", errMPHFMismatch, pos, name, got)
		}
	}

	return nil
}
