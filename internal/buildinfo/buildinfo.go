// Package buildinfo reports which source a binary was built from: the
// release version, commit and commit date the linker stamped into it,
// or the VCS settings the go command embeds when it was not stamped.
package buildinfo

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

// Stamped with -ldflags "-X <this package>.version=... -X ...commit=...
// -X ...date=..." by the Makefile, infra/Dockerfile and the release
// workflow. Empty means not stamped.
//
//nolint:gochecknoglobals // -ldflags -X can only set package-level string variables
var (
	version string
	commit  string
	date    string
)

const (
	devVersion = "dev"
	unknown    = "unknown"

	settingRevision = "vcs.revision"
	settingModified = "vcs.modified"
	settingTime     = "vcs.time"
)

// Info identifies the source a binary was built from.
type Info struct {
	// Version is the release tag (or `git describe` output), the module
	// version for `go install pkg@version` builds, or "dev".
	Version string
	// Commit is the full commit SHA, suffixed "-dirty" when the tree had
	// uncommitted changes, or "unknown".
	Commit string
	// Date is the commit time in RFC 3339, or "unknown". It is the commit
	// time rather than the build time so that rebuilding a commit yields
	// the same binary.
	Date string
}

// Get returns the build metadata of the running binary.
func Get() Info {
	bi, _ := debug.ReadBuildInfo()

	return resolve(version, commit, date, bi)
}

// resolve fills every field the linker left empty from bi, then from
// the defaults.
func resolve(stampedVersion, stampedCommit, stampedDate string, bi *debug.BuildInfo) Info {
	info := Info{Version: stampedVersion, Commit: stampedCommit, Date: stampedDate}
	if bi != nil {
		if info.Version == "" && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
			info.Version = bi.Main.Version
		}
		var revision, modified, vcsTime string
		for _, s := range bi.Settings {
			switch s.Key {
			case settingRevision:
				revision = s.Value
			case settingModified:
				modified = s.Value
			case settingTime:
				vcsTime = s.Value
			}
		}
		if info.Commit == "" && revision != "" {
			info.Commit = revision
			if modified == "true" {
				info.Commit += "-dirty"
			}
		}
		if info.Date == "" {
			info.Date = vcsTime
		}
	}
	if info.Version == "" {
		info.Version = devVersion
	}
	if info.Commit == "" {
		info.Commit = unknown
	}
	if info.Date == "" {
		info.Date = unknown
	}

	return info
}

// Line formats i as the single line a binary prints for --version.
func (i Info) Line(program string) string {
	return fmt.Sprintf("%s %s (commit %s, %s, %s %s/%s)",
		program, i.Version, i.Commit, i.Date, runtime.Version(), runtime.GOOS, runtime.GOARCH)
}
