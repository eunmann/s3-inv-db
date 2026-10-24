package buildinfo

import (
	"runtime"
	"runtime/debug"
	"testing"
)

func vcsBuildInfo(mainVersion string, settings map[string]string) *debug.BuildInfo {
	bi := &debug.BuildInfo{Main: debug.Module{Path: "github.com/eunmann/s3-inv-db", Version: mainVersion}}
	for k, v := range settings {
		bi.Settings = append(bi.Settings, debug.BuildSetting{Key: k, Value: v})
	}

	return bi
}

func TestResolve(t *testing.T) {
	cleanVCS := map[string]string{
		settingRevision: "0123456789abcdef0123456789abcdef01234567",
		settingTime:     "2026-10-24T18:15:00Z",
		settingModified: "false",
	}
	dirtyVCS := map[string]string{
		settingRevision: "0123456789abcdef0123456789abcdef01234567",
		settingTime:     "2026-10-24T18:15:00Z",
		settingModified: "true",
	}
	cases := []struct {
		name                    string
		version, commit, date   string
		bi                      *debug.BuildInfo
		wantVersion, wantCommit string
		wantDate                string
	}{
		{
			name:    "stamped values win over build info",
			version: "v1.2.3", commit: "fedcba", date: "2026-01-02T03:04:05Z",
			bi:          vcsBuildInfo("v0.0.0-20261024181500-0123456789ab", dirtyVCS),
			wantVersion: "v1.2.3", wantCommit: "fedcba", wantDate: "2026-01-02T03:04:05Z",
		},
		{
			name:        "no build info falls back to defaults",
			wantVersion: devVersion, wantCommit: unknown, wantDate: unknown,
		},
		{
			name:        "devel main version and no VCS settings",
			bi:          vcsBuildInfo("(devel)", nil),
			wantVersion: devVersion, wantCommit: unknown, wantDate: unknown,
		},
		{
			name:        "VCS settings of a clean tree",
			bi:          vcsBuildInfo("(devel)", cleanVCS),
			wantVersion: devVersion, wantCommit: "0123456789abcdef0123456789abcdef01234567", wantDate: "2026-10-24T18:15:00Z",
		},
		{
			name:        "modified tree marks the commit dirty",
			bi:          vcsBuildInfo("", dirtyVCS),
			wantVersion: devVersion, wantCommit: "0123456789abcdef0123456789abcdef01234567-dirty", wantDate: "2026-10-24T18:15:00Z",
		},
		{
			name:        "go install module version",
			bi:          vcsBuildInfo("v1.4.0", nil),
			wantVersion: "v1.4.0", wantCommit: unknown, wantDate: unknown,
		},
		{
			name:        "only the unstamped fields fall back",
			version:     "v2.0.0-rc.1",
			bi:          vcsBuildInfo("v0.0.0-20261024181500-0123456789ab", cleanVCS),
			wantVersion: "v2.0.0-rc.1", wantCommit: "0123456789abcdef0123456789abcdef01234567", wantDate: "2026-10-24T18:15:00Z",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := resolve(c.version, c.commit, c.date, c.bi)
			want := Info{Version: c.wantVersion, Commit: c.wantCommit, Date: c.wantDate}
			if got != want {
				t.Errorf("resolve() = %+v, want %+v", got, want)
			}
		})
	}
}

func TestLine(t *testing.T) {
	got := Info{Version: "v1.2.3", Commit: "abc", Date: "2026-10-24T18:15:00Z"}.Line("s3-inv-db")
	want := "s3-inv-db v1.2.3 (commit abc, 2026-10-24T18:15:00Z, " + runtime.Version() + " " + runtime.GOOS + "/" + runtime.GOARCH + ")"
	if got != want {
		t.Errorf("Line() = %q, want %q", got, want)
	}
}
