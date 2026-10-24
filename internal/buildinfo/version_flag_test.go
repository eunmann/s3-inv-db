package buildinfo_test

import (
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/buildinfo"
)

// TestVersionFlag links each binary the way the Makefile and release
// workflow do and checks that --version prints the stamped values on
// one line and exits 0. A wrong -X symbol path is silently ignored by
// the linker, so only a real build catches it.
func TestVersionFlag(t *testing.T) {
	if testing.Short() {
		t.Skip("builds three binaries")
	}
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Fatalf("go toolchain not on PATH: %v", err)
	}
	gomod, err := exec.CommandContext(t.Context(), goBin, "env", "GOMOD").Output()
	if err != nil {
		t.Fatalf("go env GOMOD: %v", err)
	}
	root := filepath.Dir(strings.TrimSpace(string(gomod)))

	pkg := reflect.TypeFor[buildinfo.Info]().PkgPath()
	const (
		version = "v9.8.7-rc.1"
		commit  = "0123456789abcdef0123456789abcdef01234567"
		date    = "2026-10-24T18:15:00Z"
	)
	ldflags := "-X " + pkg + ".version=" + version + " -X " + pkg + ".commit=" + commit + " -X " + pkg + ".date=" + date

	binDir := t.TempDir()
	programs := []string{"s3-inv-db", "s3-inv-db-server", "s3-inv-db-seeder"}
	args := make([]string, 0, 6+len(programs))
	args = append(args, "build", "-trimpath", "-ldflags", ldflags, "-o", binDir+string(filepath.Separator))
	for _, p := range programs {
		args = append(args, "./cmd/"+p)
	}
	build := exec.CommandContext(t.Context(), goBin, args...)
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}

	for _, p := range programs {
		t.Run(p, func(t *testing.T) {
			bin := filepath.Join(binDir, p)
			out, err := exec.CommandContext(t.Context(), bin, "--version").Output()
			if err != nil {
				t.Fatalf("%s --version: %v", p, err)
			}
			want := p + " " + version + " (commit " + commit + ", " + date + ", " +
				runtime.Version() + " " + runtime.GOOS + "/" + runtime.GOARCH + ")\n"
			if string(out) != want {
				t.Errorf("%s --version printed %q, want %q", p, out, want)
			}
		})
	}
}
