//go:build linux

package sysmem

import (
	"os"
	"path/filepath"
	"testing"
)

func writeProcFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	return path
}

func TestProcField(t *testing.T) {
	io := writeProcFile(t, "rchar: 4092\nread_bytes: 8192\nwrite_bytes: 4096\ncancelled_write_bytes: 99\n")
	vmstat := writeProcFile(t, "workingset_refault_anon 7\nworkingset_refault_file 1234\n")
	cases := []struct {
		path, name string
		want       int64
	}{
		{io, "read_bytes:", 8192},
		{io, "write_bytes:", 4096},
		{vmstat, "workingset_refault_file", 1234},
		{vmstat, "workingset_refault", 0}, // a prefix of a name is not the name
		{vmstat, "missing", 0},
		{filepath.Join(t.TempDir(), "absent"), "read_bytes:", 0},
	}
	for _, c := range cases {
		if got := procField(c.path, c.name); got != c.want {
			t.Errorf("procField(%q) = %d, want %d", c.name, got, c.want)
		}
	}
}

func TestPSIFullTotal(t *testing.T) {
	path := writeProcFile(t, "some avg10=0.00 avg60=0.10 avg300=0.20 total=500\nfull avg10=0.00 avg60=0.05 avg300=0.10 total=321\n")
	if got := psiFullTotal(path); got != 321 {
		t.Errorf("psiFullTotal = %d, want 321", got)
	}
	if got := psiFullTotal(filepath.Join(t.TempDir(), "absent")); got != 0 {
		t.Errorf("psiFullTotal(absent) = %d, want 0", got)
	}
}

func TestPressureSince(t *testing.T) {
	a := ReadPressure()
	b := a
	b.MajorFaults += 3
	b.ReadBytes += 4096
	if d := b.Since(a); d.MajorFaults != 3 || d.ReadBytes != 4096 || d.WrittenBytes != 0 {
		t.Errorf("Since = %+v", d)
	}
}
