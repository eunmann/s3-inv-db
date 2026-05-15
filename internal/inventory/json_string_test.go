package inventory_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/inventory"
)

// TestInfoIDSerializesAsString locks in that the typed inventory.ID
// field still ships as a JSON string. Renaming the underlying type
// must NOT change the wire format.
func TestInfoIDSerializesAsString(t *testing.T) {
	info := inventory.Info{ID: inventory.ID("a/b/c"), Name: "n", State: inventory.StateLoaded}
	b, err := json.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"id":"a/b/c"`) {
		t.Errorf("id should serialize as plain string; got %s", b)
	}
}
