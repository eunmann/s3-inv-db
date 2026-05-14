package inventory

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestInfoIDSerializesAsString locks in that the typed inventory.ID
// field still ships as a JSON string. Renaming the underlying type
// must NOT change the wire format.
func TestInfoIDSerializesAsString(t *testing.T) {
	info := Info{ID: ID("a/b/c"), Name: "n", State: StateLoaded}
	b, err := json.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"id":"a/b/c"`) {
		t.Errorf("id should serialize as plain string; got %s", b)
	}
}
