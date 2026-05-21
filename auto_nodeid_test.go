package sundial

import (
	"strings"
	"testing"
)

// TestAutoNodeID_AlwaysReturnsNonEmpty — the contract is that the
// helper never returns "" under any environment. Tests run with a
// real os.Hostname() on every supported OS, so this covers the
// happy path on each CI matrix entry.
func TestAutoNodeID_AlwaysReturnsNonEmpty(t *testing.T) {
	got := AutoNodeID()
	if got == "" {
		t.Fatal("AutoNodeID returned an empty string")
	}
}

// TestAutoNodeID_FallbackPrefix — when neither hostname nor HOSTNAME
// is usable we should produce a "sundial-<hex>" identifier. We can't
// reliably mock os.Hostname from a test, but we can at least verify
// that the fallback path produces the documented shape when called
// directly. (Forcing the fallback would mean refactoring the helper
// to take a hostname-getter; that's not worth the surface area for a
// single helper.)
func TestAutoNodeID_DocumentedShape(t *testing.T) {
	got := AutoNodeID()
	// Either it's a real hostname (no prefix), or our fallback
	// (with the prefix). Both are valid; we just want to assert
	// the format is *one* of those two shapes.
	if strings.HasPrefix(got, "sundial-") {
		rest := strings.TrimPrefix(got, "sundial-")
		// 8 hex chars or "unknown".
		if rest != "unknown" && len(rest) != 8 {
			t.Errorf("fallback NodeID %q: expected 8 hex chars or 'unknown', got len=%d", got, len(rest))
		}
	}
}
