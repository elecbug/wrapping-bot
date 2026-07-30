package textutil

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestSplitUTF8(t *testing.T) {
	input := strings.Repeat("가", 10)
	parts := SplitUTF8(input, 7)
	if strings.Join(parts, "") != input {
		t.Fatalf("split did not preserve input")
	}
	for _, part := range parts {
		if !utf8.ValidString(part) {
			t.Fatalf("invalid UTF-8 chunk: %q", part)
		}
		if len(part) > 7 {
			t.Fatalf("chunk too large: %d", len(part))
		}
	}
}

func TestStripANSI(t *testing.T) {
	got := StripANSI("\x1b[31merror\x1b[0m")
	if got != "error" {
		t.Fatalf("got %q", got)
	}
}
