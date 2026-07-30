package textutil

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

var ansiPattern = regexp.MustCompile(`\x1b(?:\[[0-?]*[ -/]*[@-~]|\][^\x07]*(?:\x07|\x1b\\))`)

func StripANSI(s string) string {
	return ansiPattern.ReplaceAllString(s, "")
}

func EscapeCodeFence(s string) string {
	return strings.ReplaceAll(s, "```", "`\u200b``")
}

// SplitUTF8 splits s into chunks of at most maxBytes without breaking UTF-8.
func SplitUTF8(s string, maxBytes int) []string {
	if maxBytes <= 0 || len(s) <= maxBytes {
		return []string{s}
	}

	chunks := make([]string, 0, len(s)/maxBytes+1)
	for len(s) > maxBytes {
		cut := maxBytes
		for cut > 0 && !utf8.RuneStart(s[cut]) {
			cut--
		}
		if cut == 0 {
			_, size := utf8.DecodeRuneInString(s)
			cut = size
		}

		// Prefer a nearby newline to preserve log readability.
		searchStart := cut / 2
		if idx := strings.LastIndexByte(s[searchStart:cut], '\n'); idx >= 0 {
			cut = searchStart + idx + 1
		}

		chunks = append(chunks, s[:cut])
		s = s[cut:]
	}
	if s != "" {
		chunks = append(chunks, s)
	}
	return chunks
}
