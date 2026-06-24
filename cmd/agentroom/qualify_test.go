package main

import (
	"strings"
	"testing"
)

func TestSanitizeHandle(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, in, want string
	}{
		{"plain", "opus-pidrive", "opus-pidrive"},
		{"keeps allowed punctuation", "go_fixer.1@host", "go_fixer.1@host"},
		{"colon becomes dash", "a:b", "a-b"},
		{"glob chars become dash", "a*b?c[d]", "a-b-c-d-"},
		{"whitespace becomes dash", "a b\tc", "a-b-c"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := sanitizeHandle(tc.in); got != tc.want {
				t.Fatalf("sanitizeHandle(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestSessionTokenUsesClaudeSessionID(t *testing.T) {
	t.Setenv("CLAUDE_SESSION_ID", "a1b2c3d4-5678-90ab-cdef-000000000000")
	if got := sessionToken(); got != "a1b2c3d4" {
		t.Fatalf("sessionToken() = %q, want %q", got, "a1b2c3d4")
	}
}

func TestSessionTokenFallsBackWithoutSession(t *testing.T) {
	t.Setenv("CLAUDE_SESSION_ID", "")
	got := sessionToken()
	if strings.Contains(got, "-") == false {
		t.Fatalf("fallback sessionToken() = %q, want host-ppid form", got)
	}
}

func TestQualifyAgentAppendsSession(t *testing.T) {
	t.Setenv("CLAUDE_SESSION_ID", "a1b2c3d4-5678-90ab-cdef-000000000000")
	if got := qualifyAgent("opus-pidrive"); got != "opus-pidrive-a1b2c3d4" {
		t.Fatalf("qualifyAgent() = %q, want %q", got, "opus-pidrive-a1b2c3d4")
	}
}

func TestQualifyAgentDistinctPerSession(t *testing.T) {
	t.Setenv("CLAUDE_SESSION_ID", "aaaaaaaa-1111")
	a := qualifyAgent("opus-pidrive")
	t.Setenv("CLAUDE_SESSION_ID", "bbbbbbbb-2222")
	b := qualifyAgent("opus-pidrive")
	if a == b {
		t.Fatalf("same handle in two sessions collided: both %q", a)
	}
}

func TestQualifyAgentEmptyHandleIsBareToken(t *testing.T) {
	t.Setenv("CLAUDE_SESSION_ID", "a1b2c3d4-5678")
	if got := qualifyAgent(""); got != "a1b2c3d4" {
		t.Fatalf("qualifyAgent(\"\") = %q, want %q", got, "a1b2c3d4")
	}
}
