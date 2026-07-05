package main

import (
	"strings"
	"testing"
	"time"

	"github.com/dotcommander/agentroom/agentroom"
)

func zeroClaims(string) int { return 0 }

func TestWhoLinesEmpty(t *testing.T) {
	t.Parallel()
	got := whoLines(map[string]agentroom.PresenceEntry{}, "", zeroClaims)
	if len(got) != 1 || got[0] != "(nobody here)" {
		t.Fatalf("empty roster = %#v, want one \"(nobody here)\" line", got)
	}
}

func TestWhoLinesRendersDescTTLClaimsSelf(t *testing.T) {
	t.Parallel()
	pres := map[string]agentroom.PresenceEntry{
		"fixer-1-a1b2c3d4": {Desc: "go-fixer: parser", TTL: 90 * time.Second},
		"anon-9f8e7d6c":    {Desc: "", TTL: 30 * time.Second},
	}
	claims := func(id string) int {
		if id == "fixer-1-a1b2c3d4" {
			return 2
		}
		return 0
	}
	joined := strings.Join(whoLines(pres, "fixer-1-a1b2c3d4", claims), "\n")
	for _, want := range []string{"go-fixer: parser", "(no role posted)", "(2 claimed)", "(you)", "[1m30s left]"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("rendered roster missing %q:\n%s", want, joined)
		}
	}
}

func TestHumanTTL(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   time.Duration
		want string
	}{
		{90 * time.Second, "1m30s"},
		{5 * time.Second, "5s"},
		{0, "expiring"},
		{-3 * time.Second, "expiring"},
	}
	for _, tc := range cases {
		if got := humanTTL(tc.in); got != tc.want {
			t.Fatalf("humanTTL(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestWhoLinesSuppressesAnonymousIdle(t *testing.T) {
	t.Parallel()
	pres := map[string]agentroom.PresenceEntry{
		"cli@Mac.lan:26859": {Desc: "", TTL: time.Minute},            // anonymous, role-less -> hidden
		"cli-host-123":      {Desc: "", TTL: time.Minute},            // anonymous (new form), role-less -> hidden
		"a1b2c3d4":          {Desc: "", TTL: time.Minute},            // live session, role-less -> kept
		"opus-a1b2c3d4":     {Desc: "role=fixer", TTL: time.Minute},  // named + role -> kept
		"cli-deadbeef":      {Desc: "role=helper", TTL: time.Minute}, // cli prefix but HAS role -> kept
	}
	joined := strings.Join(whoLines(pres, "", zeroClaims), "\n")
	for _, hidden := range []string{"cli@Mac.lan:26859", "cli-host-123"} {
		if strings.Contains(joined, hidden) {
			t.Fatalf("anonymous role-less marker %q should be hidden:\n%s", hidden, joined)
		}
	}
	for _, kept := range []string{"a1b2c3d4", "opus-a1b2c3d4", "cli-deadbeef"} {
		if !strings.Contains(joined, kept) {
			t.Fatalf("expected %q kept:\n%s", kept, joined)
		}
	}
}

func TestRedundantSessionKeysCollapsesBareToken(t *testing.T) {
	t.Parallel()
	got := redundantSessionKeys([]string{"abc12345", "opus-abc12345", "lonelytok", "bob-deadbeef", "deadbeef"})
	for _, want := range []string{"abc12345", "deadbeef"} {
		if _, ok := got[want]; !ok {
			t.Fatalf("%q should be redundant (a named <handle>-%s entry exists)", want, want)
		}
	}
	for _, keep := range []string{"lonelytok", "opus-abc12345", "bob-deadbeef"} {
		if _, ok := got[keep]; ok {
			t.Fatalf("%q should NOT be redundant", keep)
		}
	}
}

func TestWhoLinesCollapsesSessionDuplicate(t *testing.T) {
	t.Parallel()
	pres := map[string]agentroom.PresenceEntry{
		"abc12345":      {Desc: "", TTL: time.Minute},           // bare session token (hook), role-less
		"opus-abc12345": {Desc: "role=fixer", TTL: time.Minute}, // same session, named + role
	}
	lines := whoLines(pres, "", zeroClaims)
	if len(lines) != 1 {
		t.Fatalf("agent should show exactly once, got %d lines: %v", len(lines), lines)
	}
	if !strings.Contains(lines[0], "opus-abc12345") {
		t.Fatalf("named entry should be the surviving row: %q", lines[0])
	}
}
