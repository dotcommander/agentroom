package main

import (
	"strings"
	"testing"
	"time"

	"github.com/dotcommander/agentchat/agentroom"
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
