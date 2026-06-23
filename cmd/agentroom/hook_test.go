package main

import (
	"strings"
	"testing"

	"github.com/dotcommander/agentchat/agentroom"
)

func TestExtractText(t *testing.T) {
	t.Parallel()
	if got := extractText([]byte(`"hello world"`)); got != "hello world" {
		t.Errorf("string content = %q, want hello world", got)
	}
	if got := extractText([]byte(`[{"type":"text","text":"block text"}]`)); got != "block text" {
		t.Errorf("block content = %q, want block text", got)
	}
	if got := extractText([]byte(`[{"type":"tool_use","name":"x"}]`)); got != "" {
		t.Errorf("non-text blocks = %q, want empty", got)
	}
}

func TestClip(t *testing.T) {
	t.Parallel()
	if got := clip("  hi  ", 10); got != "hi" {
		t.Errorf("clip trim = %q, want hi", got)
	}
	if got := clip("abcdefghij", 5); got != "abcde..." {
		t.Errorf("clip long = %q, want abcde...", got)
	}
}

func TestDeltaDigest(t *testing.T) {
	t.Parallel()
	events := []agentroom.Event{
		{Type: "TESTS_FAILED", AgentID: "ci-1", Payload: []byte(`{"pkg":"auth"}`)},
		{Type: "AGENT_JOINED", AgentID: ""},
	}
	got := deltaDigest("auth", "main", events)
	if !strings.Contains(got, "2 new event(s) in auth:main") {
		t.Errorf("missing header: %q", got)
	}
	if !strings.Contains(got, "TESTS_FAILED  ci-1") {
		t.Errorf("missing event line: %q", got)
	}
	if !strings.Contains(got, `{"pkg":"auth"}`) {
		t.Errorf("missing payload: %q", got)
	}
	if !strings.Contains(got, "AGENT_JOINED  ?") {
		t.Errorf("missing empty-agent fallback: %q", got)
	}
}
