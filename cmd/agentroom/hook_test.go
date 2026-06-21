package main

import "testing"

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
