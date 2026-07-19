package main

import (
	"os"
	"strings"
	"testing"
)

func TestAgentGuideCommandContract(t *testing.T) {
	raw, err := os.ReadFile("../../docs/agent-guide.md")
	if err != nil {
		t.Fatalf("read agent guide: %v", err)
	}
	guide := string(raw)
	for _, command := range []string{
		"agentroom lease acquire",
		"agentroom guard",
		"agentroom window request",
		"agentroom window activate",
		"agentroom work started",
		"agentroom status",
		"agentroom version --json",
	} {
		if !strings.Contains(guide, command) {
			t.Errorf("agent guide missing %q", command)
		}
	}
	if strings.Contains(guide, "agentchat") {
		t.Fatal("agent guide contains obsolete agentchat path")
	}
}
