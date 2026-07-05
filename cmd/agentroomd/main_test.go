package main

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/dotcommander/agentroom/agentroom"
)

func TestLogWorkerIdentityInterestsAndExecute(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	w := &logWorker{id: "demo-test", logger: slog.New(slog.NewTextHandler(&buf, nil))}

	if got := w.ID(); got != "demo-test" {
		t.Fatalf("ID() = %q", got)
	}
	if got := w.Interests(); len(got) != 1 || got[0] != "*" {
		t.Fatalf("Interests() = %#v, want wildcard", got)
	}
	if err := w.Execute(context.Background(), agentroom.Event{ID: "1-0", Type: "PING", AgentID: "tester"}, nil); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out := buf.String()
	for _, want := range []string{"PING", "tester", "1-0"} {
		if !strings.Contains(out, want) {
			t.Fatalf("log output missing %q: %s", want, out)
		}
	}
}

func TestEnvOr(t *testing.T) {
	t.Setenv("AGENTROOMD_TEST_ENV", "configured")
	if got := envOr("AGENTROOMD_TEST_ENV", "fallback"); got != "configured" {
		t.Fatalf("envOr existing = %q", got)
	}
	if got := envOr("AGENTROOMD_TEST_MISSING", "fallback"); got != "fallback" {
		t.Fatalf("envOr missing = %q", got)
	}
}
