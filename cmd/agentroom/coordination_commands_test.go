package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/dotcommander/agentroom/agentroom"
	"github.com/redis/go-redis/v9"
)

func TestLeaseGuardWorkAndStatusCLI(t *testing.T) { //nolint:gocyclo // one CLI lifecycle test validates the coordinated state transitions
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	original := newRedisClient
	newRedisClient = func(string) *redis.Client { return redis.NewClient(&redis.Options{Addr: mr.Addr()}) }
	t.Cleanup(func() { newRedisClient = original })
	t.Setenv("AGENTROOM_SESSION_ID", "session-a")

	run := func(args ...string) (string, error) {
		var out, errOut bytes.Buffer
		base := []string{"--addr", mr.Addr(), "--repo", "coord", "--branch", "test"}
		err := executeWithIO(context.Background(), append(base, args...), &out, &errOut)
		return out.String() + errOut.String(), err
	}

	out, err := run("lease", "acquire", "path:internal/db", "--purpose", "test")
	if err != nil {
		t.Fatalf("lease acquire: %v", err)
	}
	var leaseOutput schemaItem[agentroom.ResourceLease]
	if err := json.Unmarshal([]byte(out), &leaseOutput); err != nil || leaseOutput.SchemaVersion != 1 || leaseOutput.Item.ID == "" {
		t.Fatalf("lease JSON = %q, err=%v", out, err)
	}
	lease := leaseOutput.Item
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	room := agentroom.NewRoom(rdb, roomCfg(mr.Addr(), "coord", "test"))
	presence, err := room.Presence(context.Background())
	_, present := presence[resolveAgent("")]
	if err != nil || !present {
		t.Fatalf("lease actor presence = %+v, err=%v", presence, err)
	}

	_, err = run("guard", "path:internal/db/queries")
	var exitErr interface{ ExitCode() int }
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 3 {
		t.Fatalf("guard error = %v, want exit 3", err)
	}
	if _, err := run("guard", "path:internal/db", "--lease", lease.ID, "--require-held"); err != nil {
		t.Fatalf("owned guard: %v", err)
	}

	if _, err := run("work", "started", "--scope", "path:internal/db", "--summary", "testing"); err != nil {
		t.Fatalf("work started: %v", err)
	}
	out, err = run("status", "--json")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	var snapshot statusSnapshot
	if err := json.Unmarshal([]byte(out), &snapshot); err != nil {
		t.Fatalf("status JSON: %v", err)
	}
	if snapshot.SchemaVersion != 1 || len(snapshot.Leases) != 1 || len(snapshot.Work) != 1 {
		t.Fatalf("status = %+v", snapshot)
	}
}

func TestInboxRecipientsForSessionSurvivePresenceExpiry(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	room := agentroom.NewRoom(rdb, roomCfg(mr.Addr(), "coord", "test"))
	ctx := context.Background()
	if err := room.EnqueueInbox(ctx, "reviewer-session-a", agentroom.Event{ID: "1-0", Type: "WORK_STATUS", To: "reviewer-session-a"}); err != nil {
		t.Fatalf("enqueue inbox: %v", err)
	}

	got := inboxRecipientsForSession(ctx, room, "session-a")
	if !strings.Contains(strings.Join(got, ","), "reviewer-session-a") {
		t.Fatalf("recipients = %v, want expired-presence inbox", got)
	}
}

func TestPostWarningsAndTailFilters(t *testing.T) {
	var warnings bytes.Buffer
	warnAdvisoryPost(&warnings, "CLAIMED", []byte(`{"to":"reviewer"}`), "")
	for _, want := range []string{"data only", "does not prevent collisions"} {
		if !strings.Contains(warnings.String(), want) {
			t.Fatalf("warnings missing %q: %s", want, warnings.String())
		}
	}
	events := []agentroom.Event{
		{ID: "1-0", Type: "BUILD", AgentID: "alice-1", To: "bob-2"},
		{ID: "2-0", Type: "TEST", AgentID: "carol-3", To: "bob-2"},
		{ID: "3-0", Type: "BUILD", AgentID: "alice-1"},
	}
	got := filterTailEvents(events, []string{"BUILD"}, "alice", "bob-2", true)
	if len(got) != 1 || got[0].ID != "1-0" {
		t.Fatalf("filtered events = %+v", got)
	}
	lines := eventLines(agentroom.Event{ID: "1-0", Type: "REPLY", AgentID: "alice", To: "bob", ReplyTo: "0-0"})
	joined := strings.Join(lines, "\n")
	if !strings.Contains(joined, "To: bob") || !strings.Contains(joined, "ReplyTo: 0-0") {
		t.Fatalf("routing fields hidden: %s", joined)
	}
}
