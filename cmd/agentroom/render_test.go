package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/dotcommander/agentroom/agentroom"
	"github.com/redis/go-redis/v9"
)

func TestTerminalText(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "plain unicode", in: "reviewer: café", want: "reviewer: café"},
		{name: "CSI", in: "before\x1b[2Jafter", want: "beforeafter"},
		{name: "OSC BEL", in: "before\x1b]0;spoofed\x07after", want: "beforeafter"},
		{name: "OSC ST", in: "before\x1b]52;c;data\x1b\\after", want: "beforeafter"},
		{name: "C0 and C1", in: "a\r\nb\x00c\u0085d", want: "abcd"},
		{name: "raw C1 CSI", in: string([]byte{'a', 0x9b, '2', 'J', 'b'}), want: "ab"},
		{name: "raw C1 OSC", in: string([]byte{'a', 0x9d, '0', ';', 'x', 0x9c, 'b'}), want: "ab"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := terminalText(tt.in); got != tt.want {
				t.Fatalf("terminalText(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestEventLinesSanitizeUntrustedFields(t *testing.T) {
	t.Parallel()
	e := agentroom.Event{
		ID:      "1-0",
		Type:    "PATCH\x1b[2J_READY",
		AgentID: "peer\x1b]0;spoof\x07",
		Payload: []byte("first\nsecond\x1b[?25l"),
	}
	got := strings.Join(eventLines(e), "\n")
	for _, unsafe := range []string{"\x1b", "[2J", "]0;", "[?25l", "first\nsecond"} {
		if strings.Contains(got, unsafe) {
			t.Fatalf("rendered event contains unsafe sequence %q: %q", unsafe, got)
		}
	}
	for _, want := range []string{"PATCH_READY", "peer", "firstsecond"} {
		if !strings.Contains(got, want) {
			t.Fatalf("rendered event missing %q: %q", want, got)
		}
	}
}

func TestCoordinationErrorsSanitizeAtTerminalBoundary(t *testing.T) {
	t.Parallel()
	err := &agentroom.LeaseConflictError{Resource: "service:build\x1b[2J", Conflicts: "service:build", Owner: "peer\x1b]0;spoof\x07"}
	got := terminalText(err.Error())
	if strings.ContainsAny(got, "\x1b\r\n") || strings.Contains(got, "[2J") || strings.Contains(got, "]0;") {
		t.Fatalf("rendered error contains terminal controls: %q", got)
	}
}

func TestJoinDescSanitizesUntrustedFields(t *testing.T) {
	t.Parallel()
	payload, err := json.Marshal(struct {
		Role      string `json:"role"`
		WorkingOn string `json:"working_on"`
	}{Role: "builder\x1b[2J", WorkingOn: "release\nspoof"})
	if err != nil {
		t.Fatalf("marshal join payload: %v", err)
	}
	got := joinDesc(payload)
	if got != "builder: releasespoof" {
		t.Fatalf("joinDesc() = %q, want sanitized description", got)
	}
}

func TestDirectTerminalCommandsSanitizeRoomContent(t *testing.T) { //nolint:gocyclo // one integration test exercises every human render path
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)

	originalClient := newRedisClient
	newRedisClient = func(string) *redis.Client {
		return redis.NewClient(&redis.Options{Addr: mr.Addr()})
	}
	t.Cleanup(func() { newRedisClient = originalClient })

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	room := agentroom.NewRoom(rdb, roomCfg(mr.Addr(), "sanitize", "main"))
	taskType := "PATCH\x1b[2J_READY"
	if err := room.RegisterTask(ctx, agentroom.TaskDef{Type: taskType, Description: "review\x1b]0;spoof\x07"}); err != nil {
		t.Fatalf("register task: %v", err)
	}
	if err := room.Publish(ctx, &agentroom.Event{Type: taskType, AgentID: "peer\x1b]0;spoof\x07", Payload: []byte("first\nsecond")}); err != nil {
		t.Fatalf("publish event: %v", err)
	}
	if err := room.Heartbeat(ctx, "alice\x1b[2J", "builder\x1b]0;spoof\x07", time.Minute); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	var out bytes.Buffer
	for _, command := range [][]string{
		{"--repo", "sanitize", "--branch", "main", "tail"},
		{"--repo", "sanitize", "--branch", "main", "catalog"},
		{"--repo", "sanitize", "--branch", "main", "open"},
		{"--repo", "sanitize", "--branch", "main", "who"},
	} {
		if err := executeWithIO(ctx, command, &out, &out); err != nil {
			t.Fatalf("executeWithIO(%q): %v", command, err)
		}
	}
	if _, err := room.AcquireResources(ctx, agentroom.ResourceLeaseRequest{Owner: "owner\x1b[2J", Resources: []string{"service:build\nspoof"}, Purpose: "release\x1b]0;spoof\x07"}); err != nil {
		t.Fatalf("acquire resources: %v", err)
	}
	if _, err := room.RequestWindow(ctx, agentroom.WindowRequest{Owner: "operator\x1b[2J", Resources: []string{"service:deploy\nspoof"}, Purpose: "deploy"}); err != nil {
		t.Fatalf("request window: %v", err)
	}
	for _, command := range [][]string{
		{"--repo", "sanitize", "--branch", "main", "lease", "list"},
		{"--repo", "sanitize", "--branch", "main", "window", "status"},
	} {
		if err := executeWithIO(ctx, command, &out, &out); err != nil {
			t.Fatalf("executeWithIO(%q): %v", command, err)
		}
	}

	got := out.String()
	for _, unsafe := range []string{"\x1b", "[2J", "]0;", "first\nsecond"} {
		if strings.Contains(got, unsafe) {
			t.Fatalf("direct command output contains unsafe sequence %q: %q", unsafe, got)
		}
	}
	for _, want := range []string{"PATCH_READY", "review", "firstsecond", "alice", "builder", "owner", "service:buildspoof", "operator", "service:deployspoof"} {
		if !strings.Contains(got, want) {
			t.Fatalf("direct command output missing %q: %q", want, got)
		}
	}
}

func TestCoordinationDigestLinesSanitizeRoomContent(t *testing.T) {
	got := strings.Join(append(
		windowDigestLines([]agentroom.CoordinationWindow{{ID: "window", State: "active", Owner: "operator\x1b[2J", Resources: []string{"service:deploy\nspoof"}}}),
		leaseDigestLines([]agentroom.ResourceLease{{ID: "lease", Owner: "owner\x1b]0;spoof\x07", Resources: []string{"service:build\nspoof"}}})...,
	), "\n")
	for _, unsafe := range []string{"\x1b", "[2J", "]0;", "deploy\nspoof", "build\nspoof"} {
		if strings.Contains(got, unsafe) {
			t.Fatalf("coordination digest contains unsafe sequence %q: %q", unsafe, got)
		}
	}
}
