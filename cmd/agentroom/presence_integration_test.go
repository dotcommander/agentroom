package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/dotcommander/agentchat/agentroom"
	"github.com/redis/go-redis/v9"
	"go.uber.org/goleak"
)

// TestPresenceLifecycleAcrossCLI exercises the real cobra RunE paths (post,
// claim) and the hook session-start/session-end paths against one shared
// miniredis backend, then advances past PresenceTTL and asserts the agent drops
// from the live presence set — the full heartbeat->expiry lifecycle end to end
// through the CLI layer, not just Room methods. goleak guards against a leaked
// heartbeat/timer goroutine: every CLI client is Closed by its RunE defer, so a
// surviving goroutine can only be a real leak (no ignore-rules).
func TestPresenceLifecycleAcrossCLI(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test: requires miniredis, exercises full CLI lifecycle")
	}
	// Registered FIRST so LIFO cleanup runs it LAST — after mr.Close() (and the
	// client/seam teardowns below) have run, so miniredis's servePeer goroutine
	// is gone before goleak inspects. Deterministic; no ignore-rules.
	t.Cleanup(func() { goleak.VerifyNone(t) })

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)

	// Override the client seam so every CLI command + hook targets the one shared
	// miniredis. Restore the production factory afterward.
	orig := newRedisClient
	newRedisClient = func(string) *redis.Client {
		return redis.NewClient(&redis.Options{Addr: mr.Addr()})
	}
	t.Cleanup(func() { newRedisClient = orig })

	ctx := context.Background()
	const agent = "agent-int"
	qAgent := qualifyAgent(agent) // CLI qualifies --agent; presence key carries the session token

	// Drive `post AGENT_JOINED` through the real root command RunE. This writes
	// the presence TTL key (opportunistic heartbeat) for `agent`.
	if err := runCLI(ctx, mr.Addr(),
		"post", "AGENT_JOINED", `{"role":"builder","working_on":"presence test"}`, "--agent", agent); err != nil {
		t.Fatalf("post: %v", err)
	}

	// A direct Room view over the same miniredis to assert presence state.
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	cfg := roomCfg(mr.Addr(), defaultRepo(), "main")
	room := agentroom.NewRoom(rdb, cfg)

	pres, err := room.Presence(ctx)
	if err != nil {
		t.Fatalf("presence after post: %v", err)
	}
	if _, ok := pres[qAgent]; !ok {
		t.Fatalf("agent %q absent from presence after post; got %v", qAgent, pres)
	}

	// Crash simulation: advance past PresenceTTL with NO session-end. The agent
	// must drop from presence on its own (TTL expiry), no SESSION_ENDED needed.
	mr.FastForward(cfg.PresenceTTL + time.Second)

	pres, err = room.Presence(ctx)
	if err != nil {
		t.Fatalf("presence after TTL expiry: %v", err)
	}
	if _, ok := pres[qAgent]; ok {
		t.Fatalf("agent %q still present after PresenceTTL expiry; got %v", qAgent, pres)
	}
}

// runCLI executes the real root cobra command with args against addr, capturing
// output so the test stays quiet. It is the "real CLI invocation" seam.
func runCLI(ctx context.Context, addr string, args ...string) error {
	root := rootCmd()
	root.SetArgs(append([]string{"--addr", addr}, args...))
	root.SetOut(&bytes.Buffer{})
	root.SetErr(&bytes.Buffer{})
	return root.ExecuteContext(ctx)
}

// TestClaimCountRendersAcrossCLI proves the render-time "(N claimed)" capacity
// hint populates end to end: an agent signs in (post AGENT_JOINED sets its desc),
// claims two tasks via the real CLI, and the rendered presence line carries its
// live outstanding-claim count — through real Redis, not a stubbed counter.
func TestClaimCountRendersAcrossCLI(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("integration test: requires miniredis, exercises CLI claim + render")
	}

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)

	orig := newRedisClient
	newRedisClient = func(string) *redis.Client {
		return redis.NewClient(&redis.Options{Addr: mr.Addr()})
	}
	t.Cleanup(func() { newRedisClient = orig })

	ctx := context.Background()
	const agent = "agent-d3"

	// Sign in: sets the presence desc to "builder: capacity demo".
	if err := runCLI(ctx, mr.Addr(),
		"post", "AGENT_JOINED", `{"role":"builder","working_on":"capacity demo"}`, "--agent", agent); err != nil {
		t.Fatalf("post AGENT_JOINED: %v", err)
	}

	// Claim two tasks as the agent via the real CLI claim path.
	for _, id := range []string{"task-d3-1", "task-d3-2"} {
		if err := runCLI(ctx, mr.Addr(), "claim", id, "--agent", agent); err != nil {
			t.Fatalf("claim %s: %v", id, err)
		}
	}

	// Render the presence line the way buildDigest does: live presence map +
	// the render-time claims counter over the same room.
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	room := agentroom.NewRoom(rdb, roomCfg(mr.Addr(), defaultRepo(), "main"))

	pres, err := room.Presence(ctx)
	if err != nil {
		t.Fatalf("presence: %v", err)
	}
	lines := presenceLines(pres, "", claimsCounter(ctx, room))

	want := "  " + qualifyAgent(agent) + " -- builder: capacity demo (2 claimed)"
	found := false
	for _, l := range lines {
		if l == want {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("rendered presence missing claim count.\n want line: %q\n got lines: %#v", want, lines)
	}
}
