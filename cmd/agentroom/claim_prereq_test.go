package main

import (
	"context"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// TestClaimPrereqGateAcrossCLI exercises the feature end to end through the real
// cobra CLI: register with --prereq, catalog prints prereq=, claim is refused
// before the prerequisite event is posted, and --force bypasses the gate.
//
// Not parallel: it swaps global os.Stdout via runCLIWithStdinOutput.
func TestClaimPrereqGateAcrossCLI(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test: requires miniredis, exercises CLI")
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
	const room = "--repo"
	const repo = "prereq-cli"
	flags := []string{room, repo, "--branch", "test"}

	if err := runCLI(ctx, mr.Addr(), append(flags,
		"register", "BUILD", "build artifacts",
		"--produces", "BUILT", "--requires", "builder", "--prereq", "SOURCE_READY")...); err != nil {
		t.Fatalf("register: %v", err)
	}

	out, err := runCLIWithStdinOutput(ctx, mr.Addr(), "", append(flags, "catalog")...)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	if !strings.Contains(out, "prereq=SOURCE_READY") {
		t.Fatalf("catalog output missing prereq=:\n%s", out)
	}

	// Publish the BUILD task event to obtain a claimable task id.
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	// post via CLI so it lands in the right room stream
	if err := runCLI(ctx, mr.Addr(), append(flags, "post", "BUILD", `{"n":1}`, "--agent", "ci")...); err != nil {
		t.Fatalf("post BUILD: %v", err)
	}
	events, err := recentEvents(ctx, mr.Addr(), repo, "test")
	if err != nil {
		t.Fatalf("recent: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("no events after post")
	}
	taskID := events[len(events)-1]

	// Claim before prerequisite posted: the CLI returns a non-zero error whose
	// message names the missing event type.
	claimErr := runCLI(ctx, mr.Addr(), append(flags, "claim", taskID, "--agent", "agent-a")...)
	if claimErr == nil {
		t.Fatal("claim before prerequisite should fail")
	}
	if !strings.Contains(claimErr.Error(), "prerequisite unmet: SOURCE_READY") {
		t.Fatalf("claim error = %q, want substring \"prerequisite unmet: SOURCE_READY\"", claimErr.Error())
	}

	// --force bypasses the gate and claims unconditionally (criterion 5).
	if err := runCLI(ctx, mr.Addr(), append(flags, "claim", taskID, "--agent", "agent-a", "--force")...); err != nil {
		t.Fatalf("claim --force: %v", err)
	}
}

// recentEvents is a thin helper that reads the room stream directly to recover
// the stream-assigned task id, mirroring the pattern in main_test.go.
func recentEvents(ctx context.Context, addr, repo, branch string) ([]string, error) {
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = rdb.Close() }()
	room := roomCfg(addr, repo, branch)
	stream := room.StreamKey()
	val, err := rdb.XRevRangeN(ctx, stream, "+", "-", 10).Result()
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(val))
	for _, m := range val {
		ids = append(ids, m.ID)
	}
	return ids, nil
}
