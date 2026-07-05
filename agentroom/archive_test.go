package agentroom

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestClient(t *testing.T) *redis.Client {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func seed(t *testing.T, rdb *redis.Client, stream string, n int) {
	t.Helper()
	ctx := context.Background()
	for i := range n {
		if err := rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			Values: map[string]any{"n": i},
		}).Err(); err != nil {
			t.Fatalf("seed %s: %v", stream, err)
		}
	}
}

func TestRunDailySweepCompactsAndPreservesLateAppend(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	rdb := newTestClient(t)
	ctx := context.Background()
	stream := "repo:auth:main:events"
	seed(t, rdb, stream, 3)

	var archived int
	persist := func(key string, events []redis.XMessage) error {
		archived = len(events)
		// An event arrives mid-sweep, after the snapshot was taken.
		return rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: key,
			Values: map[string]any{"n": 99},
		}).Err()
	}
	a := NewArchiver(rdb, 3, persist)
	if err := a.RunDailySweep(ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if archived != 3 {
		t.Errorf("archived %d entries, want 3", archived)
	}
	remaining, err := rdb.XLen(ctx, stream).Result()
	if err != nil {
		t.Fatalf("xlen: %v", err)
	}
	if remaining != 1 {
		t.Errorf("remaining = %d, want 1 (the late append must survive XDel)", remaining)
	}
}

func TestRunDailySweepSkipsBelowThreshold(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	rdb := newTestClient(t)
	ctx := context.Background()
	stream := "repo:auth:main:events"
	seed(t, rdb, stream, 2)

	called := false
	persist := func(string, []redis.XMessage) error {
		called = true
		return nil
	}
	a := NewArchiver(rdb, 5, persist)
	if err := a.RunDailySweep(ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if called {
		t.Error("persist called for a stream below threshold")
	}
	remaining, err := rdb.XLen(ctx, stream).Result()
	if err != nil {
		t.Fatalf("xlen: %v", err)
	}
	if remaining != 2 {
		t.Errorf("remaining = %d, want 2 (nothing compacted)", remaining)
	}
}

func TestRunDailySweepScansMultipleStreams(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	rdb := newTestClient(t)
	ctx := context.Background()
	seed(t, rdb, "repo:a:main:events", 3)
	seed(t, rdb, "repo:b:main:events", 3)

	persisted := map[string]int{}
	persist := func(key string, events []redis.XMessage) error {
		persisted[key] = len(events)
		return nil
	}
	a := NewArchiver(rdb, 3, persist)
	if err := a.RunDailySweep(ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if len(persisted) != 2 {
		t.Errorf("compacted %d streams, want 2 (SCAN must find both)", len(persisted))
	}
}
