package agentroom

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestNormalizeResourcesValidatesSortsAndDeduplicates(t *testing.T) {
	got, err := normalizeResources([]string{"db:clickmojo", "path:internal/db", "db:clickmojo"})
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if len(got) != 2 || got[0] != "db:clickmojo" || got[1] != "path:internal/db" {
		t.Fatalf("normalized = %v", got)
	}
	for _, invalid := range []string{"path:/tmp", "path:../db", "path:internal//db", "path:internal/*", "path:", "missing-kind"} {
		if _, err := normalizeResources([]string{invalid}); err == nil {
			t.Errorf("normalizeResources(%q) succeeded", invalid)
		}
	}
}

func TestAcquireResourcesIsAtomicAndPathAware(t *testing.T) {
	room, _ := newTestRoom(t)
	ctx := context.Background()
	first, err := room.AcquireResources(ctx, ResourceLeaseRequest{Owner: "alice", Resources: []string{"path:internal/db"}, Purpose: "migration"})
	if err != nil {
		t.Fatalf("acquire first: %v", err)
	}
	_, err = room.AcquireResources(ctx, ResourceLeaseRequest{Owner: "bob", Resources: []string{"service:web", "path:internal/db/repo"}})
	var conflict *LeaseConflictError
	if !errors.Is(err, ErrLeaseConflict) || !errors.As(err, &conflict) {
		t.Fatalf("conflict error = %v", err)
	}
	leases, err := room.ResourceLeases(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(leases) != 1 || leases[0].ID != first.ID {
		t.Fatalf("leases after rejected multi-acquire = %+v", leases)
	}
	if _, err := room.AcquireResources(ctx, ResourceLeaseRequest{Owner: "bob", Resources: []string{"path:internal/api"}}); err != nil {
		t.Fatalf("sibling acquire: %v", err)
	}
}

func TestAcquireResourcesConcurrentSingleWinner(t *testing.T) {
	room, _ := newTestRoom(t)
	ctx := context.Background()
	const contenders = 8
	var wg sync.WaitGroup
	wg.Add(contenders)
	results := make(chan error, contenders)
	for i := range contenders {
		go func() {
			defer wg.Done()
			_, err := room.AcquireResources(ctx, ResourceLeaseRequest{Owner: string(rune('a' + i)), Resources: []string{"binary:cm"}})
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	winners := 0
	conflicts := 0
	for err := range results {
		switch {
		case err == nil:
			winners++
		case errors.Is(err, ErrLeaseConflict):
			conflicts++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if winners != 1 || conflicts != contenders-1 {
		t.Fatalf("winners=%d conflicts=%d", winners, conflicts)
	}
}

func TestLeaseExpiryRenewOwnershipAndGuard(t *testing.T) {
	room, mr := newTestRoom(t)
	ctx := context.Background()
	lease, err := room.AcquireResources(ctx, ResourceLeaseRequest{Owner: "alice", Resources: []string{"db:clickmojo"}, TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := room.RenewResources(ctx, lease.ID, "bob", time.Minute); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("renew owner error=%v", err)
	}
	if err := room.GuardResources(ctx, []string{"db:clickmojo"}, "alice", lease.ID, true); err != nil {
		t.Fatalf("owned guard: %v", err)
	}
	if err := room.GuardResources(ctx, []string{"db:clickmojo"}, "bob", "", false); !errors.Is(err, ErrLeaseConflict) {
		t.Fatalf("conflicting guard=%v", err)
	}
	mr.FastForward(2 * time.Minute)
	if _, err := room.RenewResources(ctx, lease.ID, "alice", time.Minute); !errors.Is(err, ErrCoordinationExpired) {
		t.Fatalf("renew expired lease=%v", err)
	}
	if _, err := room.AcquireResources(ctx, ResourceLeaseRequest{Owner: "bob", Resources: []string{"db:clickmojo"}}); err != nil {
		t.Fatalf("reacquire after expiry: %v", err)
	}
}

func TestGuardRequiresDirectionalPathCoverage(t *testing.T) {
	room, _ := newTestRoom(t)
	ctx := context.Background()
	lease, err := room.AcquireResources(ctx, ResourceLeaseRequest{Owner: "alice", Resources: []string{"path:internal/db"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := room.GuardResources(ctx, []string{"path:internal/db/repo"}, "alice", lease.ID, true); err != nil {
		t.Fatalf("ancestor lease should cover descendant: %v", err)
	}
	if err := room.GuardResources(ctx, []string{"path:internal"}, "alice", lease.ID, true); !errors.Is(err, ErrResourceLeaseNeeded) {
		t.Fatalf("descendant lease authorized ancestor: %v", err)
	}
}

func TestLeaseAuditFailureRollsBackState(t *testing.T) {
	room, _ := newTestRoom(t)
	ctx := context.Background()
	if err := room.rdb.Set(ctx, room.cfg.StreamKey(), "not-a-stream", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := room.AcquireResources(ctx, ResourceLeaseRequest{Owner: "alice", Resources: []string{"binary:cm"}}); err == nil {
		t.Fatal("acquire succeeded despite audit stream type failure")
	}
	if leases, err := room.ResourceLeases(ctx); err != nil || len(leases) != 0 {
		t.Fatalf("lease mutation survived failed atomic audit: %+v err=%v", leases, err)
	}
}

func TestResourceLeasesCleansStaleIndexEntries(t *testing.T) {
	room, _ := newTestRoom(t)
	ctx := context.Background()
	if err := room.rdb.ZAdd(ctx, room.cfg.ResourceLeaseIndexKey(), redis.Z{Score: 1, Member: "missing"}).Err(); err != nil {
		t.Fatal(err)
	}
	leases, err := room.ResourceLeases(ctx)
	if err != nil || len(leases) != 0 {
		t.Fatalf("leases=%v err=%v", leases, err)
	}
	if n, err := room.rdb.ZCard(ctx, room.cfg.ResourceLeaseIndexKey()).Result(); err != nil || n != 0 {
		t.Fatalf("stale index size=%d err=%v", n, err)
	}
}
