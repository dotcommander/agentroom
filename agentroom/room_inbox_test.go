package agentroom

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestInboxRecipientIndexPrunesExpiredMembersWhileRoomStaysActive(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	cfg := Config{RepoID: "auth", BranchName: "main", InboxTTL: 10 * time.Minute}
	room := NewRoom(rdb, cfg)
	ctx := context.Background()
	start := time.Unix(2_000_000_000, 0)
	mr.SetTime(start)

	if err := room.EnqueueInbox(ctx, "old-session", Event{ID: "1-0", Type: "NOTICE"}); err != nil {
		t.Fatalf("enqueue old recipient: %v", err)
	}
	mr.SetTime(start.Add(5 * time.Minute))
	mr.FastForward(5 * time.Minute)
	if err := room.EnqueueInbox(ctx, "new-session", Event{ID: "2-0", Type: "NOTICE"}); err != nil {
		t.Fatalf("enqueue new recipient: %v", err)
	}
	mr.SetTime(start.Add(11 * time.Minute))
	mr.FastForward(6 * time.Minute)

	recipients, err := room.InboxRecipientsEndingWith(ctx, "-session")
	if err != nil {
		t.Fatalf("list recipients: %v", err)
	}
	if len(recipients) != 1 || recipients[0] != "new-session" {
		t.Fatalf("recipients=%v, want only live recipient", recipients)
	}
}
