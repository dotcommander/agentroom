package agentroom

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// InboxRecipientsEndingWith returns durable inbox recipients whose IDs end in
// suffix. It lets a resumed hook recover session-qualified inboxes even after
// the corresponding presence record expires.
func (r *Room) InboxRecipientsEndingWith(ctx context.Context, suffix string) ([]string, error) {
	serverNow, err := r.rdb.Time(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("agentroom: read Redis time: %w", err)
	}
	now := strconv.FormatInt(serverNow.UnixMilli(), 10)
	pipe := r.rdb.Pipeline()
	pipe.ZRemRangeByScore(ctx, r.cfg.InboxRecipientIndexKey(), "-inf", now)
	active := pipe.ZRangeByScore(ctx, r.cfg.InboxRecipientIndexKey(), &redis.ZRangeBy{Min: now, Max: "+inf"})
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("agentroom: list inbox recipients: %w", err)
	}
	recipients := make([]string, 0)
	for _, recipient := range active.Val() {
		if strings.HasSuffix(recipient, suffix) {
			recipients = append(recipients, recipient)
		}
	}
	sort.Strings(recipients)
	return recipients, nil
}

// EnqueueInbox writes ev to recipient's durable directed inbox. The normal room
// stream remains the source of global ordering; source_id ties this inbox copy
// back to the room entry for dedupe during hook injection.
func (r *Room) EnqueueInbox(ctx context.Context, recipient string, ev Event) error {
	if recipient == "" {
		return nil
	}
	args := &redis.XAddArgs{
		Stream: r.cfg.InboxKey(recipient),
		Values: map[string]any{
			"source_stream": r.cfg.StreamKey(),
			"source_id":     ev.ID,
			"type":          ev.Type,
			"agent_id":      ev.AgentID,
			"agent_handle":  ev.AgentHandle,
			"session_id":    ev.SessionID,
			"to":            ev.To,
			"reply_to":      ev.ReplyTo,
			"payload":       []byte(ev.Payload),
			"timestamp":     ev.Timestamp,
		},
	}
	if r.cfg.InboxMaxLen > 0 {
		args.MaxLen = r.cfg.InboxMaxLen
		args.Approx = true
	}
	serverNow, err := r.rdb.Time(ctx).Result()
	if err != nil {
		return fmt.Errorf("agentroom: read Redis time: %w", err)
	}
	expiresAt := float64(1<<63 - 1)
	if r.cfg.InboxTTL > 0 {
		expiresAt = float64(serverNow.Add(r.cfg.InboxTTL).UnixMilli())
	}
	pipe := r.rdb.Pipeline()
	pipe.XAdd(ctx, args)
	pipe.ZRemRangeByScore(ctx, r.cfg.InboxRecipientIndexKey(), "-inf", strconv.FormatInt(serverNow.UnixMilli(), 10))
	pipe.ZAdd(ctx, r.cfg.InboxRecipientIndexKey(), redis.Z{Score: expiresAt, Member: recipient})
	if r.cfg.InboxTTL > 0 {
		pipe.Expire(ctx, r.cfg.InboxKey(recipient), r.cfg.InboxTTL)
		pipe.Expire(ctx, r.cfg.InboxRecipientIndexKey(), r.cfg.InboxTTL)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("agentroom: enqueue inbox %s: %w", recipient, err)
	}
	return nil
}

// InboxSince returns up to count durable directed inbox entries strictly after
// cursor. A missing cursor starts from 0-0, unlike room-stream session cursors,
// so offline messages are delivered instead of baselining to the stream tail.
func (r *Room) InboxSince(ctx context.Context, recipient, cursor string, count int64) ([]InboxEvent, error) {
	if recipient == "" {
		return nil, nil
	}
	if cursor == "" {
		cursor = streamStartID
	}
	if count <= 0 {
		count = 1
	}
	msgs, err := r.rdb.XRangeN(ctx, r.cfg.InboxKey(recipient), cursor, "+", count+1).Result()
	if err != nil {
		return nil, fmt.Errorf("agentroom: inbox since %s: %w", recipient, err)
	}
	events := make([]InboxEvent, 0, len(msgs))
	for _, msg := range msgs {
		if msg.ID == cursor {
			continue
		}
		events = append(events, decodeInboxEvent(msg))
	}
	if int64(len(events)) > count {
		events = events[:count]
	}
	return events, nil
}

// ReadInboxCursor returns the last delivered inbox entry ID for recipient, or ""
// when no cursor exists yet.
func (r *Room) ReadInboxCursor(ctx context.Context, recipient string) (string, error) {
	id, err := r.rdb.Get(ctx, r.cfg.InboxCursorKey(recipient)).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("agentroom: read inbox cursor %s: %w", recipient, err)
	}
	return id, nil
}

// WriteInboxCursor stores the last delivered inbox entry ID for recipient.
func (r *Room) WriteInboxCursor(ctx context.Context, recipient, id string, ttl time.Duration) error {
	if err := r.rdb.Set(ctx, r.cfg.InboxCursorKey(recipient), id, ttl).Err(); err != nil {
		return fmt.Errorf("agentroom: write inbox cursor %s: %w", recipient, err)
	}
	return nil
}

func decodeInboxEvent(msg redis.XMessage) InboxEvent {
	return InboxEvent{
		ID:           msg.ID,
		SourceStream: stringField(msg.Values, "source_stream"),
		SourceID:     stringField(msg.Values, "source_id"),
		Event:        decodeEvent(msg),
	}
}
