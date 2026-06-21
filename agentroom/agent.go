package agentroom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Operational loop timing (not user-facing config — internal scheduling floors).
const (
	// blockDuration bounds each XREADGROUP wait so the loop periodically wakes to
	// check ctx and reclaim stale entries; it also bounds graceful-shutdown
	// latency, observed at the next loop top after cancellation.
	blockDuration = 2 * time.Second
	// claimMinIdle is how long a pending entry must sit idle before another
	// consumer may reclaim it (recovering work abandoned by a crashed worker).
	claimMinIdle = 30 * time.Second
)

// Worker is an execution engine wrapped by a Runtime. Interests selects the
// event types it acts on ("*" matches all); Execute runs the engine for one
// matching event and may publish further events through the supplied Room.
type Worker interface {
	ID() string
	Interests() []string
	Execute(ctx context.Context, ev Event, room *Room) error
}

// Runtime drives one Worker against a Room's stream using a Redis consumer
// group, giving at-least-once delivery that survives restarts: the group
// persists its position in Redis, so events published while the worker is down
// are delivered on reconnect, and entries left pending by a crashed worker are
// reclaimed via XAUTOCLAIM.
//
// Delivery model: each distinct worker TYPE should use its own Config.Group.
// Multiple instances of the same type share a Group (load-balanced) with unique
// Worker.ID() consumer names. Every event on the stream is delivered to each
// group; Interests further filters which delivered events the worker acts on —
// non-matching events are acked and skipped.
type Runtime struct {
	room   *Room
	worker Worker
}

// NewRuntime binds a Worker to a Room.
func NewRuntime(room *Room, worker Worker) *Runtime {
	return &Runtime{room: room, worker: worker}
}

// Listen consumes the room stream until ctx is canceled, dispatching matching
// events to the Worker. It returns ctx.Err() on cancellation.
func (rt *Runtime) Listen(ctx context.Context) error {
	stream := rt.room.cfg.StreamKey()
	group := rt.group()
	if err := rt.ensureGroup(ctx, stream, group); err != nil {
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		res, err := rt.room.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: rt.worker.ID(),
			Streams:  []string{stream, ">"},
			Count:    1,
			Block:    blockDuration,
		}).Result()
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, redis.Nil) {
				rt.reclaim(ctx, stream, group)
				continue
			}
			if !rt.wait(ctx, blockDuration) {
				return ctx.Err()
			}
			continue
		}
		for _, st := range res {
			for _, msg := range st.Messages {
				rt.handle(ctx, stream, group, msg)
			}
		}
	}
}

// handle dispatches one stream entry to the Worker (when it matches Interests)
// and always acks it afterward. An Execute error is published as a durable
// ENGINE_RUNTIME_ERROR event, then the original is acked too — the failure is
// recorded as its own event, so we never redeliver a poison message.
func (rt *Runtime) handle(ctx context.Context, stream, group string, msg redis.XMessage) {
	ev := decodeEvent(msg)
	if matches(rt.worker.Interests(), ev.Type) {
		if err := rt.worker.Execute(ctx, ev, rt.room); err != nil {
			rt.publishError(ctx, err)
		}
	}
	_ = rt.room.rdb.XAck(ctx, stream, group, msg.ID).Err()
}

// reclaim recovers entries left pending by crashed consumers in this group.
func (rt *Runtime) reclaim(ctx context.Context, stream, group string) {
	msgs, _, err := rt.room.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   stream,
		Group:    group,
		Consumer: rt.worker.ID(),
		MinIdle:  claimMinIdle,
		Start:    "0",
		Count:    10,
	}).Result()
	if err != nil {
		return
	}
	for _, msg := range msgs {
		rt.handle(ctx, stream, group, msg)
	}
}

// publishError emits an ENGINE_RUNTIME_ERROR event carrying the failure message
// as well-formed JSON (json.Marshal escapes quotes/newlines).
func (rt *Runtime) publishError(ctx context.Context, execErr error) {
	payload, err := json.Marshal(struct {
		Error string `json:"error"`
	}{Error: execErr.Error()})
	if err != nil {
		payload = json.RawMessage(`{"error":"marshal failed"}`)
	}
	_ = rt.room.Publish(ctx, &Event{
		Type:    EventEngineRuntimeError,
		AgentID: rt.worker.ID(),
		Payload: payload,
	})
}

// ensureGroup creates the consumer group (and the stream) if absent, tolerating
// an already-existing group.
func (rt *Runtime) ensureGroup(ctx context.Context, stream, group string) error {
	err := rt.room.rdb.XGroupCreateMkStream(ctx, stream, group, "$").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("agentroom: create group %s on %s: %w", group, stream, err)
	}
	return nil
}

// wait sleeps for d or until ctx is canceled; it reports whether the wait
// completed (true) rather than being canceled (false).
func (rt *Runtime) wait(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func (rt *Runtime) group() string {
	if rt.room.cfg.Group != "" {
		return rt.room.cfg.Group
	}
	return "agents"
}

// decodeEvent defensively reconstructs an Event from a stream entry, tolerating
// missing or wrongly-typed fields instead of panicking.
func decodeEvent(msg redis.XMessage) Event {
	ev := Event{ID: msg.ID}
	ev.Type = stringField(msg.Values, "type")
	ev.AgentID = stringField(msg.Values, "agent_id")
	if p := stringField(msg.Values, "payload"); p != "" {
		ev.Payload = json.RawMessage(p)
	}
	if ts := stringField(msg.Values, "timestamp"); ts != "" {
		if n, err := strconv.ParseInt(ts, 10, 64); err == nil {
			ev.Timestamp = n
		}
	}
	return ev
}

func stringField(m map[string]any, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}
