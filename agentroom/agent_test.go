package agentroom

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type captureWorker struct {
	id        string
	interests []string
	got       chan Event
	failOn    string
}

func (w *captureWorker) ID() string          { return w.id }
func (w *captureWorker) Interests() []string { return w.interests }

func (w *captureWorker) Execute(ctx context.Context, ev Event, _ *Room) error {
	if w.failOn != "" && ev.Type == w.failOn {
		return errors.New("boom \"quoted\"\nmultiline")
	}
	select {
	case w.got <- ev:
	case <-ctx.Done():
	}
	return nil
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	_ = redis.Nil
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(3 * time.Second)
	for {
		if cond() {
			return
		}
		select {
		case <-deadline:
			t.Fatal("condition not met within deadline")
		case <-ticker.C:
		}
	}
}

func TestRuntimeListenDispatchesMatchingEvents(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, _ := newTestRoom(t)
	room.cfg.Group = "dispatch-group"
	stream := room.cfg.StreamKey()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := room.rdb.XGroupCreateMkStream(ctx, stream, room.cfg.Group, "$").Err(); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := room.Publish(ctx, &Event{Type: "IGNORED", AgentID: "p"}); err != nil {
		t.Fatalf("publish ignored: %v", err)
	}
	if err := room.Publish(ctx, &Event{Type: "WANTED", AgentID: "p", Payload: []byte(`{"k":1}`)}); err != nil {
		t.Fatalf("publish wanted: %v", err)
	}

	w := &captureWorker{id: "w1", interests: []string{"WANTED"}, got: make(chan Event, 4)}
	rt := NewRuntime(room, w)
	errc := make(chan error, 1)
	go func() { errc <- rt.Listen(ctx) }()

	select {
	case ev := <-w.got:
		if ev.Type != "WANTED" {
			t.Errorf("dispatched event type = %q, want WANTED", ev.Type)
		}
		if string(ev.Payload) != `{"k":1}` {
			t.Errorf("payload = %q, want {\"k\":1}", ev.Payload)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for matching event")
	}

	cancel()
	select {
	case err := <-errc:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Listen returned %v, want context.Canceled or nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Error("Listen did not return after cancel")
	}
}

func TestRuntimeListenPublishesExecuteError(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, _ := newTestRoom(t)
	room.cfg.Group = "error-group"
	stream := room.cfg.StreamKey()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := room.rdb.XGroupCreateMkStream(ctx, stream, room.cfg.Group, "$").Err(); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := room.Publish(ctx, &Event{Type: "TASK", AgentID: "p"}); err != nil {
		t.Fatalf("publish task: %v", err)
	}

	w := &captureWorker{id: "w1", interests: []string{"TASK"}, got: make(chan Event, 4), failOn: "TASK"}
	rt := NewRuntime(room, w)
	errc := make(chan error, 1)
	go func() { errc <- rt.Listen(ctx) }()

	waitFor(t, func() bool {
		msgs, err := room.rdb.XRange(ctx, stream, "-", "+").Result()
		if err != nil {
			return false
		}
		for _, m := range msgs {
			if m.Values["type"] != EventEngineRuntimeError {
				continue
			}
			payload, _ := m.Values["payload"].(string)
			var pe struct {
				Error string `json:"error"`
			}
			if err := json.Unmarshal([]byte(payload), &pe); err != nil {
				t.Fatalf("error payload is not valid JSON: %v (raw=%q)", err, payload)
			}
			if pe.Error == "" {
				t.Fatal("error payload has empty message")
			}
			return true
		}
		return false
	})

	cancel()
	select {
	case <-errc:
	case <-time.After(3 * time.Second):
		t.Error("Listen did not return after cancel")
	}
}
