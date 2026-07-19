package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/dotcommander/agentroom/agentroom"
)

type tailCommand struct {
	Count int64    `help:"Deprecated alias for --limit."`
	Limit int64    `default:"100" help:"Maximum events to show (capped at 5000)."`
	Since string   `help:"Only events after a duration (for example 15m) or stream ID."`
	Types []string `name:"type" help:"Only this event type; repeatable."`
	From  string   `help:"Only events from this exact agent ID or logical handle."`
	ToMe  bool     `name:"to-me" help:"Only events actually directed to this agent."`
	JSON  bool     `help:"Write one JSON object per line (JSONL)."`
	Agent string   `help:"Agent id to attribute presence to."`
}

func (c *tailCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()
	limit := c.Limit
	if c.Count > 0 {
		limit = c.Count
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 5000 {
		limit = 5000
	}
	scanLimit := limit
	if len(c.Types) > 0 || c.From != "" || c.ToMe {
		scanLimit = 5000
	}
	events, err := tailEvents(ctx, room, c.Since, scanLimit)
	if err != nil {
		return err
	}
	agent := resolveAgent(c.Agent)
	events = filterTailEvents(events, c.Types, c.From, agent, c.ToMe)
	if int64(len(events)) > limit {
		events = events[len(events)-int(limit):]
	}
	for _, event := range events {
		if c.JSON {
			if err := json.NewEncoder(g.Out).Encode(struct {
				SchemaVersion int `json:"schema_version"`
				agentroom.Event
			}{SchemaVersion: 1, Event: event}); err != nil {
				return fmt.Errorf("encode tail event: %w", err)
			}
			continue
		}
		printEvent(g.Out, event)
	}
	writeHeartbeat(ctx, room, agent, "")
	return nil
}

func tailEvents(ctx context.Context, room *agentroom.Room, since string, limit int64) ([]agentroom.Event, error) {
	if since == "" {
		return room.Recent(ctx, limit)
	}
	cursor := since
	if duration, err := time.ParseDuration(since); err == nil {
		cursor = room.ReplayCursorFrom(time.Now(), duration)
	} else if !strings.Contains(since, "-") {
		return nil, errors.New("--since must be a duration or Redis stream ID")
	}
	return room.RecentSince(ctx, cursor, limit)
}

func filterTailEvents(events []agentroom.Event, types []string, from, agent string, toMe bool) []agentroom.Event {
	wantedTypes := make(map[string]bool, len(types))
	for _, eventType := range types {
		wantedTypes[eventType] = true
	}
	out := make([]agentroom.Event, 0, len(events))
	for _, event := range events {
		if len(wantedTypes) > 0 && !wantedTypes[event.Type] {
			continue
		}
		if from != "" && event.AgentID != from && !strings.HasPrefix(event.AgentID, sanitizeHandle(from)+"-") {
			continue
		}
		if toMe && event.To != agent {
			continue
		}
		out = append(out, event)
	}
	return out
}

type postCommand struct {
	Type    string `arg:""`
	Payload string `arg:"" optional:""`
	Agent   string `help:"Agent id to attribute the event to."`
	To      string `help:"Directed recipient: a room key or agent handle (empty = broadcast)."`
}

func (c *postCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()
	agent := resolveAgent(c.Agent)
	rawTo := c.To
	to, err := resolveTarget(ctx, room, rawTo)
	if err != nil {
		return err
	}
	if rawTo != "" && to == rawTo && !strings.Contains(rawTo, ":") {
		_, _ = fmt.Fprintf(g.Err, "warning: no live agent matches --to %q; posting verbatim\n", rawTo)
	}
	inboxRecipient := durableInboxRecipient(rawTo)
	var payload []byte
	if c.Payload != "" {
		payload = []byte(c.Payload)
	}
	warnAdvisoryPost(g.Err, c.Type, payload, rawTo)
	handle, sessionID := eventIdentity(agent)
	ev := &agentroom.Event{Type: c.Type, AgentID: agent, AgentHandle: handle, SessionID: sessionID, To: to, Payload: payload}
	if err := room.Publish(ctx, ev); err != nil {
		return err
	}
	if inboxRecipient != "" {
		if err := room.EnqueueInbox(ctx, inboxRecipient, *ev); err != nil {
			return fmt.Errorf("posted %s as %s (entry %s), but durable inbox enqueue failed: %w", ev.Type, agent, ev.ID, err)
		}
	}
	writeHeartbeat(ctx, room, agent, joinDesc(payload))
	outf("posted %s as %s (entry %s)\n", ev.Type, agent, ev.ID)
	return nil
}

func warnAdvisoryPost(w interface{ Write([]byte) (int, error) }, eventType string, payload []byte, rawTo string) {
	if rawTo == "" && len(payload) > 0 {
		var object map[string]json.RawMessage
		if json.Unmarshal(payload, &object) == nil {
			if _, ok := object["to"]; ok {
				_, _ = fmt.Fprintln(w, "warning: payload field \"to\" is data only; use --to for actual delivery")
			}
		}
	}
	switch eventType {
	case "CLAIMED", "CLAIM_UPDATED":
		_, _ = fmt.Fprintln(w, "warning: advisory claim event does not prevent collisions; use task claims or resource leases")
	case eventReply:
		_, _ = fmt.Fprintln(w, "warning: free-form REPLY has no correlation; use `agentroom reply`")
	}
}

type waitCommand struct {
	Agent   string        `help:"Agent id to match when --to-me is set."`
	ToMe    bool          `name:"to-me" help:"Only unblock for events directed to this agent."`
	Timeout time.Duration `help:"Maximum time to wait (0 = wait until interrupted)."`
}

func (c *waitCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()
	agent := resolveAgent(c.Agent)
	writeHeartbeat(ctx, room, agent, "")
	ev, err := waitForEvent(ctx, room, agent, c.ToMe, c.Timeout)
	if err != nil {
		return err
	}
	printEvent(g.Out, ev)
	return nil
}

func waitForEvent(ctx context.Context, room *agentroom.Room, agent string, toMe bool, timeout time.Duration) (agentroom.Event, error) {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	lastID, err := room.LastID(ctx)
	if err != nil {
		return agentroom.Event{}, err
	}
	for {
		events, err := room.Wait(ctx, lastID, 2*time.Second, 10)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && timeout > 0 {
				return agentroom.Event{}, fmt.Errorf("wait timed out after %s", timeout)
			}
			return agentroom.Event{}, err
		}
		for _, ev := range events {
			lastID = ev.ID
			if toMe && ev.To != agent {
				continue
			}
			return ev, nil
		}
	}
}

// resolveTarget turns a human --to value into the live qualified roster ID when
// possible. Room keys pass through; exact IDs and then unique prefixes win.
func resolveTarget(ctx context.Context, room *agentroom.Room, raw string) (string, error) {
	if raw == "" || strings.Contains(raw, ":") {
		return raw, nil
	}
	target, candidates, err := matchLiveTarget(ctx, room, raw)
	if err != nil {
		return "", err
	}
	switch len(candidates) {
	case 0:
		return raw, nil
	case 1:
		return target, nil
	default:
		return "", fmt.Errorf("--to %q is ambiguous; candidates: %s", raw, strings.Join(candidates, ", "))
	}
}

func matchLiveTarget(ctx context.Context, room *agentroom.Room, raw string) (string, []string, error) {
	pres, err := room.PresenceDetailed(ctx)
	if err != nil {
		return "", nil, err
	}
	if _, ok := pres[raw]; ok {
		return raw, []string{raw}, nil
	}
	logical := make([]string, 0)
	for id, entry := range pres {
		if entry.Identity.Handle == raw {
			logical = append(logical, id)
		}
	}
	sort.Strings(logical)
	if len(logical) == 1 {
		return logical[0], logical, nil
	}
	if len(logical) > 1 {
		return "", logical, nil
	}
	candidates := make([]string, 0)
	for id := range pres {
		if strings.HasPrefix(id, raw) {
			candidates = append(candidates, id)
		}
	}
	sort.Strings(candidates)
	if len(candidates) == 1 {
		return candidates[0], candidates, nil
	}
	return "", candidates, nil
}

func durableInboxRecipient(rawTo string) string {
	if rawTo == "" || strings.Contains(rawTo, ":") {
		return ""
	}
	return sanitizeHandle(rawTo)
}
