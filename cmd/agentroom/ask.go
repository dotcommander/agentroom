package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dotcommander/agentroom/agentroom"
)

const (
	eventAsk   = "ASK"
	eventReply = "REPLY"
)

type askCommand struct {
	Message string        `arg:""`
	Agent   string        `help:"Agent id asking the question."`
	To      string        `required:"" help:"Live agent handle or unique roster prefix to ask."`
	Timeout time.Duration `default:"10m" help:"Maximum time to wait for the matching reply."`
}

func (c *askCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()

	agent := resolveAgent(c.Agent)
	rawTo := c.To
	to, err := resolveLiveTarget(ctx, room, rawTo)
	if err != nil {
		return err
	}
	if c.Timeout <= 0 {
		return errors.New("--timeout must be greater than 0")
	}

	lockToken := fmt.Sprintf("%s:%d", agent, time.Now().UnixNano())
	ok, err := room.BeginAsk(ctx, agent, lockToken, c.Timeout)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("agent %s already has a pending ask", agent)
	}
	defer func() { _ = room.EndAsk(context.WithoutCancel(ctx), agent, lockToken) }()

	handle, sessionID := eventIdentity(agent)
	ev := &agentroom.Event{Type: eventAsk, AgentID: agent, AgentHandle: handle, SessionID: sessionID, To: to, Payload: []byte(c.Message)}
	if err := room.Publish(ctx, ev); err != nil {
		return err
	}
	if inboxRecipient := durableInboxRecipient(rawTo); inboxRecipient != "" {
		if err := room.EnqueueInbox(ctx, inboxRecipient, *ev); err != nil {
			return fmt.Errorf("asked %s as %s (entry %s), but durable inbox enqueue failed: %w", to, agent, ev.ID, err)
		}
	}

	writeHeartbeat(ctx, room, agent, "")
	outf("asked %s as %s (entry %s)\n", to, agent, ev.ID)

	reply, err := waitForReply(ctx, room, replyWait{
		AskID:             ev.ID,
		ExpectedSender:    to,
		ExpectedRecipient: agent,
		Timeout:           c.Timeout,
	})
	if err != nil {
		return err
	}
	printEvent(g.Out, reply)
	return nil
}

type replyCommand struct {
	AskID   string `arg:"" name:"ask-id"`
	Message string `arg:""`
	Agent   string `help:"Agent id sending the reply."`
}

func (c *replyCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()

	ask, ok, err := room.EventByID(ctx, c.AskID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("ask %s not found", c.AskID)
	}
	if ask.Type != eventAsk {
		return fmt.Errorf("event %s is %s, not %s", c.AskID, ask.Type, eventAsk)
	}

	agent := resolveAgent(c.Agent)
	handle, sessionID := eventIdentity(agent)
	ev := &agentroom.Event{
		Type:        eventReply,
		AgentID:     agent,
		AgentHandle: handle,
		SessionID:   sessionID,
		To:          ask.AgentID,
		ReplyTo:     ask.ID,
		Payload:     []byte(c.Message),
	}
	if err := room.Publish(ctx, ev); err != nil {
		return err
	}
	writeHeartbeat(ctx, room, agent, "")
	outf("replied to %s as %s (entry %s)\n", ask.ID, agent, ev.ID)
	return nil
}

func resolveLiveTarget(ctx context.Context, room *agentroom.Room, raw string) (string, error) {
	if raw == "" {
		return "", errors.New("--to is required")
	}
	if strings.Contains(raw, ":") {
		return "", fmt.Errorf("--to %q is a room key; ask requires one live agent", raw)
	}
	target, candidates, err := matchLiveTarget(ctx, room, raw)
	if err != nil {
		return "", err
	}
	switch len(candidates) {
	case 0:
		return "", fmt.Errorf("--to %q does not match a live agent", raw)
	case 1:
		return target, nil
	default:
		return "", fmt.Errorf("--to %q is ambiguous; candidates: %s", raw, strings.Join(candidates, ", "))
	}
}

type replyWait struct {
	AskID             string
	ExpectedSender    string
	ExpectedRecipient string
	Timeout           time.Duration
}

func waitForReply(ctx context.Context, room *agentroom.Room, wait replyWait) (agentroom.Event, error) {
	if wait.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, wait.Timeout)
		defer cancel()
	}
	lastID := wait.AskID
	for {
		events, err := room.Wait(ctx, lastID, 2*time.Second, 10)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && wait.Timeout > 0 {
				return agentroom.Event{}, fmt.Errorf("ask timed out after %s", wait.Timeout)
			}
			return agentroom.Event{}, err
		}
		for _, ev := range events {
			lastID = ev.ID
			if ev.ReplyTo == wait.AskID && ev.AgentID == wait.ExpectedSender && ev.To == wait.ExpectedRecipient {
				return ev, nil
			}
		}
	}
}
