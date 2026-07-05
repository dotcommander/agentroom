package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/dotcommander/agentroom/agentroom"
	"github.com/spf13/cobra"
)

const (
	eventAsk   = "ASK"
	eventReply = "REPLY"
)

func askCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ask <message>",
		Short: "Ask one live agent and block until its correlated reply arrives",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			room, rdb := roomFromFlags(c)
			defer func() { _ = rdb.Close() }()

			agent := resolveAgent(c)
			rawTo, _ := c.Flags().GetString("to")
			to, err := resolveLiveTarget(c.Context(), room, rawTo)
			if err != nil {
				return err
			}
			timeout, _ := c.Flags().GetDuration("timeout")
			if timeout <= 0 {
				return errors.New("--timeout must be greater than 0")
			}

			lockToken := fmt.Sprintf("%s:%d", agent, time.Now().UnixNano())
			ok, err := room.BeginAsk(c.Context(), agent, lockToken, timeout)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("agent %s already has a pending ask", agent)
			}
			defer func() { _ = room.EndAsk(context.WithoutCancel(c.Context()), agent, lockToken) }()

			ev := &agentroom.Event{Type: eventAsk, AgentID: agent, To: to, Payload: []byte(args[0])}
			if err := room.Publish(c.Context(), ev); err != nil {
				return err
			}
			if inboxRecipient := durableInboxRecipient(rawTo); inboxRecipient != "" {
				if err := room.EnqueueInbox(c.Context(), inboxRecipient, *ev); err != nil {
					return fmt.Errorf("asked %s as %s (entry %s), but durable inbox enqueue failed: %w", to, agent, ev.ID, err)
				}
			}

			writeHeartbeat(c.Context(), room, agent, "")
			outf("asked %s as %s (entry %s)\n", to, agent, ev.ID)

			reply, err := waitForReply(c.Context(), room, replyWait{
				AskID:             ev.ID,
				ExpectedSender:    to,
				ExpectedRecipient: agent,
				Timeout:           timeout,
			})
			if err != nil {
				return err
			}
			printEvent(reply)
			return nil
		},
	}
	cmd.Flags().String("agent", defaultAgent(), "agent id asking the question")
	cmd.Flags().String("to", "", "live agent handle or unique roster prefix to ask")
	cmd.Flags().Duration("timeout", 10*time.Minute, "maximum time to wait for the matching reply")
	return cmd
}

func replyCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reply <ask-id> <message>",
		Short: "Reply to an ask event, automatically targeting the asker",
		Args:  cobra.ExactArgs(2),
		RunE: func(c *cobra.Command, args []string) error {
			room, rdb := roomFromFlags(c)
			defer func() { _ = rdb.Close() }()

			ask, ok, err := room.EventByID(c.Context(), args[0])
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("ask %s not found", args[0])
			}
			if ask.Type != eventAsk {
				return fmt.Errorf("event %s is %s, not %s", args[0], ask.Type, eventAsk)
			}

			agent := resolveAgent(c)
			ev := &agentroom.Event{
				Type:    eventReply,
				AgentID: agent,
				To:      ask.AgentID,
				ReplyTo: ask.ID,
				Payload: []byte(args[1]),
			}
			if err := room.Publish(c.Context(), ev); err != nil {
				return err
			}
			writeHeartbeat(c.Context(), room, agent, "")
			outf("replied to %s as %s (entry %s)\n", ask.ID, agent, ev.ID)
			return nil
		},
	}
	cmd.Flags().String("agent", defaultAgent(), "agent id sending the reply")
	return cmd
}

func resolveLiveTarget(ctx context.Context, room *agentroom.Room, raw string) (string, error) {
	if raw == "" {
		return "", errors.New("--to is required")
	}
	if strings.Contains(raw, ":") {
		return "", fmt.Errorf("--to %q is a room key; ask requires one live agent", raw)
	}
	pres, err := room.PresenceDetailed(ctx)
	if err != nil {
		return "", err
	}
	if _, ok := pres[raw]; ok {
		return raw, nil
	}
	candidates := make([]string, 0)
	for id := range pres {
		if strings.HasPrefix(id, raw) {
			candidates = append(candidates, id)
		}
	}
	sort.Strings(candidates)
	switch len(candidates) {
	case 0:
		return "", fmt.Errorf("--to %q does not match a live agent", raw)
	case 1:
		return candidates[0], nil
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
