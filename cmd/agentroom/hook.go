package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/dotcommander/agentroom/agentroom"
)

const (
	eventAgentJoined  = "AGENT_JOINED"
	eventSessionEnded = "SESSION_ENDED"
	summaryKeySession = "session"
)

type roomRef struct {
	Addr   string
	Repo   string
	Branch string
}

// sessionStartInput is the subset of the SessionStart hook stdin we use.
type sessionStartInput struct {
	CWD       string `json:"cwd"`
	SessionID string `json:"session_id"`
	Prompt    string `json:"prompt"`
}

type hookCommand struct {
	Event string `arg:"" enum:"session-start,user-prompt-submit,session-end"`
}

func (c *hookCommand) Run(ctx context.Context, g *globals) error {
	switch c.Event {
	case "session-start":
		return sessionStart(ctx, g.Addr, g.Out)
	case "user-prompt-submit":
		return userPromptSubmit(ctx, g.Addr, g.Out)
	case "session-end":
		return sessionEnd(ctx, g.Addr)
	default:
		return fmt.Errorf("unknown hook event %q (supported: session-start, user-prompt-submit, session-end)", c.Event)
	}
}

const (
	hookOpTimeout     = 2 * time.Second
	maxDeltaEvents    = 20
	maxInboxEvents    = 20
	nullPayloadString = "null"
)

// joinCursor picks a new session's initial read cursor: replay the last
// JoinReplayWindow of events so a session landing just after a peer's
// CONFIG_CHANGED/WORK_COMPLETED still sees it (the join-trap), or — when replay
// is disabled (non-positive window) or the stream is unreadable — baseline to
// the tail so no full backlog is dumped. The replay read stays bounded by
// maxDeltaEvents.
func joinCursor(ctx context.Context, room *agentroom.Room) string {
	if w := room.Config().JoinReplayWindow; w > 0 {
		return room.ReplayCursorFrom(time.Now(), w)
	}
	last, err := room.LastID(ctx)
	if err != nil {
		return ""
	}
	return last
}

// userPromptSubmit injects room events that landed since this session last spoke,
// then advances the session's cursor. Nothing new -> no output (zero context
// cost). NEVER fails the session: any error (redis down, timeout, bad input)
// yields no output and exit 0.
func userPromptSubmit(baseCtx context.Context, addr string, out io.Writer) error {
	in := readSessionInput()
	repo, branch := resolveRoom(baseCtx, in.CWD)
	if in.SessionID == "" {
		return nil
	}
	ref := roomRef{Addr: addr, Repo: repo, Branch: branch}
	selfID := shortSession(in.SessionID)

	ctx, cancel := context.WithTimeout(baseCtx, hookOpTimeout)
	defer cancel()

	rdb := newRedisClient(addr)
	defer func() { _ = rdb.Close() }()
	room := agentroom.NewRoom(rdb, roomCfg(addr, repo, branch))
	writeInferredPresence(ctx, room, selfID, in.Prompt)
	// Keep any "<handle>-<token>" named entry for this session live too, so it
	// does not expire while only the anonymous session line is refreshed.
	_ = room.RefreshSessionPresence(ctx, selfID, room.Config().PresenceTTL)

	// Compose this room's delta with the cross-repo lobby delta. Each section is
	// independent: a quiet local room no longer suppresses cross-repo signal, and
	// the local section is unchanged (deltaDigest) when present.
	recipients := inboxRecipientsForSession(ctx, room, selfID)
	inbox, renderedSourceIDs := prepareInboxDelta(ctx, room, recipients)
	local := prepareLocalDelta(ctx, room, localDeltaOptions{
		SessionID:     in.SessionID,
		Repo:          repo,
		Branch:        branch,
		SkipTo:        recipientSet(recipients),
		SkipSourceIDs: renderedSourceIDs,
	})
	lobby := prepareLobbyDelta(ctx, rdb, ref, in.SessionID, selfID)
	prepared := []preparedSection{inbox, local, lobby}
	sections := make([]string, 0, 3)
	if inbox.text != "" {
		sections = append(sections, inbox.text)
	}
	if local.text != "" {
		sections = append(sections, local.text)
	}
	if lobby.text != "" {
		sections = append(sections, lobby.text)
	}
	if len(sections) == 0 {
		commitHookSections(context.WithoutCancel(baseCtx), prepared...)
		return nil
	}
	if !writeHookOutput(out, "UserPromptSubmit", strings.Join(sections, "\n\n")) {
		return nil
	}
	commitHookSections(context.WithoutCancel(baseCtx), prepared...)
	return nil
}

func writeHookOutput(w io.Writer, eventName, additionalContext string) bool {
	out := map[string]any{
		"hookSpecificOutput": map[string]any{
			"hookEventName":     eventName,
			"additionalContext": additionalContext,
		},
	}
	data, err := json.Marshal(out)
	if err != nil {
		return false
	}
	_, err = fmt.Fprintln(w, string(data))
	return err == nil
}

func commitHookSections(ctx context.Context, sections ...preparedSection) {
	ctx, cancel := context.WithTimeout(ctx, hookOpTimeout)
	defer cancel()
	commitPrepared(ctx, sections...)
}

const inferredPresencePrefix = "current prompt: "

func writeInferredPresence(ctx context.Context, room *agentroom.Room, agentID, prompt string) {
	desc := inferredPresenceDesc(prompt)
	if desc == "" {
		writeHeartbeat(ctx, room, agentID, "")
		return
	}
	pres, err := room.Presence(ctx)
	if err != nil {
		writeHeartbeat(ctx, room, agentID, "")
		return
	}
	current := pres[agentID]
	if current != "" && !strings.HasPrefix(current, inferredPresencePrefix) {
		writeHeartbeat(ctx, room, agentID, "")
		return
	}
	writeHeartbeat(ctx, room, agentID, desc)
}

func inferredPresenceDesc(prompt string) string {
	prompt = clip(prompt, 140)
	if prompt == "" {
		return ""
	}
	return inferredPresencePrefix + prompt
}

// deltaDigest renders new room events as a compact context block: one line per
// event (type, agent, clipped payload).
func deltaDigest(repo, branch string, events []agentroom.Event) string {
	lines := []string{
		fmt.Sprintf("agentroom -- %d new event(s) in %s:%s since your last message:", len(events), repo, branch),
	}
	for _, ev := range events {
		agent := ev.AgentID
		if agent == "" {
			agent = "?"
		}
		line := fmt.Sprintf("  %s  %s", ev.Type, agent)
		if p := clip(string(ev.Payload), 120); p != "" && p != nullPayloadString {
			line += "  " + p
		}
		lines = append(lines, line)
	}
	lines = append(lines, "", "(`agentroom tail` for the full feed)")
	return strings.Join(lines, "\n")
}
