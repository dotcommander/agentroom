package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/dotcommander/agentchat/agentroom"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cobra"
)

// sessionStartInput is the subset of the SessionStart hook stdin we use.
type sessionStartInput struct {
	CWD       string `json:"cwd"`
	SessionID string `json:"session_id"`
}

func hookCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "hook <event>",
		Short: "Run as a Claude Code hook (events: session-start, user-prompt-submit, session-end)",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			switch args[0] {
			case "session-start":
				return sessionStart(c)
			case "user-prompt-submit":
				return userPromptSubmit(c)
			case "session-end":
				return sessionEnd(c)
			default:
				return fmt.Errorf("unknown hook event %q (supported: session-start, user-prompt-submit, session-end)", args[0])
			}
		},
	}
}

// sessionStart reads the SessionStart payload, builds a digest of lobby + local
// room activity, and emits it as additionalContext. It NEVER fails the session:
// any error (redis down, bad input) yields no output and exit 0.
func sessionStart(c *cobra.Command) error {
	addr, _ := c.Flags().GetString("addr")
	in := readSessionInput()
	if in.SessionID == "" {
		return nil
	}
	repo, branch := resolveRoom(c.Context(), in.CWD)
	selfID := shortSession(in.SessionID)
	rdb := newRedisClient(addr)
	defer func() { _ = rdb.Close() }()
	joinLobby(c.Context(), rdb, addr, repo, branch, in.SessionID)
	writeLocalHeartbeat(c.Context(), rdb, addr, repo, branch, selfID)
	seedCursor(c.Context(), rdb, addr, repo, branch, in.SessionID)
	seedRoomCursor(c.Context(), lobbyRoom(rdb, addr), in.SessionID)
	digest := buildDigest(c.Context(), rdb, addr, repo, branch, selfID)
	if digest == "" {
		return nil
	}
	out := map[string]any{
		"hookSpecificOutput": map[string]any{
			"hookEventName":     "SessionStart",
			"additionalContext": digest,
		},
	}
	data, err := json.Marshal(out)
	if err != nil {
		return nil
	}
	outln(string(data))
	return nil
}

func readSessionInput() sessionStartInput {
	var in sessionStartInput
	if raw, err := io.ReadAll(io.LimitReader(os.Stdin, 1<<20)); err == nil {
		_ = json.Unmarshal(raw, &in)
	}
	if in.CWD == "" {
		in.CWD, _ = os.Getwd()
	}
	return in
}

// buildDigest reports who is currently present in the local room (live, from TTL
// presence keys) plus open tasks. Returns "" if redis is unreachable
// so the session is never blocked. The noisy lobby and raw recent-activity
// feeds are intentionally omitted — use `agentroom tail` for the full feed.
func buildDigest(ctx context.Context, rdb *redis.Client, addr, repo, branch, selfID string) string {
	local := agentroom.NewRoom(rdb, roomCfg(addr, repo, branch))

	ctx, cancel := context.WithTimeout(ctx, hookOpTimeout)
	defer cancel()

	// Presence is liveness-backed: read the live TTL key set, not a fold of the
	// event stream. Crashed agents drop within PresenceTTL with no SESSION_ENDED.
	pres, err := local.Presence(ctx)
	if err != nil {
		return ""
	}
	open, _ := local.OpenTasks(ctx, 50)

	lines := []string{
		fmt.Sprintf("agentroom -- shared agent mesh (this room: %s:%s)", repo, branch),
		"",
		"Sign in -- tell the room who you are and what you're here to do:",
		"  agentroom post AGENT_JOINED '{\"role\":\"<what you do>\",\"working_on\":\"<your goal>\"}' --agent <your-handle>",
		"(free-form; no required schema. Pick a short plain handle -- your session id is",
		"appended automatically, so two agents sharing a handle stay distinct.)",
		"",
		"== who's here (live TTL presence; absence is not proof nobody's working) ==",
	}
	lines = append(lines, presenceLines(pres, selfID, claimsCounter(ctx, local))...)
	lines = append(lines, "", "== open tasks you could claim ==")
	lines = append(lines, openLines(open)...)
	lines = append(lines, "", "How to use: `agentroom who` to see who's here, `agentroom tail` to print recent events, `agentroom open` for work, `claim <id>` before you start, `post <type> <payload>` to announce, `done <id>` when finished.")
	lines = append(lines, "Post globally (all rooms, e.g. cross-repo announcements): `agentroom post <type> <payload> --repo lobby --agent <handle>`.")
	return strings.Join(lines, "\n")
}

// joinLobby posts a best-effort AGENT_JOINED to the global lobby room so every
// session is visible cross-repo, recording which local room it belongs to. It
// publishes with StreamTTL=0 so it never re-arms expiry on the persistent lobby
// stream (the welcome banner relies on that). Never fails the session.
func joinLobby(ctx context.Context, rdb *redis.Client, addr, repo, branch, sessionID string) {
	cfg := roomCfg(addr, lobbyRepo, defaultBranch)
	cfg.StreamTTL = 0
	lobby := agentroom.NewRoom(rdb, cfg)
	payload, err := json.Marshal(map[string]any{"room": fmt.Sprintf("%s:%s", repo, branch)})
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, hookOpTimeout)
	defer cancel()
	_ = lobby.Publish(ctx, &agentroom.Event{
		Type:    "AGENT_JOINED",
		AgentID: shortSession(sessionID),
		Payload: payload,
	})
}

// writeLocalHeartbeat best-effort registers this session's presence in the local
// room with a TTL key, so it appears in "who's here" without depending on the
// event fold. The description starts empty; a later `post AGENT_JOINED` refreshes
// it with role/working_on. Never fails the session.
func writeLocalHeartbeat(ctx context.Context, rdb *redis.Client, addr, repo, branch, agentID string) {
	local := agentroom.NewRoom(rdb, roomCfg(addr, repo, branch))
	ctx, cancel := context.WithTimeout(ctx, hookOpTimeout)
	defer cancel()
	writeHeartbeat(ctx, local, agentID, "")
}

func openLines(tasks []agentroom.Task) []string {
	if len(tasks) == 0 {
		return []string{"(none)"}
	}
	lines := make([]string, 0, len(tasks))
	for _, t := range tasks {
		lines = append(lines, fmt.Sprintf("  %s  %s", t.ID, t.Type))
	}
	return lines
}

func roomCfg(addr, repo, branch string) agentroom.Config {
	cfg := agentroom.DefaultConfig()
	cfg.RedisAddr = addr
	cfg.RepoID = repo
	cfg.BranchName = branch
	return cfg
}

// sessionEndInput is the subset of the SessionEnd hook stdin we use.
type sessionEndInput struct {
	CWD            string `json:"cwd"`
	SessionID      string `json:"session_id"`
	TranscriptPath string `json:"transcript_path"`
}

// sessionEnd posts a SESSION_ENDED event to the local room with a best-effort
// summary (the session's opening ask + transcript size), so the next agent
// inherits what just happened. Best-effort: never fails session teardown.
func sessionEnd(c *cobra.Command) error {
	addr, _ := c.Flags().GetString("addr")
	var in sessionEndInput
	if raw, err := io.ReadAll(io.LimitReader(os.Stdin, 1<<20)); err == nil {
		_ = json.Unmarshal(raw, &in)
	}
	if in.CWD == "" {
		in.CWD, _ = os.Getwd()
	}
	if in.SessionID == "" {
		return nil
	}
	repo, branch := resolveRoom(c.Context(), in.CWD)
	ask, entries := transcriptSummary(in.TranscriptPath)

	rdb := newRedisClient(addr)
	defer func() { _ = rdb.Close() }()
	room := agentroom.NewRoom(rdb, roomCfg(addr, repo, branch))

	summary := map[string]any{"session": shortSession(in.SessionID), "entries": entries}
	if ask != "" {
		summary["worked_on"] = ask
	}
	payload, err := json.Marshal(summary)
	if err != nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(c.Context(), hookOpTimeout)
	defer cancel()
	_ = room.Publish(ctx, &agentroom.Event{
		Type:    "SESSION_ENDED",
		AgentID: shortSession(in.SessionID),
		Payload: payload,
	})
	_ = room.ClearPresence(ctx, shortSession(in.SessionID))
	return nil
}

func shortSession(id string) string {
	if id == "" {
		return "session"
	}
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// transcriptSummary returns the session's opening user ask and the number of
// transcript entries. Best-effort: ("", 0) on any problem.
func transcriptSummary(path string) (string, int) {
	if path == "" {
		return "", 0
	}
	f, err := os.Open(path) //nolint:gosec // transcript_path is supplied by the trusted Claude Code runtime
	if err != nil {
		return "", 0
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	entries, ask := 0, ""
	for sc.Scan() {
		entries++
		if ask == "" {
			ask = firstUserText(sc.Bytes())
		}
	}
	return ask, entries
}

func firstUserText(line []byte) string {
	var e struct {
		Message struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"message"`
	}
	if err := json.Unmarshal(line, &e); err != nil || e.Message.Role != "user" {
		return ""
	}
	return clip(extractText(e.Message.Content), 160)
}

// extractText pulls plain text from a content field that may be a raw string or
// an array of {type,text} blocks.
func extractText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &blocks) == nil {
		for _, b := range blocks {
			if b.Type == "text" && b.Text != "" {
				return b.Text
			}
		}
	}
	return ""
}

func clip(s string, n int) string {
	s = strings.TrimSpace(s)
	r := []rune(s)
	if len(r) > n {
		return string(r[:n]) + "..."
	}
	return s
}

const (
	hookOpTimeout  = 2 * time.Second
	maxDeltaEvents = 20
)

// seedCursor sets this session's read cursor to the current stream tail so the
// first UserPromptSubmit delta covers only events published after sign-in, never
// the full backlog. Best-effort: never fails the session.
func seedCursor(ctx context.Context, rdb *redis.Client, addr, repo, branch, sessionID string) {
	seedRoomCursor(ctx, agentroom.NewRoom(rdb, roomCfg(addr, repo, branch)), sessionID)
}

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
func userPromptSubmit(c *cobra.Command) error {
	addr, _ := c.Flags().GetString("addr")
	in := readSessionInput()
	repo, branch := resolveRoom(c.Context(), in.CWD)
	if in.SessionID == "" {
		return nil
	}
	selfID := shortSession(in.SessionID)

	ctx, cancel := context.WithTimeout(c.Context(), hookOpTimeout)
	defer cancel()

	rdb := newRedisClient(addr)
	defer func() { _ = rdb.Close() }()
	room := agentroom.NewRoom(rdb, roomCfg(addr, repo, branch))
	writeHeartbeat(ctx, room, selfID, "")
	// Keep any "<handle>-<token>" named entry for this session live too, so it
	// does not expire while only the anonymous session line is refreshed.
	_ = room.RefreshSessionPresence(ctx, selfID, room.Config().PresenceTTL)

	// Compose this room's delta with the cross-repo lobby delta. Each section is
	// independent: a quiet local room no longer suppresses cross-repo signal, and
	// the local section is unchanged (deltaDigest) when present.
	sections := make([]string, 0, 2)
	if s := localDelta(ctx, room, in.SessionID, repo, branch); s != "" {
		sections = append(sections, s)
	}
	if s := lobbyDelta(ctx, rdb, addr, repo, branch, in.SessionID, selfID); s != "" {
		sections = append(sections, s)
	}
	if len(sections) == 0 {
		return nil
	}

	out := map[string]any{
		"hookSpecificOutput": map[string]any{
			"hookEventName":     "UserPromptSubmit",
			"additionalContext": strings.Join(sections, "\n\n"),
		},
	}
	data, err := json.Marshal(out)
	if err != nil {
		return nil
	}
	outln(string(data))
	return nil
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
		if p := clip(string(ev.Payload), 120); p != "" && p != "null" {
			line += "  " + p
		}
		lines = append(lines, line)
	}
	lines = append(lines, "", "(`agentroom tail` for the full feed)")
	return strings.Join(lines, "\n")
}
