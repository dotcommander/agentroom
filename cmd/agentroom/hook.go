package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

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
		Short: "Run as a Claude Code hook (events: session-start, session-end)",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			switch args[0] {
			case "session-start":
				return sessionStart(c)
			case "session-end":
				return sessionEnd(c)
			default:
				return fmt.Errorf("unknown hook event %q (supported: session-start, session-end)", args[0])
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
	repo, branch := resolveRoom(in.CWD)
	selfID := shortSession(in.SessionID)
	joinLobby(c.Context(), addr, repo, branch, in.SessionID)
	writeLocalHeartbeat(c.Context(), addr, repo, branch, selfID)
	digest := buildDigest(c.Context(), addr, repo, branch, selfID)
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

// resolveRoom picks the room namespace: REPO_ID/BRANCH_NAME env win, else the
// cwd's basename and "main".
func resolveRoom(cwd string) (string, string) {
	repo := os.Getenv("REPO_ID")
	if repo == "" {
		repo = filepath.Base(cwd)
	}
	branch := os.Getenv("BRANCH_NAME")
	if branch == "" {
		branch = defaultBranch
	}
	return repo, branch
}

// buildDigest reports who is currently present in the local room (live, from TTL
// presence keys) plus open tasks. Returns "" if redis is unreachable
// so the session is never blocked. The noisy lobby and raw recent-activity
// feeds are intentionally omitted — use `agentroom tail` for the full feed.
func buildDigest(ctx context.Context, addr, repo, branch, selfID string) string {
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = rdb.Close() }()
	local := agentroom.NewRoom(rdb, roomCfg(addr, repo, branch))

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
		"(free-form; no required schema)",
		"",
		"== who's here ==",
	}
	lines = append(lines, presenceLines(pres, selfID)...)
	lines = append(lines, "", "== open tasks you could claim ==")
	lines = append(lines, openLines(open)...)
	lines = append(lines, "", "How to use: `agentroom tail` to watch the full feed, `agentroom open` for work, `claim <id>` before you start, `post <type> <payload>` to announce, `done <id>` when finished.")
	return strings.Join(lines, "\n")
}

// joinLobby posts a best-effort AGENT_JOINED to the global lobby room so every
// session is visible cross-repo, recording which local room it belongs to. It
// publishes with StreamTTL=0 so it never re-arms expiry on the persistent lobby
// stream (the welcome banner relies on that). Never fails the session.
func joinLobby(ctx context.Context, addr, repo, branch, sessionID string) {
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = rdb.Close() }()
	cfg := roomCfg(addr, lobbyRepo, defaultBranch)
	cfg.StreamTTL = 0
	lobby := agentroom.NewRoom(rdb, cfg)
	payload, err := json.Marshal(map[string]any{"room": fmt.Sprintf("%s:%s", repo, branch)})
	if err != nil {
		return
	}
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
func writeLocalHeartbeat(ctx context.Context, addr, repo, branch, agentID string) {
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = rdb.Close() }()
	local := agentroom.NewRoom(rdb, roomCfg(addr, repo, branch))
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
	repo, branch := resolveRoom(in.CWD)
	ask, entries := transcriptSummary(in.TranscriptPath)

	rdb := redis.NewClient(&redis.Options{Addr: addr})
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
	_ = room.Publish(c.Context(), &agentroom.Event{
		Type:    "SESSION_ENDED",
		AgentID: shortSession(in.SessionID),
		Payload: payload,
	})
	_ = room.ClearPresence(c.Context(), shortSession(in.SessionID))
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
