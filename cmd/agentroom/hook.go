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
	CWD string `json:"cwd"`
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
	digest := buildDigest(c.Context(), addr, repo, branch)
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

// buildDigest gathers lobby announcements, recent local activity, and open
// tasks. Returns "" if redis is unreachable so the session is never blocked.
func buildDigest(ctx context.Context, addr, repo, branch string) string {
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = rdb.Close() }()
	lobby := agentroom.NewRoom(rdb, roomCfg(addr, lobbyRepo, defaultBranch))
	local := agentroom.NewRoom(rdb, roomCfg(addr, repo, branch))

	lobbyEvents, err := lobby.Recent(ctx, 3)
	if err != nil {
		return ""
	}
	localEvents, _ := local.Recent(ctx, 8)
	open, _ := local.OpenTasks(ctx, 50)

	lines := []string{
		fmt.Sprintf("agentroom -- shared agent mesh (this room: %s:%s)", repo, branch),
		"",
		"== lobby (global announcements) ==",
	}
	lines = append(lines, eventLines(lobbyEvents)...)
	lines = append(lines, "", "== this room -- recent activity ==")
	lines = append(lines, eventLines(localEvents)...)
	lines = append(lines, "", "== open tasks you could claim ==")
	lines = append(lines, openLines(open)...)
	lines = append(lines, "", "How to use: `agentroom tail` to watch, `agentroom open` for work, `claim <id>` before you start, `post <type> <payload>` to announce, `done <id>` when finished.")
	return strings.Join(lines, "\n")
}

func eventLines(events []agentroom.Event) []string {
	if len(events) == 0 {
		return []string{"(none)"}
	}
	lines := make([]string, 0, len(events))
	for _, e := range events {
		lines = append(lines, fmt.Sprintf("  [%s] %s -- %s", e.Type, e.AgentID, previewPayload(e.Payload)))
	}
	return lines
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

func previewPayload(p []byte) string {
	s := strings.TrimSpace(string(p))
	r := []rune(s)
	if len(r) > 200 {
		return string(r[:200]) + "..."
	}
	return s
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
