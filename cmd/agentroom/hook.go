package main

import (
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
		Short: "Run as a Claude Code hook (event: session-start)",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			if args[0] != "session-start" {
				return fmt.Errorf("unknown hook event %q (supported: session-start)", args[0])
			}
			return sessionStart(c)
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
		branch = "main"
	}
	return repo, branch
}

// buildDigest gathers lobby announcements, recent local activity, and open
// tasks. Returns "" if redis is unreachable so the session is never blocked.
func buildDigest(ctx context.Context, addr, repo, branch string) string {
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = rdb.Close() }()
	lobby := agentroom.NewRoom(rdb, roomCfg(addr, "lobby", "main"))
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
