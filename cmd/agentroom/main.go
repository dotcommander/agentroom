// Command agentroom is the interactive CLI for an agentroom event mesh: tail
// recent activity, post events, browse the task catalog, and claim/complete
// tasks. It is the interface an agent (or a human) uses to join a room. Room
// coordinates come from --addr/--repo/--branch or REDIS_ADDR/REPO_ID/BRANCH_NAME.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/dotcommander/agentchat/agentroom"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cobra"
)

const (
	lobbyRepo     = "lobby"
	defaultBranch = "main"
)

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// run wires signal-based cancellation and executes the root command. It is split
// from main so os.Exit never runs while a defer (stop) is still pending.
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return rootCmd().ExecuteContext(ctx)
}

func outln(args ...any)               { _, _ = fmt.Fprintln(os.Stdout, args...) }
func outf(format string, args ...any) { _, _ = fmt.Fprintf(os.Stdout, format, args...) }

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// defaultRepo is the default room namespace: REPO_ID if set, else the current
// directory's basename -- so ad-hoc CLI use targets this repo's room, matching
// what the SessionStart hook derives.
func defaultRepo() string {
	if v := os.Getenv("REPO_ID"); v != "" {
		return v
	}
	if wd, err := os.Getwd(); err == nil {
		return filepath.Base(wd)
	}
	return "demo"
}

func rootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:          "agentroom",
		Short:        "Join an agentroom event mesh: tail, post, and claim work",
		SilenceUsage: true,
	}
	root.PersistentFlags().String("addr", envOr("REDIS_ADDR", "localhost:6379"), "Redis address")
	root.PersistentFlags().String("repo", defaultRepo(), "repo id (room namespace)")
	root.PersistentFlags().String("branch", envOr("BRANCH_NAME", "main"), "branch name (room namespace)")
	root.AddCommand(tailCmd(), postCmd(), catalogCmd(), registerCmd(), openCmd(), claimCmd(), doneCmd(), hookCmd(), welcomeCmd())
	return root
}

// roomFromFlags builds a Room and its client from the persistent flags. The
// caller must close the returned client.
func roomFromFlags(cmd *cobra.Command) (*agentroom.Room, *redis.Client) {
	addr, _ := cmd.Flags().GetString("addr")
	repo, _ := cmd.Flags().GetString("repo")
	branch, _ := cmd.Flags().GetString("branch")
	cfg := agentroom.DefaultConfig()
	cfg.RedisAddr = addr
	cfg.RepoID = repo
	cfg.BranchName = branch
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	return agentroom.NewRoom(rdb, cfg), rdb
}

func tailCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "tail",
		Short: "Print the most recent events on the room stream",
		Args:  cobra.NoArgs,
		RunE: func(c *cobra.Command, _ []string) error {
			room, rdb := roomFromFlags(c)
			defer func() { _ = rdb.Close() }()
			n, _ := c.Flags().GetInt64("count")
			events, err := room.Recent(c.Context(), n)
			if err != nil {
				return err
			}
			for _, e := range events {
				printEvent(e)
			}
			return nil
		},
	}
	cmd.Flags().Int64("count", 20, "number of recent events to show")
	return cmd
}

func postCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "post <type> [payload]",
		Short: "Publish an event to the room (payload is free-form; arg or omitted)",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(c *cobra.Command, args []string) error {
			room, rdb := roomFromFlags(c)
			defer func() { _ = rdb.Close() }()
			agent, _ := c.Flags().GetString("agent")
			var payload []byte
			if len(args) == 2 {
				payload = []byte(args[1])
			}
			ev := &agentroom.Event{Type: args[0], AgentID: agent, Payload: payload}
			if err := room.Publish(c.Context(), ev); err != nil {
				return err
			}
			// Opportunistic heartbeat: every CLI call refreshes the agent's
			// presence TTL key — this is the heartbeat in a daemonless CLI.
			writeHeartbeat(c.Context(), room, agent, "")
			outf("posted %s as %s (entry %s)\n", ev.Type, agent, ev.ID)
			return nil
		},
	}
	cmd.Flags().String("agent", defaultAgent(), "agent id to attribute the event to")
	return cmd
}

func catalogCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "catalog",
		Short: "List the task types registered in this room",
		Args:  cobra.NoArgs,
		RunE: func(c *cobra.Command, _ []string) error {
			room, rdb := roomFromFlags(c)
			defer func() { _ = rdb.Close() }()
			defs, err := room.Catalog(c.Context())
			if err != nil {
				return err
			}
			if len(defs) == 0 {
				outln("(catalog is empty)")
				return nil
			}
			for _, d := range defs {
				outf("%-16s %s\n", d.Type, d.Description)
				outf("%16s produces=%s requires=%s\n", "", d.Produces, d.Requires)
			}
			return nil
		},
	}
}

func registerCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "register <type> <description>",
		Short: "Advertise a task type in the room catalog",
		Args:  cobra.ExactArgs(2),
		RunE: func(c *cobra.Command, args []string) error {
			room, rdb := roomFromFlags(c)
			defer func() { _ = rdb.Close() }()
			produces, _ := c.Flags().GetString("produces")
			requires, _ := c.Flags().GetString("requires")
			def := agentroom.TaskDef{Type: args[0], Description: args[1], Produces: produces, Requires: requires}
			if err := room.RegisterTask(c.Context(), def); err != nil {
				return err
			}
			outf("registered task type %s\n", def.Type)
			return nil
		},
	}
	cmd.Flags().String("produces", "", "event type emitted on success")
	cmd.Flags().String("requires", "", "capability an agent needs to handle it")
	return cmd
}

func openCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "open",
		Short: "List open (unclaimed, undone) tasks an agent could pick up",
		Args:  cobra.NoArgs,
		RunE: func(c *cobra.Command, _ []string) error {
			room, rdb := roomFromFlags(c)
			defer func() { _ = rdb.Close() }()
			n, _ := c.Flags().GetInt64("count")
			tasks, err := room.OpenTasks(c.Context(), n)
			if err != nil {
				return err
			}
			if len(tasks) == 0 {
				outln("(no open tasks)")
				return nil
			}
			for _, tk := range tasks {
				outf("%s  %-16s %s\n", tk.ID, tk.Type, tk.Payload)
			}
			return nil
		},
	}
	cmd.Flags().Int64("count", 50, "how many recent stream entries to scan")
	return cmd
}

func claimCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "claim <task-id>",
		Short: "Atomically claim a task so no other agent duplicates it",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			room, rdb := roomFromFlags(c)
			defer func() { _ = rdb.Close() }()
			agent, _ := c.Flags().GetString("agent")
			ttl, _ := c.Flags().GetDuration("ttl")
			ok, err := room.Claim(c.Context(), args[0], agent, ttl)
			if err != nil {
				return err
			}
			if !ok {
				outf("task %s is already claimed or done -- skip it\n", args[0])
				return nil
			}
			writeHeartbeat(c.Context(), room, agent, "")
			outf("claimed task %s as %s (lease %s)\n", args[0], agent, ttl)
			return nil
		},
	}
	cmd.Flags().String("agent", defaultAgent(), "agent id claiming the task")
	cmd.Flags().Duration("ttl", 5*time.Minute, "claim lease before another agent may reclaim")
	return cmd
}

func doneCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "done <task-id> [result]",
		Short: "Mark a claimed task complete (releasing it)",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(c *cobra.Command, args []string) error {
			room, rdb := roomFromFlags(c)
			defer func() { _ = rdb.Close() }()
			var result []byte
			if len(args) == 2 {
				result = []byte(args[1])
			}
			if err := room.Complete(c.Context(), args[0], result); err != nil {
				return err
			}
			outf("completed task %s\n", args[0])
			return nil
		},
	}
}

func printEvent(e agentroom.Event) {
	ts := ""
	if e.Timestamp > 0 {
		ts = time.Unix(0, e.Timestamp).Format("15:04:05")
	}
	outf("%-16s %-8s %-16s %s\n", e.ID, ts, e.Type, e.AgentID)
	if len(e.Payload) > 0 {
		outf("    %s\n", e.Payload)
	}
}

func defaultAgent() string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return "cli@" + h
	}
	return "cli"
}
