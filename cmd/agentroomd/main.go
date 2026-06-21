// Command agentroomd is a minimal proof harness for the agentroom mesh: it wires
// a Redis client, registers a trivial logging Worker, runs the Runtime, publishes
// a sample event, and demonstrates the Archiver sweep. Namespace and address come
// from REPO_ID / BRANCH_NAME / REDIS_ADDR env vars (with DefaultConfig fallbacks).
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/dotcommander/agentchat/agentroom"
	"github.com/redis/go-redis/v9"
)

// logWorker logs every event (it is interested in all of them).
type logWorker struct {
	id     string
	logger *slog.Logger
}

func (w *logWorker) ID() string          { return w.id }
func (w *logWorker) Interests() []string { return []string{"*"} }

func (w *logWorker) Execute(_ context.Context, ev agentroom.Event, _ *agentroom.Room) error {
	w.logger.Info("event", "type", ev.Type, "agent", ev.AgentID, "id", ev.ID)
	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	cfg := agentroom.DefaultConfig()
	cfg.RedisAddr = envOr("REDIS_ADDR", cfg.RedisAddr)
	cfg.RepoID = envOr("REPO_ID", "demo")
	cfg.BranchName = envOr("BRANCH_NAME", "main")

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer func() { _ = rdb.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	room := agentroom.NewRoom(rdb, cfg)
	rt := agentroom.NewRuntime(room, &logWorker{id: "demo-engine", logger: logger})
	go func() {
		if err := rt.Listen(ctx); err != nil && ctx.Err() == nil {
			logger.Error("runtime stopped", "err", err)
		}
	}()

	payload, _ := json.Marshal(map[string]string{"file": "main.go"})
	if err := room.Publish(ctx, &agentroom.Event{Type: "AST_PARSED", AgentID: "demo-engine", Payload: payload}); err != nil {
		logger.Error("publish failed", "err", err)
	}

	archiver := agentroom.NewArchiver(rdb, cfg.ArchiveThreshold, func(key string, events []redis.XMessage) error {
		logger.Info("archived", "stream", key, "count", len(events))
		return nil
	})
	if err := archiver.RunDailySweep(ctx); err != nil {
		logger.Error("sweep failed", "err", err)
	}

	logger.Info("agentroomd running; press Ctrl-C to stop")
	<-ctx.Done()
	logger.Info("shutting down")
}
