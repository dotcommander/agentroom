package agentroom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Task-state key suffixes appended to Config.TaskKey(id).
const (
	taskOwnerSuffix = ":owner"
	taskDoneSuffix  = ":done"
)

// RegisterTask advertises a task definition in the room catalog so other agents
// can discover what work exists and what each type expects. Re-registering a
// type overwrites it. The catalog inherits the stream's idle-expiry lease.
func (r *Room) RegisterTask(ctx context.Context, def TaskDef) error {
	data, err := json.Marshal(def)
	if err != nil {
		return fmt.Errorf("agentroom: marshal task def %s: %w", def.Type, err)
	}
	pipe := r.rdb.Pipeline()
	pipe.HSet(ctx, r.cfg.CatalogKey(), def.Type, data)
	if r.cfg.StreamTTL > 0 {
		pipe.Expire(ctx, r.cfg.CatalogKey(), r.cfg.StreamTTL)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("agentroom: register task %s: %w", def.Type, err)
	}
	return nil
}

// Catalog returns every registered task definition, keyed by type — the entry
// point for an agent discovering what the room knows about. Malformed
// entries are skipped rather than failing the whole lookup.
func (r *Room) Catalog(ctx context.Context) (map[string]TaskDef, error) {
	raw, err := r.rdb.HGetAll(ctx, r.cfg.CatalogKey()).Result()
	if err != nil {
		return nil, fmt.Errorf("agentroom: read catalog: %w", err)
	}
	defs := make(map[string]TaskDef, len(raw))
	for typ, data := range raw {
		var def TaskDef
		if err := json.Unmarshal([]byte(data), &def); err != nil {
			continue
		}
		defs[typ] = def
	}
	return defs, nil
}

// Claim atomically takes ownership of a task for owner. It returns true on
// success, or false if another agent already holds it or it is already done.
// The claim is a lease expiring after ttl, so a crashed owner's task becomes
// claimable again. This is the cross-agent guard the consumer group cannot
// provide: it stops two different agent types from doing the same work.
func (r *Room) Claim(ctx context.Context, taskID, owner string, ttl time.Duration) (bool, error) {
	done, err := r.rdb.Exists(ctx, r.cfg.TaskKey(taskID)+taskDoneSuffix).Result()
	if err != nil {
		return false, fmt.Errorf("agentroom: check done %s: %w", taskID, err)
	}
	if done > 0 {
		return false, nil
	}
	ok, err := r.rdb.SetNX(ctx, r.cfg.TaskKey(taskID)+taskOwnerSuffix, owner, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("agentroom: claim %s: %w", taskID, err)
	}
	return ok, nil
}

// Complete marks a task done with an optional result (may be nil) and releases
// the claim, so no other agent will pick it up.
func (r *Room) Complete(ctx context.Context, taskID string, result []byte) error {
	pipe := r.rdb.Pipeline()
	pipe.Set(ctx, r.cfg.TaskKey(taskID)+taskDoneSuffix, result, r.cfg.StreamTTL)
	pipe.Del(ctx, r.cfg.TaskKey(taskID)+taskOwnerSuffix)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("agentroom: complete %s: %w", taskID, err)
	}
	return nil
}

// TaskState reports a task's coordination state: TaskDone (with Result),
// TaskClaimed (with Owner), or TaskOpen.
func (r *Room) TaskState(ctx context.Context, taskID string) (TaskStatus, error) {
	res, err := r.rdb.Get(ctx, r.cfg.TaskKey(taskID)+taskDoneSuffix).Bytes()
	if err == nil {
		return TaskStatus{State: TaskDone, Result: res}, nil
	}
	if !errors.Is(err, redis.Nil) {
		return TaskStatus{}, fmt.Errorf("agentroom: task state %s: %w", taskID, err)
	}
	owner, err := r.rdb.Get(ctx, r.cfg.TaskKey(taskID)+taskOwnerSuffix).Result()
	if err == nil {
		return TaskStatus{State: TaskClaimed, Owner: owner}, nil
	}
	if !errors.Is(err, redis.Nil) {
		return TaskStatus{}, fmt.Errorf("agentroom: task state %s: %w", taskID, err)
	}
	return TaskStatus{State: TaskOpen}, nil
}

// OpenTasks scans the last count stream entries and returns those whose type is
// in the catalog and which are neither claimed nor done — the backlog an idle
// agent can pull from without being told what to do.
func (r *Room) OpenTasks(ctx context.Context, count int64) ([]Task, error) {
	defs, err := r.Catalog(ctx)
	if err != nil {
		return nil, err
	}
	msgs, err := r.rdb.XRevRangeN(ctx, r.cfg.StreamKey(), "+", "-", count).Result()
	if err != nil {
		return nil, fmt.Errorf("agentroom: scan open tasks: %w", err)
	}
	var open []Task
	for _, msg := range msgs {
		typ := stringField(msg.Values, "type")
		if _, ok := defs[typ]; !ok {
			continue
		}
		st, err := r.TaskState(ctx, msg.ID)
		if err != nil {
			return nil, err
		}
		if st.State != TaskOpen {
			continue
		}
		task := Task{ID: msg.ID, Type: typ}
		if p := stringField(msg.Values, "payload"); p != "" {
			task.Payload = json.RawMessage(p)
		}
		open = append(open, task)
	}
	return open, nil
}
