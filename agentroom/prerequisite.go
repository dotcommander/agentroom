package agentroom

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// prereqScanWindow bounds how far back PrerequisiteMet scans the stream for the
// prerequisite event type. Separate from OpenTasks' cap so the two concerns
// evolve independently.
const prereqScanWindow = 200

// ErrPrerequisiteUnmet is returned by ClaimChecked when a task's declared
// Prerequisite event type has no matching entry in the recent stream window.
// Test with errors.Is; the wrapped message names the missing event type.
var ErrPrerequisiteUnmet = errors.New("agentroom: task prerequisite not yet satisfied")

// PrerequisiteMet reports whether def's declared Prerequisite is satisfied: it
// is trivially true when def.Prerequisite is empty (gating disabled), and true
// otherwise only if at least one event of that type exists in the recent stream
// window (bounded by prereqScanWindow). The match is on event type only —
// agent/payload matching is intentionally out of scope. Like the rest of the
// mesh, this is advisory: the scan window means a prerequisite event older than
// the window is treated as absent.
func (r *Room) PrerequisiteMet(ctx context.Context, def TaskDef) (bool, error) {
	if def.Prerequisite == "" {
		return true, nil
	}
	msgs, err := r.rdb.XRevRangeN(ctx, r.cfg.StreamKey(), "+", "-", prereqScanWindow).Result()
	if err != nil {
		return false, fmt.Errorf("agentroom: scan prerequisite %s: %w", def.Prerequisite, err)
	}
	for _, msg := range msgs {
		if stringField(msg.Values, "type") == def.Prerequisite {
			return true, nil
		}
	}
	return false, nil
}

// ClaimChecked behaves like Claim but first enforces the catalogued task's
// declared Prerequisite when the task is a resolvable stream entry: it looks up
// the triggering event's type and its TaskDef, and returns
// (false, ErrPrerequisiteUnmet) when the prerequisite event type is absent from
// the recent stream window. Gating is best-effort and advisory — if the task id
// is not a real stream entry (e.g. a synthetic id), the event has been trimmed,
// or the catalog cannot be read, ClaimChecked falls straight through to Claim.
// This keeps existing callers (which may pass arbitrary task ids) unaffected.
// The atomic claim itself is unchanged; the prerequisite check is a
// non-atomic Go-side pre-check.
func (r *Room) ClaimChecked(ctx context.Context, taskID, owner string, ttl time.Duration) (bool, error) {
	if err := r.prerequisiteBlock(ctx, taskID); err != nil {
		return false, err
	}
	return r.Claim(ctx, taskID, owner, ttl)
}

// prerequisiteBlock returns nil when the task may be claimed, or a wrapped
// ErrPrerequisiteUnmet when its declared prerequisite is unsatisfied. It is
// advisory: any failure to resolve the task or its catalog def yields nil (no
// gating), so synthetic task ids and trimmed events fall through to Claim.
func (r *Room) prerequisiteBlock(ctx context.Context, taskID string) error {
	ev, ok, err := r.EventByID(ctx, taskID)
	if err != nil || !ok {
		return nil
	}
	def, registered := r.catalogDef(ctx, ev.Type)
	if !registered || def.Prerequisite == "" {
		return nil
	}
	met, err := r.PrerequisiteMet(ctx, def)
	if err != nil {
		return fmt.Errorf("agentroom: claim %s: %w", taskID, err)
	}
	if !met {
		return fmt.Errorf("agentroom: claim %s: prerequisite unmet: %s: %w",
			taskID, def.Prerequisite, ErrPrerequisiteUnmet)
	}
	return nil
}

// catalogDef returns the registered TaskDef for typ, or ok=false when the type
// is unregistered or the catalog cannot be read. Used to gate without surfacing
// catalog read errors as claim failures.
func (r *Room) catalogDef(ctx context.Context, typ string) (TaskDef, bool) {
	defs, err := r.Catalog(ctx)
	if err != nil {
		return TaskDef{}, false
	}
	def, ok := defs[typ]
	return def, ok
}
