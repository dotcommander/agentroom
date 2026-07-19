package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/dotcommander/agentroom/agentroom"
)

const (
	coordinationWindowActive  = "active"
	coordinationWindowPending = "pending"
)

type leaseCommand struct {
	Acquire leaseAcquireCommand `cmd:"" help:"Atomically acquire one or more resources."`
	Renew   leaseRenewCommand   `cmd:"" help:"Renew an owned resource lease."`
	Release leaseReleaseCommand `cmd:"" help:"Release an owned resource lease."`
	List    leaseListCommand    `cmd:"" help:"List active resource leases."`
}

type leaseAcquireCommand struct {
	Resources []string      `arg:"" name:"resource"`
	Purpose   string        `required:"" help:"Why these resources are needed."`
	TTL       time.Duration `default:"15m" help:"Lease duration (maximum 24h)."`
	Agent     string        `help:"Lease owner identity."`
}

func (c *leaseAcquireCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()
	lease, err := room.AcquireResources(ctx, agentroom.ResourceLeaseRequest{Owner: resolveAgent(c.Agent), Resources: c.Resources, Purpose: c.Purpose, TTL: c.TTL})
	if err != nil {
		return err
	}
	writeHeartbeat(ctx, room, lease.Owner, "")
	return writeJSON(g.Out, schemaItem[agentroom.ResourceLease]{SchemaVersion: 1, Item: lease})
}

type leaseRenewCommand struct {
	ID    string        `arg:""`
	TTL   time.Duration `default:"15m" help:"Renewal duration (maximum 24h)."`
	Agent string        `help:"Lease owner identity."`
}

func (c *leaseRenewCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()
	lease, err := room.RenewResources(ctx, c.ID, resolveAgent(c.Agent), c.TTL)
	if err != nil {
		return err
	}
	writeHeartbeat(ctx, room, lease.Owner, "")
	return writeJSON(g.Out, schemaItem[agentroom.ResourceLease]{SchemaVersion: 1, Item: lease})
}

type leaseReleaseCommand struct {
	ID    string `arg:""`
	Agent string `help:"Lease owner identity."`
}

func (c *leaseReleaseCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()
	owner := resolveAgent(c.Agent)
	if err := room.ReleaseResources(ctx, c.ID, owner); err != nil {
		return err
	}
	writeHeartbeat(ctx, room, owner, "")
	return nil
}

type leaseListCommand struct {
	JSON bool `help:"Write JSON output."`
}

func (c *leaseListCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()
	leases, err := room.ResourceLeases(ctx)
	if err != nil {
		return err
	}
	if c.JSON {
		return writeJSON(g.Out, schemaList[agentroom.ResourceLease]{SchemaVersion: 1, Items: leases})
	}
	for _, lease := range leases {
		_, _ = fmt.Fprintf(g.Out, "%s  %s  %s  %s\n", lease.ID, terminalText(lease.Owner), terminalText(fmt.Sprint(lease.Resources)), terminalText(lease.Purpose))
	}
	return nil
}

type guardCommand struct {
	Resources   []string `arg:"" name:"resource"`
	Lease       string   `help:"Lease ID expected to cover the resources."`
	RequireHeld bool     `name:"require-held" help:"Fail unless --lease is owned by this agent."`
	JSON        bool     `help:"Write JSON output."`
	Agent       string   `help:"Identity checking resource ownership."`
}

type commandExitError struct {
	err  error
	code int
}

func (e *commandExitError) Error() string { return e.err.Error() }
func (e *commandExitError) Unwrap() error { return e.err }
func (e *commandExitError) ExitCode() int { return e.code }

func (c *guardCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()
	owner := resolveAgent(c.Agent)
	err := room.GuardResources(ctx, c.Resources, owner, c.Lease, c.RequireHeld)
	writeHeartbeat(ctx, room, owner, "")
	if err == nil {
		if c.JSON {
			return writeJSON(g.Out, map[string]any{"schema_version": 1, "safe": true})
		}
		_, _ = fmt.Fprintln(g.Out, "safe")
		return nil
	}
	if errors.Is(err, agentroom.ErrLeaseConflict) || errors.Is(err, agentroom.ErrResourceLeaseNeeded) {
		if c.JSON {
			_ = writeJSON(g.Out, map[string]any{"schema_version": 1, "safe": false, "error": err.Error()})
		}
		return &commandExitError{err: err, code: 3}
	}
	return err
}

type windowCommand struct {
	Request  windowRequestCommand  `cmd:"" help:"Request a quiet window reservation."`
	Ack      windowAckCommand      `cmd:"" help:"Acknowledge a required window."`
	Activate windowActivateCommand `cmd:"" help:"Activate an acknowledged, unblocked window."`
	Status   windowStatusCommand   `cmd:"" help:"Show one or all windows."`
	Release  windowReleaseCommand  `cmd:"" help:"Release an active window."`
	Cancel   windowCancelCommand   `cmd:"" help:"Cancel a pending window."`
}

type windowRequestCommand struct {
	Resources  []string      `arg:"" name:"resource"`
	Purpose    string        `required:""`
	TTL        time.Duration `default:"5m"`
	AckTimeout time.Duration `name:"ack-timeout" default:"2m"`
	Require    []string      `help:"Required live agent; repeatable."`
	Agent      string        `help:"Window owner identity."`
}

func (c *windowRequestCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()
	required := make([]string, 0, len(c.Require))
	for _, raw := range c.Require {
		target, err := resolveLiveTarget(ctx, room, raw)
		if err != nil {
			return err
		}
		required = append(required, target)
	}
	w, err := room.RequestWindow(ctx, agentroom.WindowRequest{Owner: resolveAgent(c.Agent), Resources: c.Resources, Purpose: c.Purpose, TTL: c.TTL, AckTimeout: c.AckTimeout, Required: required})
	if err != nil {
		return err
	}
	writeHeartbeat(ctx, room, w.Owner, "")
	return writeJSON(g.Out, schemaItem[agentroom.CoordinationWindow]{SchemaVersion: 1, Item: w})
}

type windowAckCommand struct {
	ID    string `arg:""`
	Agent string `help:"Acknowledging identity."`
}

func (c *windowAckCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()
	w, err := room.AcknowledgeWindow(ctx, c.ID, resolveAgent(c.Agent))
	if err != nil {
		return err
	}
	writeHeartbeat(ctx, room, resolveAgent(c.Agent), "")
	return writeJSON(g.Out, schemaItem[agentroom.CoordinationWindow]{SchemaVersion: 1, Item: w})
}

type windowActivateCommand struct {
	ID    string `arg:""`
	Agent string `help:"Window owner identity."`
}

func (c *windowActivateCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()
	w, err := room.ActivateWindow(ctx, c.ID, resolveAgent(c.Agent))
	if err != nil {
		return err
	}
	writeHeartbeat(ctx, room, w.Owner, "")
	return writeJSON(g.Out, schemaItem[agentroom.CoordinationWindow]{SchemaVersion: 1, Item: w})
}

type windowReleaseCommand struct {
	ID    string `arg:""`
	Agent string `help:"Window owner identity."`
}

func (c *windowReleaseCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()
	owner := resolveAgent(c.Agent)
	if err := room.ReleaseWindow(ctx, c.ID, owner); err != nil {
		return err
	}
	writeHeartbeat(ctx, room, owner, "")
	return nil
}

type windowCancelCommand struct {
	ID    string `arg:""`
	Agent string `help:"Window owner identity."`
}

func (c *windowCancelCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()
	owner := resolveAgent(c.Agent)
	if err := room.CancelWindow(ctx, c.ID, owner); err != nil {
		return err
	}
	writeHeartbeat(ctx, room, owner, "")
	return nil
}

type windowStatusCommand struct {
	ID   string `arg:"" optional:""`
	JSON bool   `help:"Write JSON output."`
}

func (c *windowStatusCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()
	windows, err := room.Windows(ctx)
	if err != nil {
		return err
	}
	if c.ID != "" {
		for _, w := range windows {
			if w.ID == c.ID {
				return writeJSON(g.Out, schemaItem[agentroom.CoordinationWindow]{SchemaVersion: 1, Item: w})
			}
		}
		return fmt.Errorf("window %s not found", c.ID)
	}
	if c.JSON {
		return writeJSON(g.Out, schemaList[agentroom.CoordinationWindow]{SchemaVersion: 1, Items: windows})
	}
	for _, w := range windows {
		_, _ = fmt.Fprintf(g.Out, "%s  %-9s %s  %s\n", w.ID, w.State, terminalText(w.Owner), terminalText(fmt.Sprint(w.Resources)))
	}
	return nil
}

type workCommand struct {
	State   string          `arg:"" enum:"started,waiting,blocked,completed,handoff,failed"`
	Scope   string          `required:""`
	Summary string          `required:""`
	To      string          `help:"Actual handoff recipient."`
	Data    json.RawMessage `help:"Additional JSON data."`
	TTL     time.Duration   `default:"24h"`
	Agent   string          `help:"Work owner identity."`
}

func (c *workCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()
	to := ""
	var err error
	if c.To != "" {
		to, err = resolveLiveTarget(ctx, room, c.To)
		if err != nil {
			return err
		}
	}
	status, err := room.SetWorkStatus(ctx, agentroom.WorkStatus{AgentID: resolveAgent(c.Agent), State: c.State, Scope: c.Scope, Summary: c.Summary, To: to, Data: c.Data}, c.TTL)
	if err != nil {
		return err
	}
	writeHeartbeat(ctx, room, status.AgentID, "")
	return writeJSON(g.Out, status)
}

type statusCommand struct {
	JSON bool `help:"Write JSON output."`
}
type statusSnapshot struct {
	SchemaVersion int                                `json:"schema_version"`
	Leases        []agentroom.ResourceLease          `json:"leases"`
	Windows       []agentroom.CoordinationWindow     `json:"windows"`
	Work          []agentroom.WorkStatus             `json:"work_statuses"`
	Tasks         []agentroom.Task                   `json:"open_tasks"`
	Presence      map[string]agentroom.PresenceEntry `json:"presence"`
}

func (c *statusCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()
	snapshot := statusSnapshot{SchemaVersion: 1}
	var err error
	if snapshot.Leases, err = room.ResourceLeases(ctx); err != nil {
		return err
	}
	if snapshot.Windows, err = room.Windows(ctx); err != nil {
		return err
	}
	snapshot.Windows = activeWindows(snapshot.Windows)
	if snapshot.Work, err = room.WorkStatuses(ctx); err != nil {
		return err
	}
	if snapshot.Tasks, err = room.OpenTasks(ctx, 100); err != nil {
		return err
	}
	if snapshot.Presence, err = room.PresenceDetailed(ctx); err != nil {
		return err
	}
	if c.JSON {
		return writeJSON(g.Out, snapshot)
	}
	_, _ = fmt.Fprintf(g.Out, "windows: %d\nleases: %d\nwork: %d\nopen tasks: %d\npresence: %d\n", len(snapshot.Windows), len(snapshot.Leases), len(snapshot.Work), len(snapshot.Tasks), len(snapshot.Presence))
	return nil
}
func activeWindows(in []agentroom.CoordinationWindow) []agentroom.CoordinationWindow {
	out := in[:0]
	for _, w := range in {
		if w.State == coordinationWindowPending || w.State == coordinationWindowActive {
			out = append(out, w)
		}
	}
	return out
}

type schemaList[T any] struct {
	SchemaVersion int `json:"schema_version"`
	Items         []T `json:"items"`
}
type schemaItem[T any] struct {
	SchemaVersion int `json:"schema_version"`
	Item          T   `json:"item"`
}

func writeJSON(w io.Writer, value any) error { return json.NewEncoder(w).Encode(value) }
