package main

import (
	"context"
	"fmt"
	"time"

	"github.com/dotcommander/agentroom/agentroom"
)

type catalogCommand struct{}

func (*catalogCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()
	defs, err := room.Catalog(ctx)
	if err != nil {
		return err
	}
	if len(defs) == 0 {
		outln("(catalog is empty)")
		return nil
	}
	for _, d := range defs {
		_, _ = fmt.Fprintf(g.Out, "%-16s %s\n", terminalText(d.Type), terminalText(d.Description))
		_, _ = fmt.Fprintf(g.Out, "%16s produces=%s requires=%s\n", "", terminalText(d.Produces), terminalText(d.Requires))
		if d.Prerequisite != "" {
			_, _ = fmt.Fprintf(g.Out, "%16s prereq=%s\n", "", terminalText(d.Prerequisite))
		}
	}
	return nil
}

type registerCommand struct {
	Type        string `arg:""`
	Description string `arg:""`
	Produces    string `help:"Event type emitted on success."`
	Requires    string `help:"Capability an agent needs to handle it."`
	Prereq      string `help:"Event type that must exist before this task may be claimed."`
}

func (c *registerCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()
	def := agentroom.TaskDef{Type: c.Type, Description: c.Description, Produces: c.Produces, Requires: c.Requires, Prerequisite: c.Prereq}
	if err := room.RegisterTask(ctx, def); err != nil {
		return err
	}
	outf("registered task type %s\n", def.Type)
	return nil
}

type openCommand struct {
	Count int64 `default:"50" help:"How many recent stream entries to scan (capped at 100)."`
}

func (c *openCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()
	tasks, err := room.OpenTasks(ctx, c.Count)
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		outln("(no open tasks)")
		return nil
	}
	for _, tk := range tasks {
		_, _ = fmt.Fprintf(g.Out, "%s  %-16s %s\n", terminalText(tk.ID), terminalText(tk.Type), terminalText(string(tk.Payload)))
	}
	return nil
}

type claimCommand struct {
	TaskID string        `arg:"" name:"task-id"`
	Agent  string        `help:"Agent id claiming the task."`
	TTL    time.Duration `default:"5m" help:"Claim lease before another agent may reclaim."`
	Force  bool          `help:"Bypass the declared prerequisite gate and claim unconditionally."`
}

func (c *claimCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()
	agent := resolveAgent(c.Agent)
	var (
		ok  bool
		err error
	)
	if c.Force {
		ok, err = room.Claim(ctx, c.TaskID, agent, c.TTL)
	} else {
		ok, err = room.ClaimChecked(ctx, c.TaskID, agent, c.TTL)
	}
	if err != nil {
		return err
	}
	if !ok {
		outf("task %s is already claimed or done -- skip it\n", c.TaskID)
		return nil
	}
	writeHeartbeat(ctx, room, agent, "")
	outf("claimed task %s as %s (lease %s)\n", c.TaskID, agent, c.TTL)
	return nil
}

type doneCommand struct {
	TaskID string `arg:"" name:"task-id"`
	Result string `arg:"" optional:""`
	Agent  string `help:"Agent id completing the task."`
}

func (c *doneCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()
	agent := resolveAgent(c.Agent)
	var result []byte
	if c.Result != "" {
		result = []byte(c.Result)
	}
	if err := room.Complete(ctx, c.TaskID, result); err != nil {
		return err
	}
	writeHeartbeat(ctx, room, agent, "")
	outf("completed task %s\n", c.TaskID)
	return nil
}

type leaveCommand struct {
	Agent string `help:"Deprecated; cleanup always applies to the current session."`
}

func (c *leaveCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()
	if err := room.ClearSessionPresence(ctx, sessionToken()); err != nil {
		return err
	}
	outf("cleared presence for session %s\n", sessionToken())
	return nil
}
