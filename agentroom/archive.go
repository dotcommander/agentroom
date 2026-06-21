package agentroom

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// PersistFunc ships a batch of archived stream entries to cold storage. It runs
// BEFORE the entries are removed from Redis; returning an error aborts removal
// for that stream, leaving the events in place for the next sweep.
type PersistFunc func(streamKey string, events []redis.XMessage) error

// Archiver compacts room streams that have grown past a length threshold. It
// snapshots a stream, hands the snapshot to a PersistFunc, then deletes only the
// persisted entries by ID — so events appended during the sweep are preserved.
type Archiver struct {
	rdb       *redis.Client
	threshold int64
	persist   PersistFunc
}

// NewArchiver builds an Archiver. threshold is the stream length at or above
// which a stream is compacted; persist receives each compacted batch.
func NewArchiver(rdb *redis.Client, threshold int64, persist PersistFunc) *Archiver {
	return &Archiver{rdb: rdb, threshold: threshold, persist: persist}
}

// RunDailySweep scans every room stream and compacts those at or above the
// threshold. It uses SCAN (cursor-based, non-blocking) rather than KEYS, and
// removes only the exact entries it archived. Per-stream failures are collected
// and returned together so one bad stream does not abort the whole sweep.
func (a *Archiver) RunDailySweep(ctx context.Context) error {
	iter := a.rdb.Scan(ctx, 0, "repo:*:events", 0).Iterator()
	var errs []error
	for iter.Next(ctx) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := a.sweepStream(ctx, iter.Val()); err != nil {
			errs = append(errs, err)
		}
	}
	if err := iter.Err(); err != nil {
		errs = append(errs, fmt.Errorf("agentroom: scan streams: %w", err))
	}
	return errors.Join(errs...)
}

// sweepStream compacts one stream when it is at or above the threshold.
func (a *Archiver) sweepStream(ctx context.Context, key string) error {
	length, err := a.rdb.XLen(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("agentroom: xlen %s: %w", key, err)
	}
	if length < a.threshold {
		return nil
	}
	events, err := a.rdb.XRange(ctx, key, "-", "+").Result()
	if err != nil {
		return fmt.Errorf("agentroom: xrange %s: %w", key, err)
	}
	if len(events) == 0 {
		return nil
	}
	if err := a.persist(key, events); err != nil {
		return fmt.Errorf("agentroom: persist %s: %w", key, err)
	}
	ids := make([]string, len(events))
	for i, ev := range events {
		ids[i] = ev.ID
	}
	if err := a.rdb.XDel(ctx, key, ids...).Err(); err != nil {
		return fmt.Errorf("agentroom: xdel %s: %w", key, err)
	}
	return nil
}
