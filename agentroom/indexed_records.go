package agentroom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type indexedRecordQuery struct {
	indexKey string
	keyFor   func(string) string
	reverse  bool
	label    string
}

func indexedRecords[T any](ctx context.Context, rdb *redis.Client, query indexedRecordQuery) ([]T, error) {
	ids, err := rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: query.indexKey, Start: 0, Stop: -1, Rev: query.reverse}).Result()
	if err != nil {
		return nil, fmt.Errorf("agentroom: list %s index: %w", query.label, err)
	}
	records := make([]T, 0, len(ids))
	for _, id := range ids {
		raw, err := rdb.Get(ctx, query.keyFor(id)).Bytes()
		if errors.Is(err, redis.Nil) {
			_ = rdb.ZRem(ctx, query.indexKey, id).Err()
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("agentroom: read %s %s: %w", query.label, id, err)
		}
		var record T
		if err := json.Unmarshal(raw, &record); err != nil {
			return nil, fmt.Errorf("agentroom: decode %s %s: %w", query.label, id, err)
		}
		records = append(records, record)
	}
	return records, nil
}
