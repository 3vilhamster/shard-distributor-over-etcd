package main

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/client/shard"
)

// ExampleShardHandler is a simple implementation of the shard.Handler interface
type ExampleShardHandler struct {
	shard.BaseHandler
	logger *zap.Logger
}

// NewExampleShardHandler creates a new example shard handler
func NewExampleShardHandler(shardType string, logger *zap.Logger) *ExampleShardHandler {
	return &ExampleShardHandler{
		BaseHandler: shard.BaseHandler{ShardType: shardType},
		logger:      logger,
	}
}

// Prepare prepares the shard
func (h *ExampleShardHandler) Prepare(ctx context.Context, shardID string) error {
	h.logger.Info("Preparing shard",
		zap.String("shardID", shardID))

	// Simulate some preparation work
	select {
	case <-time.After(500 * time.Millisecond):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Activate activates the shard
func (h *ExampleShardHandler) Activate(ctx context.Context, shardID string) error {
	h.logger.Info("Activating shard",
		zap.String("shardID", shardID))

	// Simulate some activation work
	select {
	case <-time.After(500 * time.Millisecond):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Deactivate deactivates the shard
func (h *ExampleShardHandler) Deactivate(ctx context.Context, shardID string) error {
	h.logger.Info("Deactivating shard",
		zap.String("shardID", shardID))

	// Simulate some deactivation work
	select {
	case <-time.After(500 * time.Millisecond):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// GetStats returns statistics about the shard
func (h *ExampleShardHandler) GetStats(shardID string) (map[string]interface{}, error) {
	return map[string]interface{}{
		"example_stat": 42,
	}, nil
}
