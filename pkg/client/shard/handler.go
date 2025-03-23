package shard

import (
	"context"
)

// Handler defines the interface for shard handlers
// A shard handler is responsible for the actual business logic
// of handling shards of a specific type
type Handler interface {
	// Prepare prepares the shard for activation (e.g., loading data)
	// This is used for fast handovers
	Prepare(ctx context.Context, shardID string, version int64) error

	// Activate activates the shard (e.g., starting processing)
	Activate(ctx context.Context, shardID string, version int64) error

	// Deactivate deactivates the shard (e.g., stopping processing)
	// The handler should make sure any in-flight work is completed
	// or properly saved before returning
	Deactivate(ctx context.Context, shardID string, version int64) error

	// GetType returns the type of shard this handler can process
	GetType() string

	// GetStats returns statistics about the shard
	GetStats(shardID string) (map[string]interface{}, error)
}

// BaseHandler provides a base implementation of the Handler interface
// that can be embedded in concrete handlers
type BaseHandler struct {
	ShardType string
}

// GetType returns the type of shard this handler can process
func (h *BaseHandler) GetType() string {
	return h.ShardType
}

// HandlerFactory is a function that creates a new handler
type HandlerFactory func() Handler

// HandlerRegistry keeps track of handler factories for different shard types
type HandlerRegistry struct {
	factories map[string]HandlerFactory
}

// NewHandlerRegistry creates a new handler registry
func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{
		factories: make(map[string]HandlerFactory),
	}
}

// Register registers a factory for a specific shard type
func (r *HandlerRegistry) Register(shardType string, factory HandlerFactory) {
	r.factories[shardType] = factory
}

// Create creates a new handler for a specific shard type
func (r *HandlerRegistry) Create(shardType string) (Handler, bool) {
	factory, ok := r.factories[shardType]
	if !ok {
		return nil, false
	}
	return factory(), true
}

// GetTypes returns all registered shard types
func (r *HandlerRegistry) GetTypes() []string {
	types := make([]string, 0, len(r.factories))
	for t := range r.factories {
		types = append(types, t)
	}
	return types
}

// ShardHandlerConfig defines configuration for shard handlers
type ShardHandlerConfig struct {
	// MaxConcurrentOperations is the maximum number of concurrent operations
	MaxConcurrentOperations int

	// ShutdownTimeout is the maximum time to wait for operations to complete on shutdown
	ShutdownTimeout int

	// Custom configuration options for specific handlers
	CustomConfig map[string]interface{}
}

// DefaultShardHandlerConfig provides default configuration values
var DefaultShardHandlerConfig = ShardHandlerConfig{
	MaxConcurrentOperations: 10,
	ShutdownTimeout:         30, // seconds
	CustomConfig:            make(map[string]interface{}),
}
