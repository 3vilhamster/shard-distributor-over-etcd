package distribution

import (
	"go.uber.org/fx"
)

// InstanceInfo contains information about a service instance
type InstanceInfo struct {
	ID         string            // Instance identifier
	Status     string            // Status: "active", "draining", "overloaded", etc.
	LoadFactor float64           // Current load factor (0-1)
	ShardCount int               // Current number of assigned shards
	Region     string            // Optional region information
	Zone       string            // Optional zone information
	Capacity   int               // Maximum number of shards the instance can handle
	Metadata   map[string]string // Additional instance metadata
}

// Strategy defines the interface for shard distribution strategies
type Strategy interface {
	// CalculateDistribution determines the optimal placement of shards across instances
	// It takes the current shard-to-instance mapping and available instances,
	// and returns a new shard-to-instance mapping
	CalculateDistribution(
		currentMap map[string]string,
		instances map[string]InstanceInfo,
	) map[string]string

	// Name returns the name of the strategy
	Name() string
}

// Module provides an Fx module for registering distribution strategies
var Module = fx.Provide(
	NewConsistentHashStrategy,
	NewFarmHashStrategy,
)

// StrategyRegistry keeps track of available distribution strategies
type StrategyRegistry struct {
	strategies      map[string]Strategy
	defaultStrategy string
}

// NewStrategyRegistry creates a new strategy registry
func NewStrategyRegistry() *StrategyRegistry {
	return &StrategyRegistry{
		strategies:      make(map[string]Strategy),
		defaultStrategy: "consistentHash",
	}
}

// RegisterStrategy registers a strategy in the registry
func (r *StrategyRegistry) RegisterStrategy(strategy Strategy) {
	r.strategies[strategy.Name()] = strategy
}

// GetStrategy returns a strategy by name, or the default if not found
func (r *StrategyRegistry) GetStrategy(name string) Strategy {
	if strategy, exists := r.strategies[name]; exists {
		return strategy
	}
	return r.strategies[r.defaultStrategy]
}

// SetDefaultStrategy sets the default strategy
func (r *StrategyRegistry) SetDefaultStrategy(name string) {
	if _, exists := r.strategies[name]; exists {
		r.defaultStrategy = name
	}
}

// GetDefaultStrategy returns the default strategy
func (r *StrategyRegistry) GetDefaultStrategy() Strategy {
	return r.strategies[r.defaultStrategy]
}

// GetAvailableStrategies returns the names of all registered strategies
func (r *StrategyRegistry) GetAvailableStrategies() []string {
	names := make([]string, 0, len(r.strategies))
	for name := range r.strategies {
		names = append(names, name)
	}
	return names
}
