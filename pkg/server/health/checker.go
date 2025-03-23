package health

import (
	"context"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/server/registry"
)

// Constants for health checking
const (
	DefaultHealthCheckInterval = 5 * time.Second
	DefaultHealthCheckTimeout  = 15 * time.Second
)

// Checker monitors the health of registered instances
type Checker struct {
	mu        sync.RWMutex
	registry  *registry.Registry
	interval  time.Duration
	timeout   time.Duration
	logger    *zap.Logger
	clock     clockwork.Clock
	isRunning bool
	stopCh    chan struct{}

	// Metrics for monitoring
	unhealthyCount int
	lastCheckTime  time.Time

	// Callback for instance removed
	onInstanceRemoved func(instanceID string)
}

// CheckerParams defines dependencies for creating a Checker
type CheckerParams struct {
	fx.In

	Registry *registry.Registry
	Logger   *zap.Logger
	Clock    clockwork.Clock `optional:"true"`
	Config   *CheckerConfig  `optional:"true"`
}

// CheckerConfig provides configuration for the health checker
type CheckerConfig struct {
	Interval time.Duration
	Timeout  time.Duration
}

// NewChecker creates a new health checker
func NewChecker(params CheckerParams) *Checker {
	// Use defaults for optional dependencies
	clock := params.Clock
	if clock == nil {
		clock = clockwork.NewRealClock()
	}

	interval := DefaultHealthCheckInterval
	timeout := DefaultHealthCheckTimeout

	if params.Config != nil {
		if params.Config.Interval > 0 {
			interval = params.Config.Interval
		}
		if params.Config.Timeout > 0 {
			timeout = params.Config.Timeout
		}
	}

	return &Checker{
		registry: params.Registry,
		interval: interval,
		timeout:  timeout,
		logger:   params.Logger,
		clock:    clock,
		stopCh:   make(chan struct{}),
	}
}

// Start starts the health checker
func (c *Checker) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.isRunning {
		c.mu.Unlock()
		return nil // Already running
	}
	c.isRunning = true
	c.mu.Unlock()

	c.logger.Info("Starting health checker",
		zap.Duration("interval", c.interval),
		zap.Duration("timeout", c.timeout))

	// Run the first check immediately
	c.checkInstanceHealth()

	// Start the check loop
	go c.runCheckLoop(ctx)

	return nil
}

// Stop stops the health checker
func (c *Checker) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.isRunning {
		return
	}

	close(c.stopCh)
	c.isRunning = false
	c.logger.Info("Health checker stopped")
}

// IsRunning returns whether the health checker is running
func (c *Checker) IsRunning() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.isRunning
}

// SetCallback sets the callback for instance removed
func (c *Checker) SetCallback(callback func(instanceID string)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onInstanceRemoved = callback
}

// runCheckLoop runs the health check loop
func (c *Checker) runCheckLoop(ctx context.Context) {
	ticker := c.clock.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.Chan():
			c.checkInstanceHealth()
		}
	}
}

// checkInstanceHealth checks the health of all registered instances
func (c *Checker) checkInstanceHealth() {
	c.mu.Lock()
	c.lastCheckTime = c.clock.Now()
	c.mu.Unlock()

	// Get all instances to check
	instances := c.registry.GetAllInstances()

	c.logger.Debug("Running health check",
		zap.Int("instance_count", len(instances)))

	// Find unhealthy instances
	unhealthy := c.registry.RemoveUnhealthyInstances(c.timeout)

	// Update metrics
	c.mu.Lock()
	c.unhealthyCount = len(unhealthy)
	c.mu.Unlock()

	if len(unhealthy) > 0 {
		c.logger.Info("Removed unhealthy instances",
			zap.Int("count", len(unhealthy)),
			zap.Strings("instances", unhealthy))

		// Call callback for each removed instance
		if c.onInstanceRemoved != nil {
			for _, instanceID := range unhealthy {
				c.onInstanceRemoved(instanceID)
			}
		}
	}
}

// GetHealthMetrics returns health checking metrics
func (c *Checker) GetHealthMetrics() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return map[string]interface{}{
		"is_running":      c.isRunning,
		"unhealthy_count": c.unhealthyCount,
		"last_check_time": c.lastCheckTime,
		"interval":        c.interval.String(),
		"timeout":         c.timeout.String(),
	}
}

// ForceCheck forces an immediate health check
func (c *Checker) ForceCheck() {
	// Run in a goroutine to avoid blocking
	go c.checkInstanceHealth()
}

// SetInterval updates the health check interval
func (c *Checker) SetInterval(interval time.Duration) {
	if interval <= 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.interval = interval
	c.logger.Info("Updated health check interval", zap.Duration("interval", interval))
}

// SetTimeout updates the health check timeout
func (c *Checker) SetTimeout(timeout time.Duration) {
	if timeout <= 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.timeout = timeout
	c.logger.Info("Updated health check timeout", zap.Duration("timeout", timeout))
}
