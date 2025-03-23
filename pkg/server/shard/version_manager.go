package shard

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/server/store"
)

// VersionManager manages versions for shards and shard groups
type VersionManager struct {
	mu             sync.RWMutex
	store          store.Store
	clock          clockwork.Clock
	shardVersions  map[string]int64    // Local cache of shard versions
	globalVersion  int64               // Current global version
	versionHistory map[int64]time.Time // When each version was created
	logger         *zap.Logger
	ctx            context.Context
	cancel         context.CancelFunc
	watchCancel    store.CancelFunc // Cancel function for global version watch
	initialized    bool
}

// VersionManagerParams defines dependencies for creating a version manager
type VersionManagerParams struct {
	fx.In

	Store     store.Store
	Clock     clockwork.Clock `optional:"true"`
	Logger    *zap.Logger     `optional:"true"`
	Lifecycle fx.Lifecycle
}

// NewVersionManager creates a new version manager
func NewVersionManager(params VersionManagerParams) *VersionManager {
	ctx, cancel := context.WithCancel(context.Background())

	return &VersionManager{
		store:          params.Store,
		clock:          params.Clock,
		shardVersions:  make(map[string]int64),
		versionHistory: make(map[int64]time.Time),
		logger:         params.Logger,
		ctx:            ctx,
		cancel:         cancel,
	}
}

// Start initializes the version manager and subscribes to global version changes
func (vm *VersionManager) Start(ctx context.Context) error {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	if vm.initialized {
		return nil // Already initialized
	}

	// Load the global version first
	globalVersion, err := vm.store.GetGlobalVersion(ctx)
	if err != nil {
		return err
	}

	vm.globalVersion = globalVersion
	vm.recordVersionTimestamp(globalVersion)

	vm.logger.Info("Initialized version manager with global version",
		zap.Int64("globalVersion", globalVersion))

	// Subscribe to global version changes
	watchCancel, err := vm.store.WatchGlobalVersion(vm.ctx, vm.handleGlobalVersionChange)
	if err != nil {
		return err
	}
	vm.watchCancel = watchCancel

	vm.initialized = true
	return nil
}

// handleGlobalVersionChange is called when the global version changes in the store
func (vm *VersionManager) handleGlobalVersionChange(event store.WatchEvent) {
	if event.Type != store.EventTypePut {
		return // Only care about puts
	}

	// Parse the version
	versionStr := event.Value
	version, err := strconv.ParseInt(versionStr, 10, 64)
	if err != nil {
		vm.logger.Warn("Failed to parse global version",
			zap.String("versionStr", versionStr),
			zap.Error(err))
		return
	}

	vm.mu.Lock()
	defer vm.mu.Unlock()

	// Only update if the version is newer
	if version > vm.globalVersion {
		vm.logger.Info("Global version updated",
			zap.Int64("oldVersion", vm.globalVersion),
			zap.Int64("newVersion", version))

		vm.globalVersion = version
		vm.recordVersionTimestamp(version)
	}
}

// Stop stops the version manager and cancels any subscriptions
func (vm *VersionManager) Stop(ctx context.Context) error {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	if vm.watchCancel != nil {
		vm.watchCancel()
		vm.watchCancel = nil
	}

	vm.cancel()
	vm.initialized = false

	return nil
}

// SyncVersions loads all version information from the store
func (vm *VersionManager) SyncVersions(ctx context.Context, shardIDs []string) error {
	vm.logger.Info("Synchronizing versions from store")

	// Load global version
	globalVersion, err := vm.store.GetGlobalVersion(ctx)
	if err != nil {
		return err
	}

	vm.SetGlobalVersion(globalVersion)

	// Load shard versions
	for _, shardID := range shardIDs {
		version, err := vm.store.GetShardVersion(ctx, shardID)
		if err != nil {
			vm.logger.Warn("Failed to load version for shard",
				zap.String("shardID", shardID),
				zap.Error(err))
			continue
		}

		vm.SetShardVersion(shardID, version)
	}

	vm.logger.Info("Version synchronization complete",
		zap.Int64("globalVersion", globalVersion),
		zap.Int("shardCount", len(shardIDs)))

	return nil
}

// GetShardVersion returns the current version for a shard
func (vm *VersionManager) GetShardVersion(shardID string) int64 {
	vm.mu.RLock()
	defer vm.mu.RUnlock()

	version, exists := vm.shardVersions[shardID]
	if !exists {
		return 0
	}
	return version
}

// SetShardVersion sets the version for a shard
func (vm *VersionManager) SetShardVersion(shardID string, version int64) {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	vm.shardVersions[shardID] = version
	vm.recordVersionTimestamp(version)
}

// GetGlobalVersion returns the current global version
func (vm *VersionManager) GetGlobalVersion() int64 {
	vm.mu.RLock()
	defer vm.mu.RUnlock()

	return vm.globalVersion
}

// SetGlobalVersion sets the global version
func (vm *VersionManager) SetGlobalVersion(version int64) {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	vm.globalVersion = version
	vm.recordVersionTimestamp(version)
}

// recordVersionTimestamp records when a version was created
func (vm *VersionManager) recordVersionTimestamp(version int64) {
	if _, exists := vm.versionHistory[version]; !exists {
		vm.versionHistory[version] = vm.clock.Now()
	}
}

// IncrementGlobalVersion increments the global version
func (vm *VersionManager) IncrementGlobalVersion(ctx context.Context) (int64, error) {
	// Use the store to perform atomic increment
	newVersion, err := vm.store.IncrementGlobalVersion(ctx)
	if err != nil {
		return 0, err
	}

	// Update local state
	vm.mu.Lock()
	defer vm.mu.Unlock()

	vm.globalVersion = newVersion
	vm.recordVersionTimestamp(newVersion)

	return newVersion, nil
}

// SaveShardVersionToStore ensures a shard's version is saved to the store
func (vm *VersionManager) SaveShardVersionToStore(ctx context.Context, shardID string) error {
	vm.mu.RLock()
	version, exists := vm.shardVersions[shardID]
	vm.mu.RUnlock()

	if !exists {
		// If we don't have a version, use the global version
		vm.mu.RLock()
		version = vm.globalVersion
		vm.mu.RUnlock()
	}

	// Save to store
	err := vm.store.SaveShardVersion(ctx, shardID, version)
	if err != nil {
		return err
	}

	// Ensure our cache is updated
	vm.SetShardVersion(shardID, version)
	return nil
}

// LoadShardVersions loads all shard versions from storage
func (vm *VersionManager) LoadShardVersions(ctx context.Context, shardIDs []string) error {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	for _, shardID := range shardIDs {
		version, err := vm.store.GetShardVersion(ctx, shardID)
		if err != nil {
			return err
		}
		vm.shardVersions[shardID] = version
		vm.recordVersionTimestamp(version)
	}

	return nil
}

// LoadGlobalVersion loads the global version from storage
func (vm *VersionManager) LoadGlobalVersion(ctx context.Context) error {
	version, err := vm.store.GetGlobalVersion(ctx)
	if err != nil {
		return err
	}

	vm.mu.Lock()
	defer vm.mu.Unlock()

	vm.globalVersion = version
	vm.recordVersionTimestamp(version)

	return nil
}

// UpdateShardVersions sets versions for multiple shards at once
func (vm *VersionManager) UpdateShardVersions(ctx context.Context, shardVersions map[string]int64) error {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	// Update local state first
	for shardID, version := range shardVersions {
		vm.shardVersions[shardID] = version
		vm.recordVersionTimestamp(version)
	}

	// Update in storage (in background)
	go func() {
		for shardID, version := range shardVersions {
			if err := vm.store.SaveShardVersion(context.Background(), shardID, version); err != nil {
				vm.logger.Warn("Failed to save shard version",
					zap.String("shard", shardID),
					zap.Int64("version", version),
					zap.Error(err))
			}
		}
	}()

	return nil
}

// GetVersions returns a copy of all shard versions
func (vm *VersionManager) GetVersions() map[string]int64 {
	vm.mu.RLock()
	defer vm.mu.RUnlock()

	// Return a copy to avoid concurrent modification
	versions := make(map[string]int64)
	for shardID, version := range vm.shardVersions {
		versions[shardID] = version
	}

	return versions
}

// GetVersionTimestamp returns when a version was created
func (vm *VersionManager) GetVersionTimestamp(version int64) (time.Time, bool) {
	vm.mu.RLock()
	defer vm.mu.RUnlock()

	timestamp, exists := vm.versionHistory[version]
	return timestamp, exists
}

// GetVersionAge returns the age of a version
func (vm *VersionManager) GetVersionAge(version int64) (time.Duration, bool) {
	vm.mu.RLock()
	defer vm.mu.RUnlock()

	timestamp, exists := vm.versionHistory[version]
	if !exists {
		return 0, false
	}

	return vm.clock.Now().Sub(timestamp), true
}
