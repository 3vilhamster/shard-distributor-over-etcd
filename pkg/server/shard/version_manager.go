package shard

import (
	"context"
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
	store          *store.EtcdStore
	clock          clockwork.Clock
	shardVersions  map[string]int64    // Local cache of shard versions
	globalVersion  int64               // Current global version
	versionHistory map[int64]time.Time // When each version was created
	logger         *zap.Logger         // Optional logger
}

// VersionManagerParams defines dependencies for creating a version manager
type VersionManagerParams struct {
	fx.In

	Store  *store.EtcdStore
	Clock  clockwork.Clock `optional:"true"`
	Logger *zap.Logger     `optional:"true"`
}

// NewVersionManager creates a new version manager
func NewVersionManager(store *store.EtcdStore, clock clockwork.Clock) *VersionManager {
	if clock == nil {
		clock = clockwork.NewRealClock()
	}

	return &VersionManager{
		store:          store,
		clock:          clock,
		shardVersions:  make(map[string]int64),
		versionHistory: make(map[int64]time.Time),
	}
}

// NewVersionManagerWithFx creates a new version manager with fx
func NewVersionManagerWithFx(params VersionManagerParams) *VersionManager {
	// Use defaults for optional dependencies
	clock := params.Clock
	if clock == nil {
		clock = clockwork.NewRealClock()
	}

	return &VersionManager{
		store:          params.Store,
		clock:          clock,
		shardVersions:  make(map[string]int64),
		versionHistory: make(map[int64]time.Time),
		logger:         params.Logger,
	}
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
				if vm.logger != nil {
					vm.logger.Warn("Failed to save shard version",
						zap.String("shard", shardID),
						zap.Int64("version", version),
						zap.Error(err))
				}
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
