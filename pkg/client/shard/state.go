package shard

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// ShardStatus represents the status of a shard
type ShardStatus string

const (
	// ShardStatusInactive indicates the shard is not active
	ShardStatusInactive ShardStatus = "inactive"

	// ShardStatusPreparing indicates the shard is being prepared
	ShardStatusPreparing ShardStatus = "preparing"

	// ShardStatusPrepared indicates the shard is prepared but not active
	ShardStatusPrepared ShardStatus = "prepared"

	// ShardStatusActivating indicates the shard is being activated
	ShardStatusActivating ShardStatus = "activating"

	// ShardStatusActive indicates the shard is active
	ShardStatusActive ShardStatus = "active"

	// ShardStatusDeactivating indicates the shard is being deactivated
	ShardStatusDeactivating ShardStatus = "deactivating"

	// ShardStatusError indicates the shard is in an error state
	ShardStatusError ShardStatus = "error"
)

// String returns the string representation of the shard status
func (s ShardStatus) String() string {
	return string(s)
}

// ShardState represents the state of a shard
type ShardState struct {
	// ShardID is the unique identifier of the shard
	ShardID string

	// Status is the current status of the shard
	Status ShardStatus

	// Version is the version of the shard assignment
	Version int64

	// LastUpdated is the time when the state was last updated
	LastUpdated time.Time

	// ErrorMessage contains error information if Status is ShardStatusError
	ErrorMessage string

	// Priority is the priority of the shard assignment
	Priority int32

	// Metadata contains additional information about the shard
	Metadata map[string]string
}

// ShardStateChange represents a change in shard state
type ShardStateChange struct {
	// ShardID is the unique identifier of the shard
	ShardID string

	// OldState is the old state of the shard, nil if the shard was not present
	OldState *ShardState

	// NewState is the new state of the shard, nil if the shard was removed
	NewState *ShardState

	// Timestamp is the time when the change occurred
	Timestamp time.Time
}

// Common errors
var (
	// ErrShardNotFound is returned when a shard is not found
	ErrShardNotFound = errors.New("shard not found")

	// ErrShardAlreadyExists is returned when a shard already exists
	ErrShardAlreadyExists = errors.New("shard already exists")

	// ErrInvalidState is returned when a shard state is invalid
	ErrInvalidState = errors.New("invalid shard state")
)

// StateManager manages the states of all shards
type StateManager struct {
	// states maps shard IDs to their states
	states map[string]*ShardState

	// listeners is a list of state change listeners
	listeners []StateChangeListener

	// mutex protects access to states and listeners
	mutex sync.RWMutex
}

// StateChangeListener is called when a shard state changes
type StateChangeListener func(change ShardStateChange)

// NewStateManager creates a new state manager
func NewStateManager() *StateManager {
	return &StateManager{
		states:    make(map[string]*ShardState),
		listeners: make([]StateChangeListener, 0),
	}
}

// AddStateChangeListener adds a listener for state changes
func (m *StateManager) AddStateChangeListener(listener StateChangeListener) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.listeners = append(m.listeners, listener)
}

// GetShardState returns the state of a shard
func (m *StateManager) GetShardState(shardID string) (*ShardState, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	state, ok := m.states[shardID]
	if !ok {
		return nil, ErrShardNotFound
	}

	// Return a copy to prevent modification
	stateCopy := *state
	if state.Metadata != nil {
		stateCopy.Metadata = make(map[string]string)
		for k, v := range state.Metadata {
			stateCopy.Metadata[k] = v
		}
	}

	return &stateCopy, nil
}

// GetAllShardStates returns all shard states
func (m *StateManager) GetAllShardStates() (map[string]*ShardState, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	// Create a copy of all states
	statesCopy := make(map[string]*ShardState)
	for id, state := range m.states {
		stateCopy := *state
		if state.Metadata != nil {
			stateCopy.Metadata = make(map[string]string)
			for k, v := range state.Metadata {
				stateCopy.Metadata[k] = v
			}
		}
		statesCopy[id] = &stateCopy
	}

	return statesCopy, nil
}

// UpdateShardState updates the state of a shard
func (m *StateManager) UpdateShardState(shardID string, newState *ShardState) error {
	if newState == nil {
		return errors.New("new state cannot be nil")
	}

	if newState.ShardID != shardID {
		return fmt.Errorf("shard ID mismatch: expected %s, got %s", shardID, newState.ShardID)
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Get old state
	oldState, exists := m.states[shardID]

	// Create a copy of new state
	stateCopy := *newState
	if newState.Metadata != nil {
		stateCopy.Metadata = make(map[string]string)
		for k, v := range newState.Metadata {
			stateCopy.Metadata[k] = v
		}
	}

	// Update state
	m.states[shardID] = &stateCopy

	// Create change event
	change := ShardStateChange{
		ShardID:   shardID,
		Timestamp: time.Now(),
	}

	// Set old state if it existed
	if exists {
		oldStateCopy := *oldState
		if oldState.Metadata != nil {
			oldStateCopy.Metadata = make(map[string]string)
			for k, v := range oldState.Metadata {
				oldStateCopy.Metadata[k] = v
			}
		}
		change.OldState = &oldStateCopy
	}

	// Set new state
	newStateCopy := *newState
	if newState.Metadata != nil {
		newStateCopy.Metadata = make(map[string]string)
		for k, v := range newState.Metadata {
			newStateCopy.Metadata[k] = v
		}
	}
	change.NewState = &newStateCopy

	// Notify listeners
	for _, listener := range m.listeners {
		go listener(change)
	}

	return nil
}

// RemoveShardState removes a shard state
func (m *StateManager) RemoveShardState(shardID string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	oldState, exists := m.states[shardID]
	if !exists {
		return ErrShardNotFound
	}

	// Remove state
	delete(m.states, shardID)

	// Create change event
	change := ShardStateChange{
		ShardID:   shardID,
		Timestamp: time.Now(),
	}

	// Set old state
	oldStateCopy := *oldState
	if oldState.Metadata != nil {
		oldStateCopy.Metadata = make(map[string]string)
		for k, v := range oldState.Metadata {
			oldStateCopy.Metadata[k] = v
		}
	}
	change.OldState = &oldStateCopy

	// Notify listeners
	for _, listener := range m.listeners {
		go listener(change)
	}

	return nil
}

// GetShardsByStatus returns all shards with a specific status
func (m *StateManager) GetShardsByStatus(status ShardStatus) (map[string]*ShardState, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	// Create a copy of all states with the specified status
	statesCopy := make(map[string]*ShardState)
	for id, state := range m.states {
		if state.Status == status {
			stateCopy := *state
			if state.Metadata != nil {
				stateCopy.Metadata = make(map[string]string)
				for k, v := range state.Metadata {
					stateCopy.Metadata[k] = v
				}
			}
			statesCopy[id] = &stateCopy
		}
	}

	return statesCopy, nil
}

// GetShardCount returns the count of shards with a specific status
func (m *StateManager) GetShardCount(status ShardStatus) (int, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	count := 0
	for _, state := range m.states {
		if state.Status == status {
			count++
		}
	}

	return count, nil
}

// UpdateShardMetadata updates the metadata of a shard
func (m *StateManager) UpdateShardMetadata(shardID string, metadata map[string]string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	state, exists := m.states[shardID]
	if !exists {
		return ErrShardNotFound
	}

	// Create change event
	change := ShardStateChange{
		ShardID:   shardID,
		Timestamp: time.Now(),
	}

	// Set old state
	oldStateCopy := *state
	if state.Metadata != nil {
		oldStateCopy.Metadata = make(map[string]string)
		for k, v := range state.Metadata {
			oldStateCopy.Metadata[k] = v
		}
	}
	change.OldState = &oldStateCopy

	// Update metadata
	if state.Metadata == nil {
		state.Metadata = make(map[string]string)
	}
	for k, v := range metadata {
		state.Metadata[k] = v
	}

	// Set new state
	newStateCopy := *state
	newStateCopy.Metadata = make(map[string]string)
	for k, v := range state.Metadata {
		newStateCopy.Metadata[k] = v
	}
	change.NewState = &newStateCopy

	// Notify listeners
	for _, listener := range m.listeners {
		go listener(change)
	}

	return nil
}

// Clear removes all shard states
func (m *StateManager) Clear() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Create change events for all states
	for shardID, state := range m.states {
		// Create change event
		change := ShardStateChange{
			ShardID:   shardID,
			Timestamp: time.Now(),
		}

		// Set old state
		oldStateCopy := *state
		if state.Metadata != nil {
			oldStateCopy.Metadata = make(map[string]string)
			for k, v := range state.Metadata {
				oldStateCopy.Metadata[k] = v
			}
		}
		change.OldState = &oldStateCopy

		// Notify listeners
		for _, listener := range m.listeners {
			go listener(change)
		}
	}

	// Clear states
	m.states = make(map[string]*ShardState)
}
