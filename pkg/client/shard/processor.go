package shard

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/3vilhamster/shard-distributor-over-etcd/gen/proto/sharddistributor/v1"
	config2 "github.com/3vilhamster/shard-distributor-over-etcd/pkg/client/config"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/client/connection"
)

// Processor manages shard assignments and their lifecycle
type Processor struct {
	namespace       string
	config          config2.ProcessorConfig
	connectionMgr   *connection.Manager
	stateManager    *StateManager
	handlers        map[string]Handler
	assignmentQueue chan *proto.ShardDistributorStreamResponse
	transferSem     chan struct{}
	logger          *zap.Logger
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	mutex           sync.RWMutex
}

// NewProcessor creates a new shard processor
func NewProcessor(
	namespace string,
	connectionMgr *connection.Manager,
	stateManager *StateManager,
	logger *zap.Logger,
	config config2.ProcessorConfig,
) *Processor {

	// Use default config if needed
	if config.MaxConcurrentTransfers <= 0 {
		config.MaxConcurrentTransfers = config2.DefaultProcessorConfig.MaxConcurrentTransfers
	}
	if config.ShardActivationTimeout <= 0 {
		config.ShardActivationTimeout = config2.DefaultProcessorConfig.ShardActivationTimeout
	}
	if config.ShardDeactivationTimeout <= 0 {
		config.ShardDeactivationTimeout = config2.DefaultProcessorConfig.ShardDeactivationTimeout
	}
	if config.AssignmentQueueSize <= 0 {
		config.AssignmentQueueSize = config2.DefaultProcessorConfig.AssignmentQueueSize
	}

	ctx, cancel := context.WithCancel(context.Background())

	p := &Processor{
		namespace:       namespace,
		config:          config,
		connectionMgr:   connectionMgr,
		stateManager:    stateManager,
		handlers:        make(map[string]Handler),
		assignmentQueue: make(chan *proto.ShardDistributorStreamResponse, config.AssignmentQueueSize),
		transferSem:     make(chan struct{}, config.MaxConcurrentTransfers),
		logger:          logger.Named("shard-processor"),
		ctx:             ctx,
		cancel:          cancel,
	}

	// Register for shard assignment messages
	connectionMgr.RegisterHandler(proto.ShardDistributorStreamResponse_MESSAGE_TYPE_SHARD_ASSIGNMENT, p.handleShardAssignment)

	// Start processing assigned shards
	p.startAssignmentProcessor()

	return p
}

// RegisterHandler registers a handler for a specific shard type
func (p *Processor) RegisterHandler(shardType string, handler Handler) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.handlers[shardType] = handler
	p.logger.Info("Registered handler for shard type", zap.String("shardType", shardType))
}

// GetHandler returns the handler for a specific shard type
func (p *Processor) GetHandler(shardType string) (Handler, error) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	handler, ok := p.handlers[shardType]
	if !ok {
		return nil, fmt.Errorf("no handler registered for shard type: %s", shardType)
	}

	return handler, nil
}

// handleShardAssignment is called when a shard assignment message is received
func (p *Processor) handleShardAssignment(ctx context.Context, msg *proto.ShardDistributorStreamResponse) error {
	if msg.Type != proto.ShardDistributorStreamResponse_MESSAGE_TYPE_SHARD_ASSIGNMENT {
		return errors.New("expected shard assignment message")
	}

	if msg.Namespace != p.namespace {
		// Other caller
		return nil
	}

	// Queue the assignment for processing
	select {
	case p.assignmentQueue <- msg:
		p.logger.Debug("Queued shard assignment",
			zap.String("shardID", msg.ShardId),
			zap.String("action", msg.Action.String()))
		return nil
	default:
		p.logger.Error("Assignment queue is full, dropping assignment",
			zap.String("shardID", msg.ShardId),
			zap.String("action", msg.Action.String()))
		return errors.New("assignment queue is full")
	}
}

// startAssignmentProcessor starts goroutine to process assignments
func (p *Processor) startAssignmentProcessor() {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()

		p.logger.Info("Starting shard assignment processor")

		for {
			select {
			case <-p.ctx.Done():
				p.logger.Info("Shutting down shard assignment processor")
				return
			case msg := <-p.assignmentQueue:
				// Acquire semaphore slot for concurrent processing
				select {
				case p.transferSem <- struct{}{}:
					// Process the assignment in a separate goroutine
					p.wg.Add(1)
					go func(msg *proto.ShardDistributorStreamResponse) {
						defer p.wg.Done()
						defer func() { <-p.transferSem }() // Release semaphore

						if err := p.processAssignment(msg); err != nil {
							p.logger.Error("Failed to process shard assignment",
								zap.String("shardID", msg.ShardId),
								zap.String("action", msg.Action.String()),
								zap.Error(err))
						}
					}(msg)
				case <-p.ctx.Done():
					return
				}
			}
		}
	}()
}

// processAssignment processes a shard assignment
func (p *Processor) processAssignment(msg *proto.ShardDistributorStreamResponse) error {
	shardID := msg.ShardId
	action := msg.Action

	p.logger.Info("Processing shard assignment",
		zap.String("shardID", shardID),
		zap.String("action", action.String()))

	// Extract shard type from shardID (assuming format like "type:id")
	// This can be adjusted based on your shardID format
	shardType, err := extractShardType(shardID)
	if err != nil {
		return fmt.Errorf("invalid shard ID format: %w", err)
	}

	// Get appropriate handler
	handler, err := p.GetHandler(shardType)
	if err != nil {
		return fmt.Errorf("no handler available: %w", err)
	}

	switch action {
	case proto.ShardAssignmentAction_SHARD_ASSIGNMENT_ACTION_ADD:
		return p.handleAssignAction(shardID, msg, handler)
	case proto.ShardAssignmentAction_SHARD_ASSIGNMENT_ACTION_PREPARE_ADD:
		return p.handlePrepareAction(shardID, msg, handler)
	case proto.ShardAssignmentAction_SHARD_ASSIGNMENT_ACTION_DROP:
		return p.handleRevokeAction(shardID, msg, handler)
	case proto.ShardAssignmentAction_SHARD_ASSIGNMENT_ACTION_PREPARE_DROP:
		// no action for now.
		return nil
	default:
		return fmt.Errorf("unknown shard assignment action: %s", action.String())
	}
}

// handleAssignAction processes shard assignment
func (p *Processor) handleAssignAction(shardID string, msg *proto.ShardDistributorStreamResponse, handler Handler) error {
	// Check current state
	currentState, err := p.stateManager.GetShardState(shardID)
	if err != nil && !errors.Is(err, ErrShardNotFound) {
		return fmt.Errorf("failed to get shard state: %w", err)
	}

	// If already assigned, just acknowledge
	if currentState != nil && currentState.Status == ShardStatusActive {
		p.logger.Debug("Shard already assigned and active, acknowledging",
			zap.String("shardID", shardID))
		return p.connectionMgr.AcknowledgeAssignment(p.ctx, shardID)
	}

	// Create activation context with timeout
	activationCtx, cancel := context.WithTimeout(p.ctx, p.config.ShardActivationTimeout)
	defer cancel()

	// Update state to activating
	err = p.stateManager.UpdateShardState(shardID, &ShardState{
		ShardID:     shardID,
		Status:      ShardStatusActivating,
		LastUpdated: time.Now(),
		Metadata:    map[string]string{},
	})
	if err != nil {
		return fmt.Errorf("failed to update shard state: %w", err)
	}

	// Activate the shard
	p.logger.Info("Activating shard", zap.String("shardID", shardID))

	// Call the handler to activate the shard
	activateErr := handler.Activate(activationCtx, shardID)

	// Update state based on result
	if activateErr != nil {
		p.logger.Error("Failed to activate shard",
			zap.String("shardID", shardID),
			zap.Error(activateErr))

		// Update state to error
		if updateErr := p.stateManager.UpdateShardState(shardID, &ShardState{
			ShardID:      shardID,
			Status:       ShardStatusError,
			LastUpdated:  time.Now(),
			ErrorMessage: activateErr.Error(),
		}); updateErr != nil {
			p.logger.Error("Failed to update shard state after activation error",
				zap.String("shardID", shardID),
				zap.Error(updateErr))
		}

		return fmt.Errorf("failed to activate shard: %w", activateErr)
	}

	// Activation successful, update state to active
	err = p.stateManager.UpdateShardState(shardID, &ShardState{
		ShardID:     shardID,
		Status:      ShardStatusActive,
		LastUpdated: time.Now(),
	})
	if err != nil {
		p.logger.Error("Failed to update shard state after successful activation",
			zap.String("shardID", shardID),
			zap.Error(err))
	}

	p.logger.Info("Shard activated successfully", zap.String("shardID", shardID))

	// Acknowledge the assignment
	return p.connectionMgr.AcknowledgeAssignment(p.ctx, shardID)
}

// handlePrepareAction processes shard preparation
func (p *Processor) handlePrepareAction(shardID string, msg *proto.ShardDistributorStreamResponse, handler Handler) error {
	// Check current state
	currentState, err := p.stateManager.GetShardState(shardID)
	if err != nil && !errors.Is(err, ErrShardNotFound) {
		return fmt.Errorf("failed to get shard state: %w", err)
	}

	// If already prepared or active, just acknowledge
	if currentState != nil &&
		(currentState.Status == ShardStatusPrepared || currentState.Status == ShardStatusActive) {
		p.logger.Debug("Shard already prepared or active, acknowledging",
			zap.String("shardID", shardID),
			zap.String("status", currentState.Status.String()))
		return p.connectionMgr.AcknowledgeAssignment(p.ctx, shardID)
	}

	// Create preparation context with timeout
	prepareCtx, cancel := context.WithTimeout(p.ctx, p.config.ShardActivationTimeout)
	defer cancel()

	// Update state to preparing
	err = p.stateManager.UpdateShardState(shardID, &ShardState{
		ShardID:     shardID,
		Status:      ShardStatusPreparing,
		LastUpdated: time.Now(),
		Metadata:    map[string]string{},
	})
	if err != nil {
		return fmt.Errorf("failed to update shard state: %w", err)
	}

	// Prepare the shard
	p.logger.Info("Preparing shard", zap.String("shardID", shardID))

	// Call the handler to prepare the shard
	prepareErr := handler.Prepare(prepareCtx, shardID)

	// Update state based on result
	if prepareErr != nil {
		p.logger.Error("Failed to prepare shard",
			zap.String("shardID", shardID),
			zap.Error(prepareErr))

		// Update state to error
		if updateErr := p.stateManager.UpdateShardState(shardID, &ShardState{
			ShardID:      shardID,
			Status:       ShardStatusError,
			LastUpdated:  time.Now(),
			ErrorMessage: prepareErr.Error(),
		}); updateErr != nil {
			p.logger.Error("Failed to update shard state after preparation error",
				zap.String("shardID", shardID),
				zap.Error(updateErr))
		}

		return fmt.Errorf("failed to prepare shard: %w", prepareErr)
	}

	// Preparation successful, update state to prepared
	err = p.stateManager.UpdateShardState(shardID, &ShardState{
		ShardID:     shardID,
		Status:      ShardStatusPrepared,
		LastUpdated: time.Now(),
	})
	if err != nil {
		p.logger.Error("Failed to update shard state after successful preparation",
			zap.String("shardID", shardID),
			zap.Error(err))
	}

	p.logger.Info("Shard prepared successfully", zap.String("shardID", shardID))

	// Acknowledge the preparation
	return p.connectionMgr.AcknowledgeAssignment(p.ctx, shardID)
}

// handleRevokeAction processes shard revocation
func (p *Processor) handleRevokeAction(shardID string, msg *proto.ShardDistributorStreamResponse, handler Handler) error {
	// Check current state
	currentState, err := p.stateManager.GetShardState(shardID)
	if err != nil {
		if errors.Is(err, ErrShardNotFound) {
			// Shard not found, nothing to revoke
			p.logger.Debug("Shard not found for revocation, acknowledging",
				zap.String("shardID", shardID))
			return p.connectionMgr.AcknowledgeAssignment(p.ctx, shardID)
		}
		return fmt.Errorf("failed to get shard state: %w", err)
	}

	// If already deactivated, just acknowledge
	if currentState.Status == ShardStatusInactive {
		p.logger.Debug("Shard already inactive, acknowledging",
			zap.String("shardID", shardID))
		return p.connectionMgr.AcknowledgeAssignment(p.ctx, shardID)
	}

	// Create deactivation context with timeout
	deactivationCtx, cancel := context.WithTimeout(p.ctx, p.config.ShardDeactivationTimeout)
	defer cancel()

	// Update state to deactivating
	err = p.stateManager.UpdateShardState(shardID, &ShardState{
		ShardID:     shardID,
		Status:      ShardStatusDeactivating,
		LastUpdated: time.Now(),
	})
	if err != nil {
		return fmt.Errorf("failed to update shard state: %w", err)
	}

	// Deactivate the shard
	p.logger.Info("Deactivating shard", zap.String("shardID", shardID))

	// Call the handler to deactivate the shard
	deactivateErr := handler.Deactivate(deactivationCtx, shardID)

	// Update state based on result
	if deactivateErr != nil {
		p.logger.Error("Failed to deactivate shard",
			zap.String("shardID", shardID),
			zap.Error(deactivateErr))

		// Update state to error
		if updateErr := p.stateManager.UpdateShardState(shardID, &ShardState{
			ShardID:      shardID,
			Status:       ShardStatusError,
			LastUpdated:  time.Now(),
			ErrorMessage: deactivateErr.Error(),
		}); updateErr != nil {
			p.logger.Error("Failed to update shard state after deactivation error",
				zap.String("shardID", shardID),
				zap.Error(updateErr))
		}

		return fmt.Errorf("failed to deactivate shard: %w", deactivateErr)
	}

	// Deactivation successful, update state to inactive
	err = p.stateManager.UpdateShardState(shardID, &ShardState{
		ShardID:     shardID,
		Status:      ShardStatusInactive,
		LastUpdated: time.Now(),
	})
	if err != nil {
		p.logger.Error("Failed to update shard state after successful deactivation",
			zap.String("shardID", shardID),
			zap.Error(err))
	}

	p.logger.Info("Shard deactivated successfully", zap.String("shardID", shardID))

	// Remove the shard state after successful deactivation
	if err := p.stateManager.RemoveShardState(shardID); err != nil {
		p.logger.Warn("Failed to remove shard state after deactivation",
			zap.String("shardID", shardID),
			zap.Error(err))
	}

	// Acknowledge the revocation
	return p.connectionMgr.AcknowledgeAssignment(p.ctx, shardID)
}

// Shutdown gracefully shuts down the processor
func (p *Processor) Shutdown(ctx context.Context) error {
	p.logger.Info("Shutting down shard processor")

	// Signal shutdown
	p.cancel()

	// Create a channel for timeout
	done := make(chan struct{})

	// Wait for all goroutines to finish with timeout
	go func() {
		p.wg.Wait()
		close(done)
	}()

	// Wait for completion or timeout
	select {
	case <-done:
		p.logger.Info("Shard processor shutdown complete")
	case <-ctx.Done():
		p.logger.Warn("Shard processor shutdown timed out")
		return ctx.Err()
	}

	return nil
}

// GetAllShardStates returns all current shard states
func (p *Processor) GetAllShardStates() (map[string]*ShardState, error) {
	return p.stateManager.GetAllShardStates()
}

// GetActiveShardCount returns the count of active shards
func (p *Processor) GetActiveShardCount() (int, error) {
	states, err := p.stateManager.GetAllShardStates()
	if err != nil {
		return 0, err
	}

	count := 0
	for _, state := range states {
		if state.Status == ShardStatusActive {
			count++
		}
	}

	return count, nil
}

// GetStandbyShardCount returns the count of standby (prepared) shards
func (p *Processor) GetStandbyShardCount() (int, error) {
	states, err := p.stateManager.GetAllShardStates()
	if err != nil {
		return 0, err
	}

	count := 0
	for _, state := range states {
		if state.Status == ShardStatusPrepared {
			count++
		}
	}

	return count, nil
}

// extractShardType extracts the type from a shardID
// This function can be adjusted based on your shard ID format
func extractShardType(shardID string) (string, error) {
	// Simple implementation assuming format like "type:id"
	// In a real implementation, this would parse based on your specific format
	parts := strings.SplitN(shardID, ":", 2)
	if len(parts) < 2 {
		return "", errors.New("invalid shard ID format, expected 'type:id'")
	}
	return parts[0], nil
}
