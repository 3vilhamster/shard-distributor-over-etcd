package shard

import (
	"fmt"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"go.uber.org/zap"

	"github.com/3vilhamster/shard-distributor-over-etcd/gen/proto"
)

// AssignmentState represents the state of a shard assignment
type AssignmentState int

// Assignment states
const (
	StateUnassigned AssignmentState = iota
	StateAssigned
	StateTransferring
	StatePreparing
)

// ShardAssignment represents an individual shard assignment
type ShardAssignment struct {
	ShardID           string
	GroupID           string
	InstanceID        string
	PreviousInstance  string
	State             AssignmentState
	Version           int64
	AssignedAt        time.Time
	LastHeartbeatAt   time.Time
	TransferStartedAt time.Time
	PreparedInstances []string
}

// NewAssignment creates a new shard assignment
func NewAssignment(shardID, groupID string, clock clockwork.Clock) *ShardAssignment {
	return &ShardAssignment{
		ShardID:           shardID,
		GroupID:           groupID,
		State:             StateUnassigned,
		PreparedInstances: make([]string, 0),
		AssignedAt:        clock.Now(),
	}
}

// Assign assigns the shard to an instance
func (a *ShardAssignment) Assign(instanceID string, version int64, clock clockwork.Clock) {
	a.PreviousInstance = a.InstanceID
	a.InstanceID = instanceID
	a.State = StateAssigned
	a.Version = version
	a.AssignedAt = clock.Now()
}

// BeginTransfer starts the transfer of the shard to a new instance
func (a *ShardAssignment) BeginTransfer(targetInstance string, clock clockwork.Clock) {
	a.PreviousInstance = a.InstanceID
	a.InstanceID = targetInstance
	a.State = StateTransferring
	a.TransferStartedAt = clock.Now()
}

// MarkPrepared marks an instance as prepared for this shard
func (a *ShardAssignment) MarkPrepared(instanceID string) {
	// Check if already in the list
	for _, id := range a.PreparedInstances {
		if id == instanceID {
			return
		}
	}

	a.PreparedInstances = append(a.PreparedInstances, instanceID)
}

// IsPrepared checks if an instance is prepared for this shard
func (a *ShardAssignment) IsPrepared(instanceID string) bool {
	for _, id := range a.PreparedInstances {
		if id == instanceID {
			return true
		}
	}
	return false
}

// Unassign removes the instance assignment
func (a *ShardAssignment) Unassign() {
	a.PreviousInstance = a.InstanceID
	a.InstanceID = ""
	a.State = StateUnassigned
}

// UpdateHeartbeat updates the heartbeat timestamp
func (a *ShardAssignment) UpdateHeartbeat(clock clockwork.Clock) {
	a.LastHeartbeatAt = clock.Now()
}

// TransferDuration returns the duration of the current transfer
func (a *ShardAssignment) TransferDuration(clock clockwork.Clock) time.Duration {
	if a.State != StateTransferring || a.TransferStartedAt.IsZero() {
		return 0
	}
	return clock.Now().Sub(a.TransferStartedAt)
}

// AssignmentAge returns the age of the current assignment
func (a *ShardAssignment) AssignmentAge(clock clockwork.Clock) time.Duration {
	if a.AssignedAt.IsZero() {
		return 0
	}
	return clock.Now().Sub(a.AssignedAt)
}

// String returns a string representation of the assignment
func (a *ShardAssignment) String() string {
	var state string
	switch a.State {
	case StateUnassigned:
		state = "Unassigned"
	case StateAssigned:
		state = "Assigned"
	case StateTransferring:
		state = "Transferring"
	case StatePreparing:
		state = "Preparing"
	default:
		state = "Unknown"
	}

	return fmt.Sprintf(
		"Shard: %s, Group: %s, Instance: %s, State: %s, Version: %d",
		a.ShardID, a.GroupID, a.InstanceID, state, a.Version,
	)
}

// ShardNotifier handles notifying instances about shard changes
type ShardNotifier struct {
	logger *zap.Logger
	mu     sync.RWMutex
}

// NewShardNotifier creates a new shard notifier
func NewShardNotifier(logger *zap.Logger) *DefaultShardNotifier {
	return &DefaultShardNotifier{
		logger: logger,
	}
}

// DefaultShardNotifier is the default implementation of shard notification
type DefaultShardNotifier struct {
	logger *zap.Logger
}

// SendNotification sends a shard assignment notification
func (n *DefaultShardNotifier) SendNotification(
	stream proto.ShardDistributor_ShardDistributorStreamServer,
	shardID string,
	action proto.ShardAssignmentAction,
	sourceInstanceID string,
	version int64,
) error {
	msg := &proto.ServerMessage{
		Type:             proto.ServerMessage_SHARD_ASSIGNMENT,
		ShardId:          shardID,
		Action:           action,
		SourceInstanceId: sourceInstanceID,
		Version:          version,
	}

	err := stream.Send(msg)
	if err != nil {
		n.logger.Warn("Failed to send notification",
			zap.String("shard", shardID),
			zap.Stringer("action", action),
			zap.Error(err))
		return err
	}

	return nil
}

// SendNotifications sends multiple notifications to a stream
func (n *DefaultShardNotifier) SendNotifications(
	stream proto.ShardDistributor_ShardDistributorStreamServer,
	notifications []*proto.ServerMessage,
) error {
	for _, notification := range notifications {
		err := stream.Send(notification)
		if err != nil {
			n.logger.Warn("Failed to send notification",
				zap.String("shard", notification.ShardId),
				zap.Stringer("action", notification.Action),
				zap.Error(err))
			return err
		}
	}

	return nil
}

// BeginReconciliation sends a start reconciliation notification
func (n *DefaultShardNotifier) BeginReconciliation(
	stream proto.ShardDistributor_ShardDistributorStreamServer,
) error {
	msg := &proto.ServerMessage{
		Type:             proto.ServerMessage_SHARD_ASSIGNMENT,
		ShardId:          "reconciliation-start",
		Action:           proto.ShardAssignmentAction_RECONCILE,
		IsReconciliation: true,
	}

	err := stream.Send(msg)
	if err != nil {
		n.logger.Warn("Failed to send reconciliation start", zap.Error(err))
		return err
	}

	return nil
}

// EndReconciliation sends an end reconciliation notification
func (n *DefaultShardNotifier) EndReconciliation(
	stream proto.ShardDistributor_ShardDistributorStreamServer,
) error {
	msg := &proto.ServerMessage{
		Type:             proto.ServerMessage_SHARD_ASSIGNMENT,
		ShardId:          "reconciliation-end",
		Action:           proto.ShardAssignmentAction_RECONCILE,
		IsReconciliation: true,
	}

	err := stream.Send(msg)
	if err != nil {
		n.logger.Warn("Failed to send reconciliation end", zap.Error(err))
		return err
	}

	return nil
}

// ReconcileAssignments sends reconciliation notifications for all assignments
func (n *DefaultShardNotifier) ReconcileAssignments(
	stream proto.ShardDistributor_ShardDistributorStreamServer,
	assignments map[string]string,
	versions map[string]int64,
) error {
	// Start reconciliation
	err := n.BeginReconciliation(stream)
	if err != nil {
		return err
	}

	// Send all shard assignments
	for shardID, instanceID := range assignments {
		// Skip shards not assigned to this instance
		if instanceID != "" {
			version := int64(0)
			if v, exists := versions[shardID]; exists {
				version = v
			}

			msg := &proto.ServerMessage{
				Type:             proto.ServerMessage_SHARD_ASSIGNMENT,
				ShardId:          shardID,
				Action:           proto.ShardAssignmentAction_ASSIGN,
				Version:          version,
				IsReconciliation: true,
			}

			err := stream.Send(msg)
			if err != nil {
				n.logger.Warn("Failed to send reconciliation assignment",
					zap.String("shard", shardID),
					zap.Error(err))
				return err
			}
		}
	}

	// End reconciliation
	return n.EndReconciliation(stream)
}
