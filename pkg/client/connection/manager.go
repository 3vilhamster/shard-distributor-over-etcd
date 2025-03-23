package connection

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/3vilhamster/shard-distributor-over-etcd/gen/proto"
)

const (
	// DefaultHeartbeatInterval is the default interval for sending heartbeats
	DefaultHeartbeatInterval = 5 * time.Second
	// DefaultReconnectBackoff is the default backoff for reconnection attempts
	DefaultReconnectBackoff = 1 * time.Second
	// DefaultMaxReconnectBackoff is the maximum backoff for reconnection attempts
	DefaultMaxReconnectBackoff = 30 * time.Second
	// DefaultReconnectJitter is the jitter added to reconnection attempts
	DefaultReconnectJitter = 0.2
)

// Manager handles the connection to the ShardDistributor service
type Manager struct {
	conn                *grpc.ClientConn
	client              proto.ShardDistributorClient
	stream              proto.ShardDistributor_ShardDistributorStreamClient
	instanceID          string
	instanceInfo        *proto.InstanceInfo
	serverAddr          string
	reconnectBackoff    backoff.Config
	heartbeatInterval   time.Duration
	heartbeatTicker     *time.Ticker
	reconnectCh         chan struct{}
	mutex               sync.RWMutex
	streamMutex         sync.Mutex
	logger              *zap.Logger
	handlers            map[proto.ServerMessage_MessageType][]ServerMessageHandler
	lastAssignedShards  []string
	lastLeasedID        int64
	ctx                 context.Context
	cancel              context.CancelFunc
	isRegistered        bool
	isShutdown          bool
	onRegistered        func([]string)
	onConnectionChanged func(connectivity.State)
}

// ServerMessageHandler defines a handler function for server messages
type ServerMessageHandler func(context.Context, *proto.ServerMessage) error

// ManagerOption defines a functional option for the Manager
type ManagerOption func(*Manager)

// WithHeartbeatInterval sets the heartbeat interval
func WithHeartbeatInterval(interval time.Duration) ManagerOption {
	return func(m *Manager) {
		m.heartbeatInterval = interval
	}
}

// WithLogger sets the logger
func WithLogger(logger *zap.Logger) ManagerOption {
	return func(m *Manager) {
		m.logger = logger
	}
}

// WithReconnectBackoff sets custom reconnect backoff parameters
func WithReconnectBackoff(initialBackoff, maxBackoff time.Duration, jitter float64) ManagerOption {
	return func(m *Manager) {
		m.reconnectBackoff = backoff.Config{
			BaseDelay:  initialBackoff,
			Multiplier: 1.5,
			Jitter:     jitter,
			MaxDelay:   maxBackoff,
		}
	}
}

// WithOnRegistered sets the callback for successful registration
func WithOnRegistered(callback func([]string)) ManagerOption {
	return func(m *Manager) {
		m.onRegistered = callback
	}
}

// WithOnConnectionChanged sets the callback for connection state changes
func WithOnConnectionChanged(callback func(connectivity.State)) ManagerOption {
	return func(m *Manager) {
		m.onConnectionChanged = callback
	}
}

// NewManager creates a new connection manager
func NewManager(serverAddr, instanceID string, instanceInfo *proto.InstanceInfo, options ...ManagerOption) *Manager {
	ctx, cancel := context.WithCancel(context.Background())

	m := &Manager{
		serverAddr:        serverAddr,
		instanceID:        instanceID,
		instanceInfo:      instanceInfo,
		heartbeatInterval: DefaultHeartbeatInterval,
		reconnectCh:       make(chan struct{}, 1),
		handlers:          make(map[proto.ServerMessage_MessageType][]ServerMessageHandler),
		logger:            zap.NewNop(),
		reconnectBackoff: backoff.Config{
			BaseDelay:  DefaultReconnectBackoff,
			Multiplier: 1.5,
			Jitter:     DefaultReconnectJitter,
			MaxDelay:   DefaultMaxReconnectBackoff,
		},
		ctx:    ctx,
		cancel: cancel,
	}

	// Apply options
	for _, option := range options {
		option(m)
	}

	return m
}

// Connect establishes a connection to the ShardDistributor service
func (m *Manager) Connect(ctx context.Context) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.isShutdown {
		return errors.New("manager is shut down")
	}

	if m.conn != nil {
		return nil // Already connected
	}

	m.logger.Info("Connecting to ShardDistributor server", zap.String("server", m.serverAddr))

	// Set up connection options with reconnect parameters
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: m.reconnectBackoff,
		}),
	}

	// Create gRPC connection
	conn, err := grpc.DialContext(ctx, m.serverAddr, opts...)
	if err != nil {
		m.logger.Error("Failed to connect to server",
			zap.String("server", m.serverAddr),
			zap.Error(err))
		return fmt.Errorf("failed to connect to server: %w", err)
	}

	m.conn = conn
	m.client = proto.NewShardDistributorClient(conn)

	// Start the state monitor
	go m.monitorConnectionState()

	// Start the stream
	if err := m.startStream(ctx); err != nil {
		m.logger.Error("Failed to start stream", zap.Error(err))
		return fmt.Errorf("failed to start stream: %w", err)
	}

	return nil
}

// startStream initializes the bidirectional stream with the server
func (m *Manager) startStream(ctx context.Context) error {
	m.streamMutex.Lock()
	defer m.streamMutex.Unlock()

	if m.stream != nil {
		return nil // Stream already established
	}

	stream, err := m.client.ShardDistributorStream(ctx)
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}

	m.stream = stream
	m.logger.Info("Stream established with server")

	// Start receiving messages
	go m.receiveMessages()

	return nil
}

func (m *Manager) InstanceID() string {
	return m.instanceID
}

// Register registers the instance with the server
func (m *Manager) Register(ctx context.Context) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.isRegistered {
		return nil // Already registered
	}

	m.logger.Info("Registering instance with server",
		zap.String("instanceID", m.instanceID))

	// Prepare register message
	msg := &proto.ClientMessage{
		InstanceId:   m.instanceID,
		Type:         proto.ClientMessage_REGISTER,
		InstanceInfo: m.instanceInfo,
	}

	// Send registration message
	if err := m.sendMessage(msg); err != nil {
		return fmt.Errorf("failed to send registration message: %w", err)
	}

	// Start heartbeat
	m.startHeartbeat()

	return nil
}

// Deregister deregisters the instance from the server
func (m *Manager) Deregister(ctx context.Context) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if !m.isRegistered {
		return nil // Not registered
	}

	m.logger.Info("Deregistering instance from server",
		zap.String("instanceID", m.instanceID))

	// Stop heartbeat
	m.stopHeartbeat()

	// Prepare deregister message
	msg := &proto.ClientMessage{
		InstanceId: m.instanceID,
		Type:       proto.ClientMessage_DEREGISTER,
	}

	// Send deregistration message
	if err := m.sendMessage(msg); err != nil {
		m.logger.Warn("Failed to send deregistration message", zap.Error(err))
		// Continue with shutdown even if deregistration fails
	}

	m.isRegistered = false
	return nil
}

// StartWatching starts watching for shard assignments
func (m *Manager) StartWatching(ctx context.Context) error {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if !m.isRegistered {
		return errors.New("instance not registered")
	}

	m.logger.Info("Starting to watch for shard assignments")

	// Prepare watch message
	msg := &proto.ClientMessage{
		InstanceId: m.instanceID,
		Type:       proto.ClientMessage_WATCH,
	}

	// Send watch message
	if err := m.sendMessage(msg); err != nil {
		return fmt.Errorf("failed to send watch message: %w", err)
	}

	return nil
}

// AcknowledgeAssignment acknowledges a shard assignment
func (m *Manager) AcknowledgeAssignment(ctx context.Context, shardID string) error {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if !m.isRegistered {
		return errors.New("instance not registered")
	}

	m.logger.Debug("Acknowledging shard assignment",
		zap.String("shardID", shardID))

	// Prepare ack message
	msg := &proto.ClientMessage{
		InstanceId: m.instanceID,
		Type:       proto.ClientMessage_ACK,
		ShardId:    shardID,
	}

	// Send ack message
	if err := m.sendMessage(msg); err != nil {
		return fmt.Errorf("failed to send ack message: %w", err)
	}

	return nil
}

// ReportStatus sends a status report to the server
func (m *Manager) ReportStatus(ctx context.Context, status *proto.StatusReport) error {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if !m.isRegistered {
		return errors.New("instance not registered")
	}

	m.logger.Debug("Sending status report",
		zap.String("status", status.Status.String()),
		zap.Int32("activeShards", status.ActiveShardCount))

	// Prepare status report message
	msg := &proto.ClientMessage{
		InstanceId: m.instanceID,
		Type:       proto.ClientMessage_STATUS_REPORT,
		Status:     status,
	}

	// Send status report
	if err := m.sendMessage(msg); err != nil {
		return fmt.Errorf("failed to send status report: %w", err)
	}

	return nil
}

// RegisterHandler registers a handler for a specific message type
func (m *Manager) RegisterHandler(msgType proto.ServerMessage_MessageType, handler ServerMessageHandler) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.handlers[msgType] = append(m.handlers[msgType], handler)
}

// Shutdown gracefully shuts down the connection manager
func (m *Manager) Shutdown(ctx context.Context) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.isShutdown {
		return nil // Already shut down
	}

	m.logger.Info("Shutting down connection manager")
	m.isShutdown = true

	// Deregister if registered
	if m.isRegistered {
		if err := m.Deregister(ctx); err != nil {
			m.logger.Warn("Failed to deregister during shutdown", zap.Error(err))
		}
	}

	// Stop heartbeat
	m.stopHeartbeat()

	// Close stream
	if m.stream != nil {
		if err := m.stream.CloseSend(); err != nil {
			m.logger.Warn("Error closing stream", zap.Error(err))
		}
	}

	// Close connection
	if m.conn != nil {
		if err := m.conn.Close(); err != nil {
			m.logger.Warn("Error closing connection", zap.Error(err))
		}
	}

	// Cancel context
	m.cancel()

	return nil
}

// IsRegistered returns whether the instance is registered
func (m *Manager) IsRegistered() bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.isRegistered
}

// LastAssignedShards returns the last assigned shards from registration
func (m *Manager) LastAssignedShards() []string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.lastAssignedShards
}

// LastLeaseID returns the last lease ID from registration
func (m *Manager) LastLeaseID() int64 {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.lastLeasedID
}

// sendMessage sends a message to the server
func (m *Manager) sendMessage(msg *proto.ClientMessage) error {
	m.streamMutex.Lock()
	defer m.streamMutex.Unlock()

	if m.stream == nil {
		return errors.New("stream not established")
	}

	if err := m.stream.Send(msg); err != nil {
		m.logger.Error("Failed to send message",
			zap.String("type", msg.Type.String()),
			zap.Error(err))

		// Trigger reconnection
		m.triggerReconnect()
		return err
	}

	return nil
}

// receiveMessages handles incoming messages from the server
func (m *Manager) receiveMessages() {
	for {
		if m.isShutdown {
			return
		}

		if m.stream == nil {
			m.logger.Warn("Stream is nil, waiting for reconnection")
			time.Sleep(1 * time.Second)
			continue
		}

		msg, err := m.stream.Recv()
		if err != nil {
			if err == io.EOF {
				m.logger.Info("Stream closed by server")
			} else {
				m.logger.Error("Error receiving message", zap.Error(err))
			}

			// Trigger reconnection
			m.triggerReconnect()
			return
		}

		// Process the message
		if err := m.handleServerMessage(msg); err != nil {
			m.logger.Error("Error handling server message",
				zap.Error(err),
				zap.String("messageType", msg.Type.String()))
		}
	}
}

// handleServerMessage processes incoming server messages
func (m *Manager) handleServerMessage(msg *proto.ServerMessage) error {
	m.mutex.RLock()
	handlers := m.handlers[msg.Type]
	m.mutex.RUnlock()

	// Handle special messages internally
	switch msg.Type {
	case proto.ServerMessage_REGISTER_RESPONSE:
		m.handleRegisterResponse(msg)
	case proto.ServerMessage_HEARTBEAT_ACK:
		// Just log at debug level
		m.logger.Debug("Received heartbeat acknowledgment")
		return nil
	}

	// Call registered handlers
	ctx := context.Background()
	for _, handler := range handlers {
		if err := handler(ctx, msg); err != nil {
			m.logger.Warn("Handler error",
				zap.String("messageType", msg.Type.String()),
				zap.Error(err))
		}
	}

	return nil
}

// handleRegisterResponse processes registration response
func (m *Manager) handleRegisterResponse(msg *proto.ServerMessage) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if !msg.Success {
		m.logger.Error("Registration failed",
			zap.String("reason", msg.Message))
		return
	}

	m.isRegistered = true
	m.lastLeasedID = msg.LeaseId
	m.lastAssignedShards = msg.AssignedShards

	m.logger.Info("Registration successful",
		zap.Int64("leaseID", msg.LeaseId),
		zap.Int("assignedShards", len(msg.AssignedShards)))

	// Call the registered callback
	if m.onRegistered != nil {
		go m.onRegistered(msg.AssignedShards)
	}
}

// startHeartbeat starts the heartbeat ticker
func (m *Manager) startHeartbeat() {
	if m.heartbeatTicker != nil {
		m.stopHeartbeat()
	}

	m.heartbeatTicker = time.NewTicker(m.heartbeatInterval)
	go func() {
		for {
			select {
			case <-m.ctx.Done():
				return
			case <-m.heartbeatTicker.C:
				// Send heartbeat
				msg := &proto.ClientMessage{
					InstanceId: m.instanceID,
					Type:       proto.ClientMessage_HEARTBEAT,
				}
				if err := m.sendMessage(msg); err != nil {
					m.logger.Warn("Failed to send heartbeat", zap.Error(err))
				}
			}
		}
	}()
}

// stopHeartbeat stops the heartbeat ticker
func (m *Manager) stopHeartbeat() {
	if m.heartbeatTicker != nil {
		m.heartbeatTicker.Stop()
		m.heartbeatTicker = nil
	}
}

// triggerReconnect triggers the reconnection process
func (m *Manager) triggerReconnect() {
	select {
	case m.reconnectCh <- struct{}{}:
		// Signal sent successfully
	default:
		// Channel already has a pending signal
	}
}

// monitorConnectionState monitors the connection state and handles reconnection
func (m *Manager) monitorConnectionState() {
	var lastState connectivity.State = connectivity.Idle
	for {
		if m.isShutdown {
			return
		}

		// Check current state
		if m.conn != nil {
			currentState := m.conn.GetState()
			if currentState != lastState {
				m.logger.Info("Connection state changed",
					zap.String("from", lastState.String()),
					zap.String("to", currentState.String()))

				lastState = currentState

				// Notify state change
				if m.onConnectionChanged != nil {
					m.onConnectionChanged(currentState)
				}
			}

			// If disconnected, try to reconnect
			if currentState == connectivity.TransientFailure ||
				currentState == connectivity.Shutdown {
				m.triggerReconnect()
			}
		}

		// Handle reconnection signal
		select {
		case <-m.ctx.Done():
			return
		case <-m.reconnectCh:
			m.handleReconnect()
		case <-time.After(time.Second):
			// Regular check interval
		}
	}
}

// handleReconnect handles the reconnection process
func (m *Manager) handleReconnect() {
	m.mutex.Lock()
	wasRegistered := m.isRegistered
	m.mutex.Unlock()

	// Close existing resources
	m.streamMutex.Lock()
	if m.stream != nil {
		_ = m.stream.CloseSend()
		m.stream = nil
	}
	m.streamMutex.Unlock()

	// Try to reconnect
	for i := 0; i < 5; i++ {
		if m.isShutdown {
			return
		}

		m.logger.Info("Attempting to reconnect", zap.Int("attempt", i+1))

		// Wait with exponential backoff
		backoffFactor := 1 << i
		jitterFactor := 1.0 + DefaultReconnectJitter*(float64(2*time.Now().UnixNano()%100)/100.0)
		backoffDuration := time.Duration(float64(DefaultReconnectBackoff) * float64(backoffFactor) * jitterFactor)
		if backoffDuration > DefaultMaxReconnectBackoff {
			backoffDuration = DefaultMaxReconnectBackoff
		}
		time.Sleep(backoffDuration)

		// Try to start stream
		ctx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
		err := m.startStream(ctx)
		cancel()

		if err == nil {
			m.logger.Info("Successfully reconnected")

			// Re-register if needed
			if wasRegistered {
				m.mutex.Lock()
				m.isRegistered = false
				m.mutex.Unlock()

				registrationCtx, registrationCancel := context.WithTimeout(m.ctx, 5*time.Second)
				if err := m.Register(registrationCtx); err != nil {
					registrationCancel()
					m.logger.Error("Failed to re-register after reconnection", zap.Error(err))
					continue
				}
				registrationCancel()

				// Restart watching
				watchCtx, watchCancel := context.WithTimeout(m.ctx, 5*time.Second)
				if err := m.StartWatching(watchCtx); err != nil {
					watchCancel()
					m.logger.Error("Failed to restart watching after reconnection", zap.Error(err))
				}
				watchCancel()
			}

			return
		}

		m.logger.Error("Reconnection attempt failed", zap.Error(err))
	}

	m.logger.Error("Failed to reconnect after multiple attempts")
}
