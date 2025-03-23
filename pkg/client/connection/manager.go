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

	"github.com/3vilhamster/shard-distributor-over-etcd/gen/proto/sharddistributor/v1"
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
	conn               *grpc.ClientConn
	client             proto.ShardDistributorServiceClient
	stream             proto.ShardDistributorService_ShardDistributorStreamClient
	instanceID         string
	instanceInfo       *proto.InstanceInfo
	serverAddr         string
	reconnectBackoff   backoff.Config
	heartbeatInterval  time.Duration
	heartbeatTicker    *time.Ticker
	reconnectCh        chan struct{}
	streamMutex        sync.Mutex
	logger             *zap.Logger
	handlers           map[proto.ShardDistributorStreamResponse_MessageType][]ServerMessageHandler
	lastAssignedShards []string
	ctx                context.Context
	cancel             context.CancelFunc
	isShutdown         bool
}

// ServerMessageHandler defines a handler function for server messages
type ServerMessageHandler func(context.Context, *proto.ShardDistributorStreamResponse) error

// NewManager creates a new connection manager
func NewManager(serverAddr, instanceID string, instanceInfo *proto.InstanceInfo) *Manager {
	ctx, cancel := context.WithCancel(context.Background())

	m := &Manager{
		serverAddr:        serverAddr,
		instanceID:        instanceID,
		instanceInfo:      instanceInfo,
		heartbeatInterval: DefaultHeartbeatInterval,
		reconnectCh:       make(chan struct{}, 1),
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

	m.handlers = map[proto.ShardDistributorStreamResponse_MessageType][]ServerMessageHandler{
		proto.ShardDistributorStreamResponse_MESSAGE_TYPE_HEARTBEAT_ACK: {func(ctx context.Context, message *proto.ShardDistributorStreamResponse) error {
			// Just log at debug level
			m.logger.Debug("Received heartbeat acknowledgment")
			return nil
		}},
	}

	return m
}

// Connect establishes a connection to the ShardDistributor service
func (m *Manager) Connect(ctx context.Context) error {
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
	conn, err := grpc.NewClient(m.serverAddr, opts...)
	if err != nil {
		m.logger.Error("Failed to connect to server",
			zap.String("server", m.serverAddr),
			zap.Error(err))
		return fmt.Errorf("failed to connect to server: %w", err)
	}

	m.conn = conn
	m.client = proto.NewShardDistributorServiceClient(conn)

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

// AcknowledgeAssignment acknowledges a shard assignment
func (m *Manager) AcknowledgeAssignment(ctx context.Context, shardID string) error {
	m.logger.Debug("Acknowledging shard assignment",
		zap.String("shardID", shardID))

	// Prepare ack message
	msg := &proto.ShardDistributorStreamRequest{
		InstanceId: m.instanceID,
		Type:       proto.ShardDistributorStreamRequest_MESSAGE_TYPE_ACK,
		ShardId:    shardID,
	}

	// Send ack message
	if err := m.sendMessage(msg); err != nil {
		return fmt.Errorf("failed to send ack message: %w", err)
	}

	return nil
}

// RegisterHandler registers a handler for a specific message type
func (m *Manager) RegisterHandler(msgType proto.ShardDistributorStreamResponse_MessageType, handler ServerMessageHandler) {
	m.handlers[msgType] = append(m.handlers[msgType], handler)
}

// Shutdown gracefully shuts down the connection manager
func (m *Manager) Shutdown(ctx context.Context) error {
	if m.isShutdown {
		return nil // Already shut down
	}

	m.logger.Info("Shutting down connection manager")
	m.isShutdown = true

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

// sendMessage sends a message to the server
func (m *Manager) sendMessage(msg *proto.ShardDistributorStreamRequest) error {
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
func (m *Manager) handleServerMessage(msg *proto.ShardDistributorStreamResponse) error {
	handlers := m.handlers[msg.Type]

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
				msg := &proto.ShardDistributorStreamRequest{
					InstanceId: m.instanceID,
					Type:       proto.ShardDistributorStreamRequest_MESSAGE_TYPE_HEARTBEAT,
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
	var lastState = connectivity.Idle
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
			return
		}

		m.logger.Error("Reconnection attempt failed", zap.Error(err))
	}

	m.logger.Error("Failed to reconnect after multiple attempts")
}
