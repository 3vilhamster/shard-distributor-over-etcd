package leader

import (
	"context"
	"fmt"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/server/config"
)

// ElectionParams defines the parameters for creating a new election
type ElectionParams struct {
	fx.In

	Client *clientv3.Client
	Logger *zap.Logger
	Config config.Config
}

// Callback is a function that is called when leadership status changes
type Callback func(isLeader bool)

// Election manages leader election for the shard distributor
type Election struct {
	mu           sync.RWMutex
	client       *clientv3.Client
	logger       *zap.Logger
	electionPath string
	session      *concurrency.Session
	election     *concurrency.Election
	isLeader     bool
	leaderKey    string
	callback     Callback
	stopCh       chan struct{}
	isStopped    bool
}

// NewElection creates a new leader election manager
func NewElection(params ElectionParams) (*Election, error) {
	return &Election{
		client:       params.Client,
		logger:       params.Logger,
		electionPath: params.Config.LeaderElectionPath,
		isLeader:     false,
		stopCh:       make(chan struct{}),
	}, nil
}

// Start begins the leader election process
func (e *Election) Start(ctx context.Context) error {
	e.mu.Lock()
	if e.isStopped {
		e.mu.Unlock()
		return fmt.Errorf("election has been stopped")
	}
	e.mu.Unlock()

	// Create a session for leader election with 15s TTL
	session, err := concurrency.NewSession(e.client, concurrency.WithTTL(15))
	if err != nil {
		return fmt.Errorf("failed to create etcd session: %w", err)
	}

	e.mu.Lock()
	e.session = session
	// Create election instance
	e.election = concurrency.NewElection(session, e.electionPath)
	e.mu.Unlock()

	// Start campaigning for leadership
	go e.campaignForLeadership(ctx)

	return nil
}

// Stop stops the leader election
func (e *Election) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.isStopped {
		return
	}

	close(e.stopCh)
	e.isStopped = true

	// If we're the leader, resign
	if e.isLeader && e.election != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := e.election.Resign(ctx)
		if err != nil {
			e.logger.Warn("Failed to resign leadership", zap.Error(err))
		}
	}

	// Close the session
	if e.session != nil {
		e.session.Close()
	}

	e.isLeader = false
	e.logger.Info("Leader election stopped")
}

// campaignForLeadership continuously campaigns for leadership
func (e *Election) campaignForLeadership(ctx context.Context) {
	backoff := 1 * time.Second
	maxBackoff := 10 * time.Second

	for {
		select {
		case <-e.stopCh:
			return
		default:
			// Try to become leader
			e.logger.Debug("Campaigning for leadership")

			campaignCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := e.election.Campaign(campaignCtx, "candidate")
			cancel()

			if err != nil {
				e.logger.Warn("Failed to campaign for leadership", zap.Error(err))
				// Use exponential backoff for retry
				select {
				case <-e.stopCh:
					return
				case <-time.After(backoff):
					backoff = min(backoff*2, maxBackoff)
					continue
				}
			}

			// Reset backoff on success
			backoff = 1 * time.Second

			// Get our leader key to identify our own leadership
			observeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			leaderResp, err := e.election.Leader(observeCtx)
			cancel()

			if err != nil {
				e.logger.Warn("Failed to get leader key", zap.Error(err))
				time.Sleep(time.Second)
				continue
			}

			e.mu.Lock()
			e.leaderKey = string(leaderResp.Kvs[0].Key)
			e.isLeader = true
			e.mu.Unlock()

			e.logger.Info("Became leader for shard distribution")

			// Call the callback
			if e.callback != nil {
				e.callback(true)
			}

			// Watch for leadership changes
			watchCh := e.election.Observe(ctx)
			leaderChanged := false

			for !leaderChanged {
				select {
				case <-e.stopCh:
					return
				case resp, ok := <-watchCh:
					if !ok {
						e.logger.Info("Leadership observer closed")
						leaderChanged = true
						break
					}

					// Check if we're still the leader by comparing keys
					currentLeaderKey := string(resp.Kvs[0].Key)
					myLeaderKey := e.leaderKey

					if currentLeaderKey != myLeaderKey {
						e.logger.Info("Leadership transferred to another instance",
							zap.String("current_key", currentLeaderKey),
							zap.String("my_key", myLeaderKey))
						leaderChanged = true
						break
					}
				}
			}

			e.logger.Info("No longer leader for shard distribution")

			e.mu.Lock()
			e.isLeader = false
			e.mu.Unlock()

			// Call the callback
			if e.callback != nil {
				e.callback(false)
			}

			// Wait before trying again to avoid rapid oscillation
			select {
			case <-e.stopCh:
				return
			case <-time.After(3 * time.Second):
				// Continue with the loop
			}
		}
	}
}

// IsLeader returns whether this instance is the leader
func (e *Election) IsLeader() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.isLeader
}

// SetCallback sets the callback for leadership changes
func (e *Election) SetCallback(callback Callback) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.callback = callback
}
