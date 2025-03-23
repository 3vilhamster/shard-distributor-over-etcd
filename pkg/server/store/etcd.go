package store

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// Constants for etcd paths
const (
	InstancesPrefix      = "/services/"
	ShardsPrefix         = "/shards/"
	ShardVersionsPrefix  = "/shard-versions/"
	GlobalVersionKey     = "/shard-distributor/global-version"
	WorkloadGroupsPrefix = "/workload-groups/"
)

// EtcdStore provides storage using etcd
type EtcdStore struct {
	client  clientv3.KV
	watcher clientv3.Watcher
	leaser  clientv3.Lease
	logger  *zap.Logger
}

// EtcdStoreParams defines dependencies for creating an EtcdStore
type EtcdStoreParams struct {
	fx.In

	Client *clientv3.Client
	Logger *zap.Logger
}

// NewEtcdStore creates a new etcd store
func NewEtcdStore(params EtcdStoreParams) (*EtcdStore, error) {
	params.Logger.Info("Creating new etcd store")

	// check etcd
	endpoints := params.Client.Endpoints()
	for _, endpoint := range endpoints {
		st, err := params.Client.Status(context.Background(), endpoint)
		if err != nil {
			params.Logger.Warn("Failed to connect to etcd", zap.String("endpoint", endpoint), zap.Error(err))
			return nil, err
		}
		params.Logger.Info("etcd status", zap.String("endpoint", endpoint), zap.Any("status", st))
	}

	return &EtcdStore{
		client:  params.Client,
		watcher: params.Client,
		leaser:  params.Client,
		logger:  params.Logger,
	}, nil
}

// SaveInstance saves an instance to etcd
func (s *EtcdStore) SaveInstance(
	ctx context.Context,
	instanceID string,
	endpoint string,
	leaseID clientv3.LeaseID,
) error {
	key := InstancesPrefix + instanceID
	_, err := s.client.Put(ctx, key, endpoint, clientv3.WithLease(leaseID))
	if err != nil {
		return fmt.Errorf("failed to save instance: %w", err)
	}
	return nil
}

// DeleteInstance removes an instance from etcd
func (s *EtcdStore) DeleteInstance(ctx context.Context, instanceID string) error {
	key := InstancesPrefix + instanceID
	_, err := s.client.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to delete instance: %w", err)
	}
	return nil
}

// GetInstances retrieves all instances from etcd
func (s *EtcdStore) GetInstances(ctx context.Context) (map[string]string, error) {
	resp, err := s.client.Get(ctx, InstancesPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to get instances: %w", err)
	}

	instances := make(map[string]string)
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		instanceID := strings.TrimPrefix(key, InstancesPrefix)
		endpoint := string(kv.Value)
		instances[instanceID] = endpoint
	}

	return instances, nil
}

// SaveShardAssignments saves shard assignments to etcd
func (s *EtcdStore) SaveShardAssignments(
	ctx context.Context,
	assignments map[string]string,
) error {
	// Use a transaction for atomicity
	txn := s.client.Txn(ctx)

	// Create operations for all assignments
	ops := make([]clientv3.Op, 0, len(assignments))
	for shardID, instanceID := range assignments {
		key := ShardsPrefix + shardID
		ops = append(ops, clientv3.OpPut(key, instanceID))
	}

	// Execute the transaction
	_, err := txn.Then(ops...).Commit()
	if err != nil {
		return fmt.Errorf("failed to save shard assignments: %w", err)
	}

	return nil
}

// GetShardAssignments retrieves all shard assignments from etcd
func (s *EtcdStore) GetShardAssignments(ctx context.Context) (map[string]string, error) {
	resp, err := s.client.Get(ctx, ShardsPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to get shard assignments: %w", err)
	}

	assignments := make(map[string]string)
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		shardID := strings.TrimPrefix(key, ShardsPrefix)
		instanceID := string(kv.Value)
		assignments[shardID] = instanceID
	}

	return assignments, nil
}

// SaveShardVersion saves a shard version to etcd
func (s *EtcdStore) SaveShardVersion(
	ctx context.Context,
	shardID string,
	version int64,
) error {
	key := ShardVersionsPrefix + shardID
	_, err := s.client.Put(ctx, key, strconv.FormatInt(version, 10))
	if err != nil {
		return fmt.Errorf("failed to save shard version: %w", err)
	}
	return nil
}

// GetShardVersion retrieves a shard version from etcd
func (s *EtcdStore) GetShardVersion(ctx context.Context, shardID string) (int64, error) {
	key := ShardVersionsPrefix + shardID
	resp, err := s.client.Get(ctx, key)
	if err != nil {
		return 0, fmt.Errorf("failed to get shard version: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return 0, nil
	}

	version, err := strconv.ParseInt(string(resp.Kvs[0].Value), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse shard version: %w", err)
	}

	return version, nil
}

// GetGlobalVersion retrieves the global version from etcd
func (s *EtcdStore) GetGlobalVersion(ctx context.Context) (int64, error) {
	resp, err := s.client.Get(ctx, GlobalVersionKey)
	if err != nil {
		return 0, fmt.Errorf("failed to get global version: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return 0, nil
	}

	version, err := strconv.ParseInt(string(resp.Kvs[0].Value), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse global version: %w", err)
	}

	return version, nil
}

// IncrementGlobalVersion atomically increments the global version in etcd
func (s *EtcdStore) IncrementGlobalVersion(ctx context.Context) (int64, error) {
	// Get current version
	currentVersion, err := s.GetGlobalVersion(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get current global version: %w", err)
	}

	// Increment
	newVersion := currentVersion + 1

	// Use a transaction for optimistic concurrency control
	txn := s.client.Txn(ctx)
	txn = txn.If(clientv3.Compare(clientv3.Value(GlobalVersionKey), "=", strconv.FormatInt(currentVersion, 10)))
	txn = txn.Then(clientv3.OpPut(GlobalVersionKey, strconv.FormatInt(newVersion, 10)))

	txnResp, err := txn.Commit()
	if err != nil {
		return 0, fmt.Errorf("failed to commit increment transaction: %w", err)
	}

	if !txnResp.Succeeded {
		// Someone else updated the version, retry
		return s.IncrementGlobalVersion(ctx)
	}

	return newVersion, nil
}

// InitializeGlobalVersion initializes the global version in etcd if it doesn't exist
func (s *EtcdStore) InitializeGlobalVersion(ctx context.Context) error {
	s.logger.Info("Attempting to initialize global version")

	// Use a transaction for atomic check-and-set
	txn := s.client.Txn(ctx)

	s.logger.Info("started tx")

	// If the key doesn't exist
	txn = txn.If(clientv3.Compare(clientv3.CreateRevision(GlobalVersionKey), "=", 0))

	// Then create it with initial value "0"
	txn = txn.Then(clientv3.OpPut(GlobalVersionKey, "0"))

	// Otherwise do nothing
	txn = txn.Else()

	s.logger.Info("committing")

	// Execute the transaction
	txnResp, err := txn.Commit()
	if err != nil {
		s.logger.Warn("Failed to initialize global version", zap.Error(err))
		return err
	}

	s.logger.Info("resp", zap.Any("resp", txnResp))

	if txnResp.Succeeded {
		s.logger.Info("Initialized global version to 0")
	} else {
		// Key already exists, get its current value for logging
		resp, err := s.client.Get(ctx, GlobalVersionKey)
		if err != nil {
			s.logger.Warn("Failed to get existing global version", zap.Error(err))
		} else if len(resp.Kvs) > 0 {
			s.logger.Info("Global version already initialized",
				zap.String("value", string(resp.Kvs[0].Value)))
		}
	}

	return nil
}

// SaveShardGroup saves a workload group definition to etcd
func (s *EtcdStore) SaveShardGroup(ctx context.Context, groupID string, data string) error {
	key := WorkloadGroupsPrefix + groupID

	_, err := s.client.Put(ctx, key, data)
	if err != nil {
		return fmt.Errorf("failed to save shard group: %w", err)
	}

	return nil
}

// GetShardGroups retrieves all workload groups from etcd
func (s *EtcdStore) GetShardGroups(ctx context.Context) (map[string]string, error) {
	resp, err := s.client.Get(ctx, WorkloadGroupsPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to get shard groups: %w", err)
	}

	groups := make(map[string]string)
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		groupID := strings.TrimPrefix(key, WorkloadGroupsPrefix)
		data := string(kv.Value)
		groups[groupID] = data
	}

	return groups, nil
}

// DeleteShardGroup removes a workload group definition
func (s *EtcdStore) DeleteShardGroup(ctx context.Context, groupID string) error {
	key := WorkloadGroupsPrefix + groupID
	_, err := s.client.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to delete shard group: %w", err)
	}
	return nil
}

// WatchInstances sets up a watch for instance changes
func (s *EtcdStore) WatchInstances(ctx context.Context) clientv3.WatchChan {
	return s.watcher.Watch(ctx, InstancesPrefix, clientv3.WithPrefix())
}

// WatchShardAssignments sets up a watch for shard assignment changes
func (s *EtcdStore) WatchShardAssignments(ctx context.Context) clientv3.WatchChan {
	return s.watcher.Watch(ctx, ShardsPrefix, clientv3.WithPrefix())
}

// WatchGlobalVersion sets up a watch for global version changes
func (s *EtcdStore) WatchGlobalVersion(ctx context.Context) clientv3.WatchChan {
	return s.watcher.Watch(ctx, GlobalVersionKey)
}

// WatchShardGroup sets up a watch for a specific shard group
func (s *EtcdStore) WatchShardGroup(ctx context.Context, groupID string) clientv3.WatchChan {
	key := WorkloadGroupsPrefix + groupID
	return s.watcher.Watch(ctx, key)
}

// BatchOperation performs multiple operations in a single transaction
func (s *EtcdStore) BatchOperation(ctx context.Context, ops []clientv3.Op) error {
	if len(ops) == 0 {
		return nil
	}

	// Execute the transaction
	txn := s.client.Txn(ctx)
	_, err := txn.Then(ops...).Commit()
	if err != nil {
		return fmt.Errorf("failed to perform batch operation: %w", err)
	}

	return nil
}

// CreateLease creates a new lease with the specified TTL
func (s *EtcdStore) CreateLease(ctx context.Context, ttl int64) (clientv3.LeaseID, error) {
	lease, err := s.leaser.Grant(ctx, ttl)
	if err != nil {
		return 0, fmt.Errorf("failed to create lease: %w", err)
	}
	return lease.ID, nil
}

// RevokeLease revokes a lease
func (s *EtcdStore) RevokeLease(ctx context.Context, leaseID clientv3.LeaseID) error {
	_, err := s.leaser.Revoke(ctx, leaseID)
	if err != nil {
		return fmt.Errorf("failed to revoke lease: %w", err)
	}
	return nil
}

// KeepAliveLease keeps a lease alive once
func (s *EtcdStore) KeepAliveLease(ctx context.Context, leaseID clientv3.LeaseID) error {
	_, err := s.leaser.KeepAliveOnce(ctx, leaseID)
	if err != nil {
		return fmt.Errorf("failed to keep lease alive: %w", err)
	}
	return nil
}
