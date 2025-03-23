package store

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// EtcdStore provides storage using etcd
type EtcdStore struct {
	client       *clientv3.Client
	logger       *zap.Logger
	globalPrefix string // Global prefix for all keys
}

// EtcdStoreParams defines dependencies for creating an EtcdStore
type EtcdStoreParams struct {
	fx.In

	Client       *clientv3.Client
	Logger       *zap.Logger
	GlobalPrefix string `optional:"true" name:"etcdGlobalPrefix"`
}

// NewEtcdStore creates a new etcd store
func NewEtcdStore(params EtcdStoreParams) (Store, error) {
	// Set default global prefix if not provided
	globalPrefix := params.GlobalPrefix
	if globalPrefix == "" {
		globalPrefix = "/shard-distributor"
	}

	// Ensure prefix starts with / and doesn't end with /
	if !strings.HasPrefix(globalPrefix, "/") {
		globalPrefix = "/" + globalPrefix
	}
	if strings.HasSuffix(globalPrefix, "/") {
		globalPrefix = globalPrefix[:len(globalPrefix)-1]
	}

	params.Logger.Info("Creating new etcd store",
		zap.String("globalPrefix", globalPrefix))

	return &EtcdStore{
		client:       params.Client,
		logger:       params.Logger,
		globalPrefix: globalPrefix,
	}, nil
}

// buildKey builds a full etcd key with the global prefix
func (s *EtcdStore) buildKey(parts ...string) string {
	elements := append([]string{s.globalPrefix}, parts...)
	return strings.Join(elements, "/")
}

// instancesPrefix returns the prefix for instance keys
func (s *EtcdStore) instancesPrefix() string {
	return s.buildKey("instances")
}

// instanceKey returns the key for a specific instance
func (s *EtcdStore) instanceKey(instanceID string) string {
	return s.buildKey("instances", instanceID)
}

// shardGroupsPrefix returns the prefix for shard group keys
func (s *EtcdStore) shardGroupsPrefix() string {
	return s.buildKey("groups")
}

// shardGroupKey returns the key for a specific shard group
func (s *EtcdStore) shardGroupKey(namespace string) string {
	return s.buildKey("groups", namespace)
}

// assignmentsPrefix returns the prefix for assignments within a group
func (s *EtcdStore) assignmentsPrefix(namespace string) string {
	return s.buildKey("assignments", namespace)
}

// assignmentKey returns the key for a specific shard assignment
func (s *EtcdStore) assignmentKey(namespace, shardID string) string {
	return s.buildKey("assignments", namespace, shardID)
}

// SaveInstance saves an instance to etcd with TTL
func (s *EtcdStore) SaveInstance(ctx context.Context, instanceID string, endpoint string, ttl time.Duration) error {
	// Create a lease with the specified TTL
	ttlSeconds := int64(ttl.Seconds())
	if ttlSeconds <= 0 {
		ttlSeconds = 10 // Default 10 second TTL
	}

	lease, err := s.client.Grant(ctx, ttlSeconds)
	if err != nil {
		return fmt.Errorf("failed to create lease: %w", err)
	}

	key := s.instanceKey(instanceID)
	_, err = s.client.Put(ctx, key, endpoint, clientv3.WithLease(lease.ID))
	if err != nil {
		return fmt.Errorf("failed to save instance with lease: %w", err)
	}

	s.logger.Debug("Saved instance with TTL",
		zap.String("instanceID", instanceID),
		zap.Duration("ttl", ttl),
		zap.Int64("leaseID", int64(lease.ID)))

	return nil
}

// GetInstances retrieves all instances from etcd
func (s *EtcdStore) GetInstances(ctx context.Context) (map[string]string, error) {
	prefix := s.instancesPrefix()
	resp, err := s.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to get instances: %w", err)
	}

	instances := make(map[string]string)
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		// Extract instanceID from the key
		instanceID := path.Base(key)
		endpoint := string(kv.Value)
		instances[instanceID] = endpoint
	}

	return instances, nil
}

// SaveShardAssignments saves shard assignments to etcd
func (s *EtcdStore) SaveShardAssignments(
	ctx context.Context,
	namespace string,
	assignments map[string]Assignment,
) error {
	// Use a transaction for atomicity
	ops := make([]clientv3.Op, 0, len(assignments))

	for shardID, assignment := range assignments {
		key := s.assignmentKey(namespace, shardID)

		data, err := json.Marshal(assignment)
		if err != nil {
			return fmt.Errorf("failed to marshal assignment for shard %s: %w", shardID, err)
		}

		ops = append(ops, clientv3.OpPut(key, string(data)))
	}

	// Execute the transaction
	if len(ops) > 0 {
		_, err := s.client.Txn(ctx).Then(ops...).Commit()
		if err != nil {
			return fmt.Errorf("failed to save shard assignments: %w", err)
		}
	}

	return nil
}

// GetShardAssignments retrieves all shard assignments for a group from etcd
func (s *EtcdStore) GetShardAssignments(
	ctx context.Context,
	namespace string,
) (map[string]Assignment, error) {
	prefix := s.assignmentsPrefix(namespace)
	resp, err := s.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to get shard assignments: %w", err)
	}

	assignments := make(map[string]Assignment)
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		// Extract shardID from the key
		shardID := path.Base(key)

		var assignment Assignment
		if err := json.Unmarshal(kv.Value, &assignment); err != nil {
			s.logger.Warn("Failed to unmarshal assignment",
				zap.String("shardID", shardID),
				zap.Error(err))
			continue
		}

		assignments[shardID] = assignment
	}

	return assignments, nil
}

// SaveShardGroup saves a workload group definition to etcd
func (s *EtcdStore) SaveNamespace(
	ctx context.Context,
	namespace string,
	data Namespace,
) error {
	key := s.shardGroupKey(namespace)

	// Ensure the namespace in the data matches the key
	data.Namespace = namespace

	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal shard group: %w", err)
	}

	_, err = s.client.Put(ctx, key, string(jsonData))
	if err != nil {
		return fmt.Errorf("failed to save shard group: %w", err)
	}

	return nil
}

// GetNamespaces retrieves all workload groups from etcd
func (s *EtcdStore) GetNamespaces(ctx context.Context) (map[string]Namespace, error) {
	prefix := s.shardGroupsPrefix()
	resp, err := s.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to get shard groups: %w", err)
	}

	groups := make(map[string]Namespace)
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		// Extract namespace from the key
		namespace := path.Base(key)

		var group Namespace
		if err := json.Unmarshal(kv.Value, &group); err != nil {
			s.logger.Warn("Failed to unmarshal shard group",
				zap.String("namespace", namespace),
				zap.Error(err))
			continue
		}

		groups[namespace] = group
	}

	return groups, nil
}

// GetShardGroup retrieves information about a single group from etcd
func (s *EtcdStore) GetNamespace(
	ctx context.Context,
	namespace string,
) (Namespace, error) {
	key := s.shardGroupKey(namespace)
	resp, err := s.client.Get(ctx, key)
	if err != nil {
		return Namespace{}, fmt.Errorf("failed to get shard group: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return Namespace{}, fmt.Errorf("shard group not found: %s", namespace)
	}

	var group Namespace
	if err := json.Unmarshal(resp.Kvs[0].Value, &group); err != nil {
		return Namespace{}, fmt.Errorf("failed to unmarshal shard group: %w", err)
	}

	return group, nil
}
