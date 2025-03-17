package sharding

import (
	"hash/fnv"
	"sort"
	"strconv"
)

// ConsistentHashRing implements a consistent hash ring for shard distribution
type ConsistentHashRing struct {
	VirtualNodes int                 // Number of virtual nodes per real node
	hashRing     []uint32            // Sorted hash values on the ring
	nodeMap      map[uint32]string   // Map from hash to node ID
	reverseMap   map[string][]uint32 // Map from node ID to its hashes
}

// NewConsistentHashRing creates a new consistent hash ring
func NewConsistentHashRing(virtualNodes int) *ConsistentHashRing {
	return &ConsistentHashRing{
		VirtualNodes: virtualNodes,
		hashRing:     make([]uint32, 0),
		nodeMap:      make(map[uint32]string),
		reverseMap:   make(map[string][]uint32),
	}
}

// AddNode adds a node to the hash ring
func (r *ConsistentHashRing) AddNode(nodeID string) {
	// Store all hash values for this node for easy removal
	hashes := make([]uint32, 0, r.VirtualNodes)

	// Add the node to the ring multiple times (virtual nodes)
	for i := 0; i < r.VirtualNodes; i++ {
		// Create a unique virtual node ID
		virtualID := nodeID + ":" + strconv.Itoa(i)
		hash := hashKey(virtualID)

		r.hashRing = append(r.hashRing, hash)
		r.nodeMap[hash] = nodeID
		hashes = append(hashes, hash)
	}

	// Store the hashes for this node
	r.reverseMap[nodeID] = hashes

	// Sort the hash ring
	sort.Slice(r.hashRing, func(i, j int) bool {
		return r.hashRing[i] < r.hashRing[j]
	})
}

// RemoveNode removes a node from the hash ring
func (r *ConsistentHashRing) RemoveNode(nodeID string) {
	// Get the hashes for this node
	hashes, exists := r.reverseMap[nodeID]
	if !exists {
		return
	}

	// Remove the node from the maps
	for _, hash := range hashes {
		delete(r.nodeMap, hash)
	}

	// Remove the node's hashes from the ring
	newRing := make([]uint32, 0, len(r.hashRing)-len(hashes))
	for _, hash := range r.hashRing {
		if _, exists := r.nodeMap[hash]; exists {
			newRing = append(newRing, hash)
		}
	}

	// Update the hash ring
	r.hashRing = newRing

	// Remove from reverse map
	delete(r.reverseMap, nodeID)
}

// GetNode returns the node responsible for the given key
func (r *ConsistentHashRing) GetNode(key string) string {
	if len(r.hashRing) == 0 {
		return ""
	}

	// Hash the key
	hash := hashKey(key)

	// Find the first node with hash >= key's hash
	idx := sort.Search(len(r.hashRing), func(i int) bool {
		return r.hashRing[i] >= hash
	})

	// If we went past the end, wrap around to the first node
	if idx == len(r.hashRing) {
		idx = 0
	}

	// Return the node ID
	return r.nodeMap[r.hashRing[idx]]
}

// GetAllNodes returns all nodes in the hash ring
func (r *ConsistentHashRing) GetAllNodes() []string {
	nodes := make([]string, 0, len(r.reverseMap))
	for nodeID := range r.reverseMap {
		nodes = append(nodes, nodeID)
	}
	return nodes
}

// GetNodeCount returns the number of nodes in the hash ring
func (r *ConsistentHashRing) GetNodeCount() int {
	return len(r.reverseMap)
}

// hashKey produces a hash for the given key
func hashKey(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

// ConsistentHashStrategy implements a consistent hashing sharding strategy
type ConsistentHashStrategy struct {
	VirtualNodes int
	ring         *ConsistentHashRing
}

// NewConsistentHashStrategy creates a new consistent hashing strategy
func NewConsistentHashStrategy(virtualNodes int) *ConsistentHashStrategy {
	return &ConsistentHashStrategy{
		VirtualNodes: virtualNodes,
		ring:         NewConsistentHashRing(virtualNodes),
	}
}

// CalculateDistribution determines optimal shard placement using consistent hashing
func (s *ConsistentHashStrategy) CalculateDistribution(
	currentMap map[string]string,
	instances map[string]InstanceInfo,
) map[string]string {
	// Create a new ring for this calculation
	ring := NewConsistentHashRing(s.VirtualNodes)

	// Add all active instances to the ring
	for id, info := range instances {
		if info.Status == "active" {
			ring.AddNode(id)
		}
	}

	// If no active instances, return empty map
	if ring.GetNodeCount() == 0 {
		return make(map[string]string)
	}

	// Assign shards using consistent hashing
	distribution := make(map[string]string)
	for shardID := range currentMap {
		distribution[shardID] = ring.GetNode(shardID)
	}

	return distribution
}

// InstanceInfo contains information about a service instance
type InstanceInfo struct {
	ID         string
	Status     string
	LoadFactor float64
	ShardCount int
}
