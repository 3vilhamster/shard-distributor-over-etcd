package sharding

import (
	"github.com/dgryski/go-farm" // Import the farmhash library
)

// SimpleHashDistributor provides a straightforward hash-based distribution
type SimpleHashDistributor struct{}

// NewSimpleHashDistributor creates a new simple hash-based distributor
func NewSimpleHashDistributor() *SimpleHashDistributor {
	return &SimpleHashDistributor{}
}

// CalculateDistribution determines shard placement using simple hash distribution
func (d *SimpleHashDistributor) CalculateDistribution(
	currentMap map[string]string,
	instances map[string]InstanceInfo,
) map[string]string {
	// Create a new distribution map
	distribution := make(map[string]string)

	// If no instances available, return empty map
	if len(instances) == 0 {
		return distribution
	}

	// Convert instance map to a sorted slice for consistent indexing
	hosts := make([]string, 0, len(instances))
	for id, info := range instances {
		// Skip draining or inactive instances
		if info.Status == "active" {
			hosts = append(hosts, id)
		}
	}

	// If no active hosts, return empty map
	if len(hosts) == 0 {
		return distribution
	}

	for shardID := range currentMap {
		// Calculate hash using farmhash
		hash := int(farm.Fingerprint32([]byte(shardID)))

		// Get instance index using modulo
		idx := hash % len(hosts)

		// Assign shard to selected instance
		distribution[shardID] = hosts[idx]
	}

	return distribution
}
