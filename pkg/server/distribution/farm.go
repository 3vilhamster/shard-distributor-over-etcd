package distribution

import (
	"github.com/dgryski/go-farm" // Import the farmhash library

	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/server/store"
)

// FarmHashDistributor provides a straightforward hash-based distribution
type FarmHashDistributor struct{}

// NewFarmHashStrategy creates a new simple hash-based distributor
func NewFarmHashStrategy() *FarmHashDistributor {
	return &FarmHashDistributor{}
}

func (d *FarmHashDistributor) Name() string {
	return "FarmHash"
}

// CalculateDistribution determines shard placement using simple hash distribution
func (d *FarmHashDistributor) CalculateDistribution(
	currentMap map[string]store.Assignment,
	instances map[string]InstanceInfo,
) map[string]store.Assignment {
	// Create a new distribution map
	distribution := make(map[string]store.Assignment)

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
		distribution[shardID] = store.Assignment{
			OwnerID: hosts[idx],
		}
	}

	return distribution
}
