package registry

import (
	"time"

	"github.com/3vilhamster/shard-distributor-over-etcd/gen/proto/sharddistributor/v1"
)

// InstanceData contains information about a service instance
type InstanceData struct {
	// Basic instance information
	InstanceID string
	Info       *proto.InstanceInfo
	Status     *proto.StatusReport
	Endpoint   string

	// Communication streams
	Streams []proto.ShardDistributorService_ShardDistributorStreamServer

	// Stats and metrics
	Stats InstanceStats
}

// InstanceStats contains operational statistics for an instance
type InstanceStats struct {
	// Connection stats
	ConnectedAt      time.Time
	LastDisconnectAt time.Time
	ReconnectCount   int

	// Performance metrics
	AverageLatency time.Duration
	PeakLoad       float64

	// Shard handling stats
	TotalShardsHandled   int
	CurrentActiveShards  int
	CurrentStandbyShards int
}

// NewInstanceData creates a new instance data object
func NewInstanceData(
	instanceID string,
	info *proto.InstanceInfo,
	endpoint string,
	now time.Time,
) *InstanceData {
	return &InstanceData{
		InstanceID: instanceID,
		Info:       info,
		Status: &proto.StatusReport{
			InstanceId: instanceID,
			Status:     proto.StatusReport_STATUS_ACTIVE,
		},
		Endpoint: endpoint,
		Streams:  make([]proto.ShardDistributorService_ShardDistributorStreamServer, 0),
		Stats: InstanceStats{
			ConnectedAt: now,
		},
	}
}

// AddStream adds a new stream to the instance
func (i *InstanceData) AddStream(stream proto.ShardDistributorService_ShardDistributorStreamServer) {
	i.Streams = append(i.Streams, stream)
}

// RemoveStream removes a stream from the instance
func (i *InstanceData) RemoveStream(stream proto.ShardDistributorService_ShardDistributorStreamServer) bool {
	for idx, s := range i.Streams {
		if s == stream {
			// Remove this stream by replacing with the last element and truncating
			lastIdx := len(i.Streams) - 1
			if idx < lastIdx {
				i.Streams[idx] = i.Streams[lastIdx]
			}
			i.Streams = i.Streams[:lastIdx]
			return true
		}
	}
	return false
}

// GetStreamCount returns the number of active streams
func (i *InstanceData) GetStreamCount() int {
	return len(i.Streams)
}

// UpdateStatus updates the instance status
func (i *InstanceData) UpdateStatus(status *proto.StatusReport) {
	i.Status = status
}

// IsDraining returns whether the instance is in draining state
func (i *InstanceData) IsDraining() bool {
	return i.Status.Status == proto.StatusReport_STATUS_DRAINING
}
