package health

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/3vilhamster/shard-distributor-over-etcd/gen/proto"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/client/connection"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/client/shard"
)

// ReporterConfig defines configuration for the health reporter
type ReporterConfig struct {
	// ReportInterval is the interval between health reports
	ReportInterval time.Duration

	// CustomMetricsProvider provides custom metrics for health reports
	CustomMetricsProvider CustomMetricsProvider

	// InitialStatus is the initial status of the instance
	InitialStatus proto.StatusReport_Status
}

// DefaultReporterConfig provides default configuration values
var DefaultReporterConfig = ReporterConfig{
	ReportInterval: time.Second * 10,
	InitialStatus:  proto.StatusReport_ACTIVE,
}

// CustomMetricsProvider provides custom metrics for health reports
type CustomMetricsProvider interface {
	// GetCustomMetrics returns custom metrics for health reports
	GetCustomMetrics() (map[string]float64, error)
}

// Reporter reports health information to the shard distributor
type Reporter struct {
	connectionMgr  *connection.Manager
	shardProcessor *shard.Processor
	config         ReporterConfig
	logger         *zap.Logger
	statusMutex    sync.RWMutex
	currentStatus  proto.StatusReport_Status
	ticker         *time.Ticker
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
}

// NewReporter creates a new health reporter
func NewReporter(
	connectionMgr *connection.Manager,
	shardProcessor *shard.Processor,
	logger *zap.Logger,
	config ReporterConfig,
) *Reporter {
	// Set default config values if not provided
	if config.ReportInterval == 0 {
		config.ReportInterval = DefaultReporterConfig.ReportInterval
	}

	ctx, cancel := context.WithCancel(context.Background())

	reporter := &Reporter{
		connectionMgr:  connectionMgr,
		shardProcessor: shardProcessor,
		config:         config,
		logger:         logger,
		currentStatus:  config.InitialStatus,
		ctx:            ctx,
		cancel:         cancel,
	}

	return reporter
}

// Start starts the health reporter
func (r *Reporter) Start() error {
	r.logger.Info("Starting health reporter",
		zap.Duration("reportInterval", r.config.ReportInterval))

	r.ticker = time.NewTicker(r.config.ReportInterval)

	r.wg.Add(1)
	go r.reportLoop()

	return nil
}

// Stop stops the health reporter
func (r *Reporter) Stop() error {
	r.logger.Info("Stopping health reporter")

	if r.ticker != nil {
		r.ticker.Stop()
	}

	r.cancel()
	r.wg.Wait()

	return nil
}

// SetStatus sets the status of the instance
func (r *Reporter) SetStatus(status proto.StatusReport_Status) {
	r.statusMutex.Lock()
	defer r.statusMutex.Unlock()

	if r.currentStatus != status {
		r.logger.Info("Changing instance status",
			zap.String("oldStatus", r.currentStatus.String()),
			zap.String("newStatus", status.String()))
	}

	r.currentStatus = status

	// Trigger an immediate report
	go r.sendReport()
}

// GetStatus returns the current status of the instance
func (r *Reporter) GetStatus() proto.StatusReport_Status {
	r.statusMutex.RLock()
	defer r.statusMutex.RUnlock()

	return r.currentStatus
}

// reportLoop periodically sends health reports
func (r *Reporter) reportLoop() {
	defer r.wg.Done()

	// Send initial report
	r.sendReport()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-r.ticker.C:
			r.sendReport()
		}
	}
}

// sendReport sends a health report to the server
func (r *Reporter) sendReport() {
	if !r.connectionMgr.IsRegistered() {
		r.logger.Debug("Instance not registered, skipping health report")
		return
	}

	// Get shard counts
	activeShardCount, err := r.shardProcessor.GetActiveShardCount()
	if err != nil {
		r.logger.Warn("Failed to get active shard count", zap.Error(err))
		activeShardCount = 0
	}

	standbyShardCount, err := r.shardProcessor.GetStandbyShardCount()
	if err != nil {
		r.logger.Warn("Failed to get standby shard count", zap.Error(err))
		standbyShardCount = 0
	}

	// Get custom metrics
	customMetrics := make(map[string]float64)
	if r.config.CustomMetricsProvider != nil {
		metrics, err := r.config.CustomMetricsProvider.GetCustomMetrics()
		if err != nil {
			r.logger.Warn("Failed to get custom metrics", zap.Error(err))
		} else {
			customMetrics = metrics
		}
	}

	// Get current status
	r.statusMutex.RLock()
	status := r.currentStatus
	r.statusMutex.RUnlock()

	// Create status report
	report := &proto.StatusReport{
		InstanceId:        r.connectionMgr.InstanceID(),
		Status:            status,
		ActiveShardCount:  int32(activeShardCount),
		StandbyShardCount: int32(standbyShardCount),
		CustomMetrics:     customMetrics,
	}

	// Send report
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := r.connectionMgr.ReportStatus(ctx, report); err != nil {
		r.logger.Warn("Failed to send health report", zap.Error(err))
	} else {
		r.logger.Debug("Sent health report",
			zap.String("status", status.String()),
			zap.Int("activeShardCount", activeShardCount),
			zap.Int("standbyShardCount", standbyShardCount))
	}
}

// InstanceID returns the instance ID
func (r *Reporter) InstanceID() string {
	return r.connectionMgr.InstanceID()
}
