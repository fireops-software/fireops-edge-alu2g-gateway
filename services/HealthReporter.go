package services

import (
	"time"

	"github.com/fireops-software/fireops-edge-alu2g-gateway/domain"
	"github.com/uoul/go-common/collections"
	"github.com/uoul/go-common/health"
	"github.com/uoul/go-common/log"
)

type HealthReporter struct {
	logger         log.ILogger
	publisher      IPublishService
	healthExchange string
	reportInterval time.Duration
	serviceName    string

	stop chan bool
}

// Close implements IService.
func (h *HealthReporter) Close() error {
	h.stop <- true
	return nil
}

// Run implements IService.
func (h *HealthReporter) Run() {
	ticker := time.NewTicker(h.reportInterval)
LP1:
	for {
		select {
		case <-h.stop:
			break LP1
		case <-ticker.C:
			// Execute Readyness checks
			e := health.GetHealthMonitor().DoReadynessChecks()
			// Evaluate current state
			s := domain.STATE_NOT_READY
			if len(e) <= 0 {
				s = domain.STATE_READY
			}
			currentState := &domain.Health{
				ServiceName: h.serviceName,
				Timestamp:   time.Now(),
				State:       s,
				Errors:      collections.MapSlice(e, func(e error) string { return e.Error() }),
				Description: "",
			}
			// Publish
			err := h.publisher.Publish(h.healthExchange, currentState)
			if err != nil {
				h.logger.Errorf("failed to publish current health state - %v", err)
			}
		}
	}
}

func WithHealthReporterInterval(interval time.Duration) func(*HealthReporter) {
	return func(hr *HealthReporter) {
		hr.reportInterval = interval
	}
}

func NewHealthReporter(logger log.ILogger, publisher IPublishService, healthExchange string, serviceName string, opts ...func(*HealthReporter)) IService {
	hr := &HealthReporter{
		logger:         logger,
		publisher:      publisher,
		healthExchange: healthExchange,
		reportInterval: 30 * time.Second,
		serviceName:    serviceName,
		stop:           make(chan bool),
	}
	for _, o := range opts {
		o(hr)
	}
	return hr
}
