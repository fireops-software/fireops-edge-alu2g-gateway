package services

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"hash/crc32"

	"github.com/fireops-software/fireops-edge-alu2g-gateway/domain"
	"github.com/fireops-software/fireops-edge-alu2g-gateway/pkg/buffer"
	"github.com/uoul/go-common/log"
)

// -----------------------------------------------------------------------------------
// Type
// -----------------------------------------------------------------------------------
type AlertManager struct {
	logger               log.ILogger
	alertSrc             INotificationService[domain.AlertCollection]
	rabbitMqPublisher    IPublishService
	alertSrcBufferSize   uint
	newAlertsExchange    string
	activeAlertsExchange string

	stop           chan bool
	alertHistory   *buffer.RingBuffer[domain.AlertId]
	backupChecksum uint32
}

//-----------------------------------------------------------------------------------
// Public
//-----------------------------------------------------------------------------------

// Close implements IService.
func (a *AlertManager) Close() error {
	a.stop <- true
	return nil
}

// Run implements IService.
func (a *AlertManager) Run() {
	// Subscribe for alerts
	src := a.alertSrc.Subscribe(a.alertSrcBufferSize)
	defer a.alertSrc.Unsubscribe(src)

	// Run
LP1:
	for {
		select {
		case <-a.stop:
			break LP1
		case alerts := <-src:
			if alerts.Error == nil {
				a.logger.Tracef("incomming data on AlertManager: %v", alerts.Result)
				// Check for changes
				checksum, err := createCrc32(alerts.Result)
				if err != nil {
					a.logger.Errorf("failed to create checksum for incomming alerts - %v", err)
				}
				if checksum != a.backupChecksum || err != nil {
					a.backupChecksum = checksum
					err := a.rabbitMqPublisher.Publish(a.activeAlertsExchange, &alerts.Result)
					if err != nil {
						a.logger.Errorf(err.Error())
					}
				}
				// Check new alerts
				newAlerts := a.getNewAlerts(alerts.Result)
				if len(newAlerts.Alerts) > 0 {
					a.logger.Infof("new alert: %v", mustJson(newAlerts))
					err := a.rabbitMqPublisher.Publish(a.newAlertsExchange, &newAlerts)
					if err != nil {
						a.logger.Errorf(err.Error())
					}
				}
			}
		}
	}
}

// -----------------------------------------------------------------------------------
// Private
// -----------------------------------------------------------------------------------
func (a *AlertManager) getNewAlerts(alertCollection domain.AlertCollection) domain.AlertCollection {
	newAlerts := domain.AlertCollection{
		Alerts: map[domain.AlertId]domain.Alert{},
	}
	for alertId, alert := range alertCollection.Alerts {
		if !a.alertHistory.Contains(alertId) {
			newAlerts.Alerts[alertId] = alert
			a.alertHistory.Push(alertId)
		}
	}
	return newAlerts
}

func createCrc32[T any](obj T) (uint32, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(obj); err != nil {
		return 0, err
	}
	return crc32.ChecksumIEEE(buf.Bytes()), nil
}

func mustJson(item any) string {
	data, _ := json.Marshal(item)
	return string(data)
}

// -----------------------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------------------
func NewAlertManager(
	logger log.ILogger,
	alertSrc INotificationService[domain.AlertCollection],
	rabbitMqPublisher IPublishService,
	activeAlertsExchange string,
	newAlertsExchange string,
	opts ...func(*AlertManager),
) IService {
	am := &AlertManager{
		logger:               logger,
		alertSrc:             alertSrc,
		rabbitMqPublisher:    rabbitMqPublisher,
		activeAlertsExchange: activeAlertsExchange,
		newAlertsExchange:    newAlertsExchange,

		alertSrcBufferSize: 10,

		stop:         make(chan bool),
		alertHistory: buffer.NewRingBuffer[domain.AlertId](50),
	}
	for _, o := range opts {
		o(am)
	}
	return am
}

// -----------------------------------------------------------------------------------
// Options
// -----------------------------------------------------------------------------------
