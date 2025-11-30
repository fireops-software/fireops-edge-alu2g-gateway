package services

import (
	"context"
	"encoding/json"

	"github.com/fireops-software/fireops-edge-alu2g-gateway/domain"
	"github.com/fireops-software/fireops-edge-alu2g-gateway/pkg/buffer"
	"github.com/rabbitmq/amqp091-go"
	"github.com/uoul/go-common/log"
	"github.com/uoul/go-common/messaging"
)

// -----------------------------------------------------------------------------------
// Type
// -----------------------------------------------------------------------------------
type EventManager struct {
	logger             log.ILogger
	eventSrc           INotificationService[[]domain.Event]
	messenger          messaging.IMessenger[messaging.RabbitMqExchange, amqp091.Delivery]
	eventSrcBufferSize uint
	eventsExchange     messaging.RabbitMqExchange
	ctx                context.Context

	eventHistory *buffer.RingBuffer[string]
}

//-----------------------------------------------------------------------------------
// Public
//-----------------------------------------------------------------------------------

// Run implements IService.
func (a *EventManager) Run() {
	// Subscribe for events
	src := a.eventSrc.Subscribe(a.eventSrcBufferSize)
	defer a.eventSrc.Unsubscribe(src)

	// Run
LP1:
	for {
		select {
		case <-a.ctx.Done():
			break LP1
		case events := <-src:
			if events.Error == nil {
				a.logger.Debugf("incomming data on EventManager: %v", events.Result)
				// Publish active events
				err := a.messenger.Publish(a.eventsExchange, &events.Result)
				if err != nil {
					a.logger.Errorf(err.Error())
				}
				// Check new events (print to console)
				newEvents := a.getNewEvents(events.Result)
				if len(newEvents) > 0 {
					a.logger.Infof("new event: %v", mustJson(newEvents))
				}
			}
		}
	}
}

// -----------------------------------------------------------------------------------
// Private
// -----------------------------------------------------------------------------------
func (a *EventManager) getNewEvents(events []domain.Event) []domain.Event {
	newEvents := []domain.Event{}
	for _, e := range events {
		if e.Num1 != nil && !a.eventHistory.Contains(*e.Num1) {
			newEvents = append(newEvents, e)
			a.eventHistory.Push(*e.Num1)
		}
	}
	return newEvents
}

func mustJson(item any) string {
	data, _ := json.Marshal(item)
	return string(data)
}

// -----------------------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------------------
func NewEventManager(
	ctx context.Context,
	logger log.ILogger,
	eventSrc INotificationService[[]domain.Event],
	messenger messaging.IMessenger[messaging.RabbitMqExchange, amqp091.Delivery],
	eventsExcahnge messaging.RabbitMqExchange,
	opts ...func(*EventManager),
) IService {
	am := &EventManager{
		logger:         logger,
		eventSrc:       eventSrc,
		messenger:      messenger,
		eventsExchange: eventsExcahnge,

		eventSrcBufferSize: 10,

		ctx:          ctx,
		eventHistory: buffer.NewRingBuffer[string](50),
	}
	for _, o := range opts {
		o(am)
	}
	return am
}

// -----------------------------------------------------------------------------------
// Options
// -----------------------------------------------------------------------------------
