package services

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/fireops-software/fireops-edge-alu2g-gateway/domain"
	"github.com/uoul/go-common/log"

	appError "github.com/fireops-software/fireops-edge-alu2g-gateway/error"
	amqp "github.com/rabbitmq/amqp091-go"
)

// -----------------------------------------------------------------------------------
// Type
// -----------------------------------------------------------------------------------
type RabbitMqPublisher[T any] struct {
	logger   log.ILogger
	host     string
	port     uint16
	user     string
	password string
	exchange string

	stop            chan bool
	retryInterval   time.Duration
	internalMsgChan chan []byte
}

//-----------------------------------------------------------------------------------
// Public
//-----------------------------------------------------------------------------------

// Close implements IService.
func (r *RabbitMqPublisher[T]) Close() error {
	r.stop <- true
	return nil
}

// Run implements IService.
func (r *RabbitMqPublisher[T]) Run() {
LP1:
	for {
		select {
		case <-r.stop:
			break LP1
		default:
			err := r.execService()
			// Retry on error
			if err != nil {
				r.logger.Error(err.Error())
				time.Sleep(r.retryInterval)
			}
		}
	}
}

// Publish implements IPublishService.
func (r *RabbitMqPublisher[T]) Publish(item *domain.AlertCollection) error {
	data, err := json.Marshal(item)
	if err != nil {
		return appError.NewErrInvalidData("failed to parse data to json string - %v", err)
	}
	r.internalMsgChan <- data
	return nil
}

// -----------------------------------------------------------------------------------
// Private
// -----------------------------------------------------------------------------------

func (r *RabbitMqPublisher[T]) execService() error {
	// Create rabbitmq connection
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d", r.user, r.password, r.host, r.port))
	if err != nil {
		return appError.NewErrTcpConnect("failed to connect to rabbitmq (%s) - %v", fmt.Sprintf("amqp://<USER>:<PASSWORD>@%s:%d", r.host, r.port), err)
	}
	defer conn.Close()

	// Create channel
	ch, err := conn.Channel()
	if err != nil {
		return appError.NewErrRabbitMq("failed to create channel on rabbitmq (%s) - %v", conn.RemoteAddr().String(), err)
	}
	defer ch.Close()

	// Create exchange
	err = ch.ExchangeDeclare(
		r.exchange, // name
		"fanout",   // type
		true,       // durable
		false,      // auto-deleted
		false,      // internal
		false,      // no-wait
		nil,        // arguments
	)
	if err != nil {
		return appError.NewErrRabbitMq("failed to declare exchange on rabbitmq (%s) - %v", conn.RemoteAddr().String(), err)
	}

	// Listen on close event
	closeChan := conn.NotifyClose(make(chan *amqp.Error))

	r.logger.Infof("successfully connected to rabbitmq (%s) and declared exchange %s", conn.RemoteAddr().String(), r.exchange)
	for {
		select {
		case <-r.stop:
			return nil
		case err := <-closeChan:
			return appError.NewErrRabbitMq("rabbitmq connection has been closed - %v", err)
		case msg := <-r.internalMsgChan:
			err := ch.Publish(
				r.exchange,
				"",
				false,
				false,
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        msg,
				},
			)
			if err != nil {
				return appError.NewErrRabbitMq("failed to publish alerts to rabbitmq (%s) on exchange %s - %v", conn.RemoteAddr().String(), r.exchange, err)
			}
			r.logger.Infof("message has been successfully sent to rabbitMq (%s) on exchange %s: %s", conn.RemoteAddr().String(), r.exchange, string(msg))
		}
	}
}

// -----------------------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------------------

func NewRabbitMqPublisher[T any](
	logger log.ILogger,
	host string,
	port uint16,
	user string,
	password string,
	exchange string,
) IPublishService[domain.AlertCollection] {
	return &RabbitMqPublisher[T]{
		logger:   logger,
		host:     host,
		port:     port,
		user:     user,
		password: password,
		exchange: exchange,

		retryInterval: 10 * time.Second,

		internalMsgChan: make(chan []byte),
		stop:            make(chan bool),
	}
}

// -----------------------------------------------------------------------------------
// Options
// -----------------------------------------------------------------------------------
func WithRabbitMqPublisherRetryInterval[T any](interval time.Duration) func(*RabbitMqPublisher[T]) {
	return func(rmp *RabbitMqPublisher[T]) {
		rmp.retryInterval = interval
	}
}
