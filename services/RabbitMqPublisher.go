package services

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/uoul/go-common/log"

	appError "github.com/fireops-software/fireops-edge-alu2g-gateway/error"
	amqp "github.com/rabbitmq/amqp091-go"
)

// -----------------------------------------------------------------------------------
// Type
// -----------------------------------------------------------------------------------
type RabbitMqPublisher struct {
	logger   log.ILogger
	host     string
	port     uint16
	user     string
	password string

	stop            chan bool
	retryInterval   time.Duration
	internalMsgChan chan internalMsg
}

type internalMsg struct {
	exchange string
	data     []byte
}

//-----------------------------------------------------------------------------------
// Public
//-----------------------------------------------------------------------------------

// Close implements IService.
func (r *RabbitMqPublisher) Close() error {
	r.stop <- true
	return nil
}

// Run implements IService.
func (r *RabbitMqPublisher) Run() {
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
func (r *RabbitMqPublisher) Publish(exchange string, item any) error {
	data, err := json.Marshal(item)
	if err != nil {
		return appError.NewErrInvalidData("failed to parse data to json string - %v", err)
	}
	r.internalMsgChan <- internalMsg{
		exchange: exchange,
		data:     data,
	}
	return nil
}

// -----------------------------------------------------------------------------------
// Private
// -----------------------------------------------------------------------------------

func (r *RabbitMqPublisher) execService() error {
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

	// Listen on close event
	closeChan := conn.NotifyClose(make(chan *amqp.Error))

	r.logger.Infof("successfully connected to rabbitmq (%s)", conn.RemoteAddr().String())
	for {
		select {
		case <-r.stop:
			return nil
		case err := <-closeChan:
			return appError.NewErrRabbitMq("rabbitmq connection has been closed - %v", err)
		case msg := <-r.internalMsgChan:
			// Check exchange
			err = ch.ExchangeDeclare(
				msg.exchange, // name
				"fanout",     // type
				true,         // durable
				false,        // auto-deleted
				false,        // internal
				false,        // no-wait
				nil,          // arguments
			)
			if err != nil {
				return appError.NewErrRabbitMq("failed to declare exchange(%s) on rabbitmq (%s) - %v", msg.exchange, conn.RemoteAddr().String(), err)
			}
			err := ch.Publish(
				msg.exchange,
				"",
				false,
				false,
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        msg.data,
				},
			)
			if err != nil {
				return appError.NewErrRabbitMq("failed to publish alerts to rabbitmq (%s) on exchange %s - %v", conn.RemoteAddr().String(), msg.exchange, err)
			}
			r.logger.Tracef("message has been successfully sent to rabbitMq (%s) on exchange %s: %s", conn.RemoteAddr().String(), msg.exchange, string(msg.data))
		}
	}
}

// -----------------------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------------------

func NewRabbitMqPublisher(
	logger log.ILogger,
	host string,
	port uint16,
	user string,
	password string,
) IPublishService {
	return &RabbitMqPublisher{
		logger:   logger,
		host:     host,
		port:     port,
		user:     user,
		password: password,

		retryInterval: 10 * time.Second,

		internalMsgChan: make(chan internalMsg),
		stop:            make(chan bool),
	}
}

// -----------------------------------------------------------------------------------
// Options
// -----------------------------------------------------------------------------------
func WithRabbitMqPublisherRetryInterval(interval time.Duration) func(*RabbitMqPublisher) {
	return func(rmp *RabbitMqPublisher) {
		rmp.retryInterval = interval
	}
}
