package services

import (
	"fmt"
	"io"
	"net"
	"time"

	"github.com/fireops-software/fireops-edge-alu2g-gateway/domain"
	"github.com/uoul/go-common/async"
	"github.com/uoul/go-common/log"

	appError "github.com/fireops-software/fireops-edge-alu2g-gateway/error"
)

// -----------------------------------------------------------------------------------
// Type
// -----------------------------------------------------------------------------------
type Alu2gClient struct {
	host     string
	port     uint16
	interval time.Duration
	logger   log.ILogger

	tcpTimeout    time.Duration
	tcpBufferSize int
	stop          chan bool
	subscriptions map[async.Stream[domain.AlertCollection]]bool
}

//-----------------------------------------------------------------------------------
// Public
//-----------------------------------------------------------------------------------

// Close implements INotificationService.
func (a *Alu2gClient) Close() error {
	a.stop <- true
	return nil
}

// Subscribe implements INotificationService.
func (a *Alu2gClient) Subscribe(chBufferSize uint) async.Stream[domain.AlertCollection] {
	c := async.NewBufferedStream[domain.AlertCollection](chBufferSize)
	a.subscriptions[c] = true
	return c
}

// Unsubscribe implements INotificationService.
func (a *Alu2gClient) Unsubscribe(client async.Stream[domain.AlertCollection]) {
	close(client)
	delete(a.subscriptions, client)
}

// Run implements INotificationService.
func (a *Alu2gClient) Run() {
LP1:
	for {
		select {
		case <-a.stop:
			break LP1
		default:
			a.pollAlu2g()
		}
	}
}

// -----------------------------------------------------------------------------------
// Private
// -----------------------------------------------------------------------------------
func (a *Alu2gClient) getDataFromAlu2g() ([]byte, error) {
	// Resolve tcp address
	tcpAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", a.host, a.port))
	if err != nil {
		return nil, appError.NewErrTcpConnect("failed to resolve tcp address for host: %s and port: %d - %v", a.host, a.port, err)
	}
	// Connect to Alu2g
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return nil, appError.NewErrTcpConnect("failed to connect to alu2g (%s) - %v", tcpAddr.String(), err)
	}
	defer func() {
		conn.Close()
		a.logger.Tracef("closed connection to Alu2g (%s)", tcpAddr.String())
	}()
	a.logger.Tracef("successfully connected to Alu2g (%s)", tcpAddr.String())

	// Set timeout
	conn.SetDeadline(time.Now().Add(a.tcpTimeout))

	// Send Request
	_, err = conn.Write([]byte("GET"))
	if err != nil {
		return nil, appError.NewErrAlu2g("failed to write request to Alu2g (%s) - %v", tcpAddr.String(), err)
	}

	// Wait for Response
	data := []byte{}
	for {
		buffer := make([]byte, a.tcpBufferSize)
		n, err := conn.Read(buffer)
		if err != nil {
			// Connection closed
			if err != io.EOF {
				break
			}
			// Return other errors
			return nil, appError.NewErrAlu2g("failed to resolve response from Alu2g (%s) - %v", tcpAddr.String(), err)
		}
		data = append(data, buffer[:n]...)
		if n < a.tcpBufferSize {
			// completed response
			break
		}
	}

	// Return result
	return data, nil
}

func (a *Alu2gClient) notify(msg async.ActionResult[domain.AlertCollection]) {
	for client := range a.subscriptions {
		client <- msg
	}
}

func (a *Alu2gClient) pollAlu2g() {
	defer time.Sleep(a.interval)
	// Get data from Alu2g
	data, err := a.getDataFromAlu2g()
	if err != nil {
		a.logger.Error(err.Error())
		a.notify(
			async.NewErrorActionResult[domain.AlertCollection](err),
		)
	}
	// Parse data
	alerts, err := domain.CreateAlertCollection(data)
	if err != nil {
		a.logger.Error(err.Error())
		a.notify(
			async.NewErrorActionResult[domain.AlertCollection](err),
		)
	}
	// Notify clients
	a.notify(async.ActionResult[domain.AlertCollection]{
		Result: *alerts,
		Error:  nil,
	})
}

// -----------------------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------------------
func NewAlu2gClient(host string, port uint16, logger log.ILogger, interval time.Duration, opts ...func(*Alu2gClient)) INotificationService[domain.AlertCollection] {
	// Create default Alu2gClient
	r := &Alu2gClient{
		host:     host,
		port:     port,
		interval: interval,
		logger:   logger,

		tcpBufferSize: 1024,
		tcpTimeout:    time.Duration(5) * time.Second,
		stop:          make(chan bool),
		subscriptions: map[async.Stream[domain.AlertCollection]]bool{},
	}
	// Apply options
	for _, o := range opts {
		o(r)
	}
	return r
}

// -----------------------------------------------------------------------------------
// Options
// -----------------------------------------------------------------------------------
func WithAlu2gTcpTimeOut(timeout time.Duration) func(*Alu2gClient) {
	return func(ac *Alu2gClient) {
		ac.tcpTimeout = timeout
	}
}

func WithAlu2gTcpBufferSize(size int) func(*Alu2gClient) {
	return func(ac *Alu2gClient) {
		ac.tcpBufferSize = size
	}
}
