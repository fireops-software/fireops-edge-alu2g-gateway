package services

import (
	"bytes"
	"fmt"
	"net"
	"time"

	"github.com/fireops-software/fireops-edge-alu2g-gateway/domain"
	"github.com/uoul/go-common/async"
	"github.com/uoul/go-common/health"
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

	tcpTimeout             time.Duration
	tcpBufferSize          int
	stop                   chan bool
	subscriptions          map[async.Stream[domain.AlertCollection]]bool
	lastSuccessfullRequest time.Time
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
	// Run
LP1:
	for {
		select {
		case <-a.stop:
			break LP1
		default:
			a.pollAlu2g()
			time.Sleep(a.interval)
		}
	}
}

// -----------------------------------------------------------------------------------
// Private
// -----------------------------------------------------------------------------------
func (a *Alu2gClient) getDataFromAlu2g() ([]byte, error) {
	// Connect to Alu2g
	addr := fmt.Sprintf("%s:%d", a.host, a.port)
	conn, err := net.DialTimeout("tcp", addr, a.tcpTimeout)
	if err != nil {
		return nil, appError.NewErrTcpConnect("failed to connect to alu2g (%s) - %v", addr, err)
	}
	defer func() {
		conn.Close()
		a.logger.Tracef("closed connection to Alu2g (%s)", addr)
	}()
	a.logger.Tracef("successfully connected to Alu2g (%s)", addr)

	// Set timeout
	err = conn.SetDeadline(time.Now().Add(a.tcpTimeout))
	if err != nil {
		return nil, appError.NewErrTcpConnect("failed to set deadline for connection to Alu2g (%s) - %v", addr, err)
	}

	// Send Request
	_, err = conn.Write([]byte("GET\n"))
	if err != nil {
		return nil, appError.NewErrAlu2g("failed to write request to Alu2g (%s) - %v", addr, err)
	}
	a.logger.Tracef("request for alert data has been sent to Alu2g (%s)", addr)

	// Wait for Response
	data := []byte{}
	for {
		buffer := make([]byte, a.tcpBufferSize)
		n, err := conn.Read(buffer)
		if err != nil {
			return nil, appError.NewErrAlu2g("failed to resolve response from Alu2g (%s) - %v", addr, err)
		}
		data = append(data, buffer[:n]...)
		if bytes.Contains(data, []byte("</pdu>")) {
			// completed response
			break
		}
	}
	a.logger.Tracef("received response from Alu2g (%s): %s (%d bytes)", addr, string(data), len(data))

	// Return result
	return []byte(data), nil
}

func (a *Alu2gClient) notify(msg async.ActionResult[domain.AlertCollection]) {
	for client := range a.subscriptions {
		client <- msg
	}
}

func (a *Alu2gClient) pollAlu2g() {
	// Get data from Alu2g
	data, err := a.getDataFromAlu2g()
	if err != nil {
		a.logger.Error(err.Error())
		a.notify(
			async.NewErrorActionResult[domain.AlertCollection](err),
		)
		return
	}
	// Store successfull request
	a.lastSuccessfullRequest = time.Now()
	// Parse data
	alerts, err := domain.CreateAlertCollection(data)
	if err != nil {
		a.logger.Error(err.Error())
		a.notify(
			async.NewErrorActionResult[domain.AlertCollection](err),
		)
		return
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

		tcpBufferSize:          1024,
		tcpTimeout:             time.Duration(30) * time.Second,
		stop:                   make(chan bool),
		subscriptions:          map[async.Stream[domain.AlertCollection]]bool{},
		lastSuccessfullRequest: time.Now(),
	}
	// Apply options
	for _, o := range opts {
		o(r)
	}
	// Register readyness check
	health.GetHealthMonitor().RegisterReadynessCheck("check alu2g connection", func() error {
		if time.Since(r.lastSuccessfullRequest) > 2*r.interval {
			return appError.NewErrAlu2g("alu2g connection not ready")
		}
		return nil
	})
	// Return Alu2gClient
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
