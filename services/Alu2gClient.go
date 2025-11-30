package services

import (
	"bytes"
	"context"
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
	ctx      context.Context

	tcpTimeout             time.Duration
	tcpBufferSize          int
	subscriptions          map[async.Stream[[]domain.Event]]bool
	lastSuccessfullRequest time.Time
}

//-----------------------------------------------------------------------------------
// Public
//-----------------------------------------------------------------------------------

// Subscribe implements INotificationService.
func (a *Alu2gClient) Subscribe(chBufferSize uint) async.Stream[[]domain.Event] {
	c := async.NewBufferedStream[[]domain.Event](chBufferSize)
	a.subscriptions[c] = true
	return c
}

// Unsubscribe implements INotificationService.
func (a *Alu2gClient) Unsubscribe(client async.Stream[[]domain.Event]) {
	close(client)
	delete(a.subscriptions, client)
}

// Run implements INotificationService.
func (a *Alu2gClient) Run() {
	// Run
LP1:
	for {
		select {
		case <-a.ctx.Done():
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
	addr := net.JoinHostPort(a.host, fmt.Sprintf("%d", a.port))
	conn, err := net.DialTimeout("tcp", addr, a.tcpTimeout)
	if err != nil {
		return nil, appError.NewErrTcpConnect("failed to connect to alu2g (%s) - %v", addr, err)
	}
	defer func() {
		conn.Close()
		a.logger.Debugf("closed connection to Alu2g (%s)", addr)
	}()
	a.logger.Debugf("successfully connected to Alu2g (%s)", addr)

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
	a.logger.Debugf("request for event data has been sent to Alu2g (%s)", addr)

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
	a.logger.Debugf("received response from Alu2g (%s): %s (%d bytes)", addr, string(data), len(data))

	// Return result
	return []byte(data), nil
}

func (a *Alu2gClient) notify(msg async.ActionResult[[]domain.Event]) {
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
			async.NewErrorActionResult[[]domain.Event](err),
		)
		return
	}
	// Store successfull request
	a.lastSuccessfullRequest = time.Now()
	// Parse data
	events, err := domain.CreateEvents(data)
	if err != nil {
		a.logger.Error(err.Error())
		a.notify(
			async.NewErrorActionResult[[]domain.Event](err),
		)
		return
	}
	// Notify clients
	a.notify(async.ActionResult[[]domain.Event]{
		Result: events,
		Error:  nil,
	})
}

// -----------------------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------------------
func NewAlu2gClient(ctx context.Context, host string, port uint16, logger log.ILogger, interval time.Duration, opts ...func(*Alu2gClient)) INotificationService[[]domain.Event] {
	// Create default Alu2gClient
	r := &Alu2gClient{
		host:     host,
		port:     port,
		interval: interval,
		logger:   logger,
		ctx:      ctx,

		tcpBufferSize:          1024,
		tcpTimeout:             time.Duration(30) * time.Second,
		subscriptions:          map[async.Stream[[]domain.Event]]bool{},
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
