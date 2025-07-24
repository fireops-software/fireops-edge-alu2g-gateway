package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fireops-software/fireops-edge-alu2g-gateway/services"
	"github.com/uoul/go-common/config"
	"github.com/uoul/go-common/log"
	"github.com/uoul/go-common/messaging"
)

const (
	VERSION      = "{VERSION}"
	SERVICE_NAME = "fireops-edge-alu2g-gateway"
)

func main() {
	// Create Application Context
	appCtx, appCtxCancel := context.WithCancel(context.Background())

	// Create config provider
	cp := config.NewEnvVarProvider()

	// Create logger
	logger := log.NewConsoleLogger(
		log.StringToLogLevel(cp.StringOrDefault("LOG_LEVEL", ""), log.INFO),
	)

	// Create Alu2g client
	alu2gClient := services.NewAlu2gClient(
		appCtx,
		cp.StringOrDefault("ALU2G_HOST", "192.168.130.100"),
		cp.UInt16OrDefault("ALU2G_PORT", 47000),
		logger,
		time.Duration(cp.IntOrDefault("ALU2G_POLL_INTERVAL", 15))*time.Second,
	)

	// Create RabbitMq Messenger
	rabbitMq := messaging.NewRabbitMqMessenger(
		appCtx,
		logger,
		cp.StringOrDefault("RABBITMQ_HOST", ""),
		cp.UInt16OrDefault("RABBITMQ_PORT", 5672),
		cp.StringOrDefault("RABBITMQ_USER", ""),
		cp.StringOrDefault("RABBITMQ_PW", ""),
	)

	// Create EventManager
	rabbitmqExchange := cp.StringOrDefault("RABBITMQ_EXCHANGE", "fireops-edge-events")
	eventManager := services.NewEventManager(
		appCtx,
		logger,
		alu2gClient,
		rabbitMq,
		messaging.RabbitMqExchange{
			Type:       "topic",
			Exchange:   rabbitmqExchange,
			RoutingKey: cp.StringOrDefault("RABBITMQ_ROUTING_KEY_ACTIVE", "alu2g.active"),
		},
		messaging.RabbitMqExchange{
			Type:       "topic",
			Exchange:   rabbitmqExchange,
			RoutingKey: cp.StringOrDefault("RABBITMQ_ROUTING_KEY_NEW", "alu2g.new"),
		},
	)

	// Create HealthReporter
	healthReporter := services.NewHealthReporter(
		appCtx,
		logger,
		rabbitMq,
		messaging.RabbitMqExchange{
			Type:       "topic",
			Exchange:   cp.StringOrDefault("RABBITMQ_HEALTH_EXCHANGE", "fireops-edge-health"),
			RoutingKey: cp.StringOrDefault("RABBITMQ_HEALTH_ROUTING_KEY", ""),
		},
		SERVICE_NAME,
	)

	// Run services
	go alu2gClient.Run()
	go eventManager.Run()
	go healthReporter.Run()

	// Wait until stop
	osSig := make(chan os.Signal, 1)
	signal.Notify(osSig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	<-osSig
	appCtxCancel()
	logger.Info("Shutting down...")
}
