package main

import (
	"time"

	"github.com/fireops-software/fireops-edge-alu2g-gateway/domain"
	"github.com/fireops-software/fireops-edge-alu2g-gateway/services"
	"github.com/uoul/go-common/config"
	"github.com/uoul/go-common/log"
	"github.com/uoul/go-common/resource"
)

const (
	SHUTDOWN_TIMEOUT = time.Duration(5) * time.Second
)

func main() {
	// Create config provider
	cp := config.NewEnvVarProvider()

	// Create logger
	logger := log.NewConsoleLogger(
		log.StringToLogLevel(cp.StringOrDefault("LOG_LEVEL", ""), log.INFO),
	)

	// Create ResourceManager
	rm := resource.NewResourceManager(SHUTDOWN_TIMEOUT, logger)

	// Create Alu2g client
	alu2gClient := services.NewAlu2gClient(
		cp.StringOrDefault("ALU2G_HOST", "192.168.130.100"),
		cp.UInt16OrDefault("ALU2G_PORT", 47000),
		logger,
		time.Duration(cp.IntOrDefault("ALU2G_POLL_INTERVAL", 10))*time.Second,
	)

	// Create RabbitMq publisher
	rabbitMqHost := cp.StringOrDefault("RABBITMQ_HOST", "")
	rabbitMqPort := cp.UInt16OrDefault("RABBITMQ_PORT", 5672)
	rabbitMqUser := cp.StringOrDefault("RABBITMQ_USER", "")
	rabbitMqPw := cp.StringOrDefault("RABBITMQ_PW", "")

	activeAlertsPublisher := services.NewRabbitMqPublisher[domain.AlertCollection](
		logger,
		rabbitMqHost,
		rabbitMqPort,
		rabbitMqUser,
		rabbitMqPw,
		cp.StringOrDefault("RABBITMQ_EXCHANGE_ACTIVE", "ActiveAlerts"),
	)

	newAlertsPublisher := services.NewRabbitMqPublisher[domain.AlertCollection](
		logger,
		rabbitMqHost,
		rabbitMqPort,
		rabbitMqUser,
		rabbitMqPw,
		cp.StringOrDefault("RABBITMQ_EXCHANGE_NEW", "NewAlerts"),
	)

	// Create AlertManager
	alertManager := services.NewAlertManager(
		logger,
		alu2gClient,
		activeAlertsPublisher,
		newAlertsPublisher,
	)

	// Run services
	go alu2gClient.Run()
	rm.Register(alu2gClient)

	go activeAlertsPublisher.Run()
	rm.Register(activeAlertsPublisher)

	go newAlertsPublisher.Run()
	rm.Register(newAlertsPublisher)

	go alertManager.Run()
	rm.Register(alertManager)

	// Wait until resources has been closed
	rm.Wait()
}
