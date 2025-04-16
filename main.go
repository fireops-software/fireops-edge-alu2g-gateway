package main

import (
	"time"

	"github.com/fireops-software/fireops-edge-alu2g-gateway/services"
	"github.com/uoul/go-common/config"
	"github.com/uoul/go-common/log"
	"github.com/uoul/go-common/resource"
)

const (
	VERSION          = "{VERSION}"
	SERVICE_NAME     = "fireops-edge-alu2g-gateway"
	SHUTDOWN_TIMEOUT = time.Duration(20) * time.Second
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

	rabbitMqPublisher := services.NewRabbitMqPublisher(
		logger,
		rabbitMqHost,
		rabbitMqPort,
		rabbitMqUser,
		rabbitMqPw,
	)

	// Create AlertManager
	alertManager := services.NewAlertManager(
		logger,
		alu2gClient,
		rabbitMqPublisher,
		cp.StringOrDefault("RABBITMQ_EXCHANGE_ACTIVE", "ActiveAlerts"),
		cp.StringOrDefault("RABBITMQ_EXCHANGE_NEW", "NewAlerts"),
	)

	// Create HealthReporter
	healthReporter := services.NewHealthReporter(
		logger,
		rabbitMqPublisher,
		cp.StringOrDefault("RABBITMQ_EXCHANGE_HEALTH", "Health"),
		SERVICE_NAME,
	)

	// Run services
	go alu2gClient.Run()
	rm.Register(alu2gClient)

	go rabbitMqPublisher.Run()
	rm.Register(rabbitMqPublisher)

	go alertManager.Run()
	rm.Register(alertManager)

	go healthReporter.Run()
	rm.Register(healthReporter)

	// Wait until resources has been closed
	rm.Wait()
}
