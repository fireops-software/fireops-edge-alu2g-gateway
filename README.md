# FireOps Edge Alu2g Gateway
This repository contains the service, that is used as gateway component between the firedepartment device Alu2g and RabbitMq. 

It notifies about two different events:
* Active alerts has changed  (e.g. a new one has been received from Alu2g, or an already known has been finished)
* New alerts (only delta between already known and received from Alu2g)

## Configuration
All configuration is done via environmental variables because the intended form of running the project is in a Docker container.

| Variable | Default | Description |
|----------|---------|-------------|
| RABBITMQ_HOST | "" | RabbitMQ host (e.g. 192.168.x.x or Hostname) |
| RABBITMQ_PORT | 5672 | RabbitMQ port |
| RABBITMQ_USER | "" | RabbitMQ user |
| RABBITMQ_PW | "" | RabbitMQ password |
| RABBITMQ_EXCHANGE_ACTIVE | ActiveAlerts | RabbitMQ Exchange, where changes will be published |
| RABBITMQ_EXCHANGE_NEW | NewAlerts | RabbitMQ Exchange, where new alerts will be published |
||||
| ALU2G_HOST | 192.168.130.100 | IP or Hostname of Alu2g device |
| ALU2G_PORT | 47000 | TCP-Port of Alu2g xml interface |
| ALU2G_POLL_INTERVAL | 10 | Polling interval on xml interface in seconds |
||||
| LOG_LEVEL | INFO | Possible TRACE, DEBUG, INFO, WARNING, ERROR, FATAL, OFF |

## Dataformat
The following json is an example of the dataformat, that the service will publish on RabbitMQ
```json
{
    "alerts": {
        "<ALERT_ID_1>": {
            "origin": {
                "tid": 300012,
                "name": "Musterstadt"
            },
            "receiveTad": "2023-02-28 13:21:01",
            "operationName": "BRAND BAUM-, FLUR-, BÖSCHUNG",
            "program": "Feuer",
            "level": 2,
            "contact": {
                "name": "Max Mustermann",
                "phoneNumber": "+43 650 5555555"
            },
            "location": "Musterstraße 42, 4300 Musterhausen",
            "info": "Some info",
            "destinations": {
                "<DESTINATION_ID_1>": "FF-Musterhausen",
                "<DESTINATION_ID_2>": "FF-Musterberg"
            }
        },
        "<ALERT_ID_2>": {
            "origin": {
                "tid": 300012,
                "name": "Musterstadt"
            },
            "receiveTad": "2023-02-28 13:25:01",
            "operationName": "TE TIERRETTUNG",
            "program": "Feuer",
            "level": 1,
            "contact": {
                "name": "Hasso",
                "phoneNumber": "+43 650 4444444"
            },
            "location": "9999 Musterhausen, Musterweg 1",
            "info": "Tiger im Tank",
            "destinations": {
                "40117": "FF-Musterhausen"
            }
        }
    }
}
```
