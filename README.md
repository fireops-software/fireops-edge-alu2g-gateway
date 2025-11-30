# FireOps Edge Alu2g Gateway
This repository contains the service, that is used as gateway component between the firedepartment device Alu2g and RabbitMq. 

It notifies about two different events:
* Active events has changed  (e.g. a new one has been received from Alu2g, or an already known has been finished)
* New events (only delta between already known and received from Alu2g)

## Configuration
All configuration is done via environmental variables because the intended form of running the project is in a Docker container.

| Variable | Default | Description |
|----------|---------|-------------|
| RABBITMQ_HOST | "" | RabbitMQ host (e.g. 192.168.x.x or Hostname) |
| RABBITMQ_PORT | 5672 | RabbitMQ port |
| RABBITMQ_USER | "" | RabbitMQ user |
| RABBITMQ_PW | "" | RabbitMQ password |
| RABBITMQ_EVENTS_EXCHANGE | fireops-edge-events | RabbitMQ Exchange, where events will be published |
| RABBITMQ_EVENTS_ROUTING_KEY | alu2g | RabbitMQ routing key for events from alu2g |
| RABBITMQ_HEALTH_EXCHANGE | fireops-edge-health | RabbitMQ exchange for health messages |
| RABBITMQ_HEALTH_ROUTING_KEY | "" | RabbitMQ routing key for health messages |
||||
| ALU2G_HOST | 192.168.130.100 | IP or Hostname of Alu2g device |
| ALU2G_PORT | 47000 | TCP-Port of Alu2g xml interface |
| ALU2G_POLL_INTERVAL | 15 | Polling interval on xml interface in seconds |
||||
| LOG_LEVEL | INFO | TRACE, DEBUG, INFO, WARNING, ERROR, FATAL, OFF |

## Dataformat
The following json is an example of the dataformat, that the service will publish on RabbitMQ
```json
[
  {
    "eid": null,
    "num_1": "BWSt40007",
    "location": "Hauptplatz 5, 9500 Villach",
    "location_info": null,
    "location_involved": null,
    "category": "Feuer",
    "typ_eng": "BRAND PKW",
    "sub_eng": null,
    "alarm_lev": 2,
    "event_alarmtext": "Fahrzeugbrand auf Parkplatz",
    "create_time": "2025-06-05 15:20:11",
    "firstdispatch_time": null,
    "latitude": null,
    "longitude": null,
    "caller_name": "Andreas Maier",
    "caller_number": "+55 676 4445566",
    "destinations": [
      {
        "id": 70001,
        "name": "FF-Villach-Stadt"
      },
      {
        "id": 70002,
        "name": "FF-Villach-Land"
      }
    ],
    "user_responses": {
      "accepted": [],
      "declined": []
    }
  }
]
```
