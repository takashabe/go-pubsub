# go-pubsub

[![GoDoc](https://godoc.org/github.com/takashabe/go-pubsub?status.svg)](https://godoc.org/github.com/takashabe/go-pubsub)
[![CircleCI](https://circleci.com/gh/takashabe/go-pubsub.svg?style=shield)](https://circleci.com/gh/takashabe/go-pubsub)
[![Go Report Card](https://goreportcard.com/badge/github.com/takashabe/go-pubsub)](https://goreportcard.com/report/github.com/takashabe/go-pubsub)

Provide pubsub server and simple stats monitoring, available both by REST API.

You can select the background datastore of pubsub server one out of in  the `in-memory`, `mysql` and `redis`.

If you need pubsub client library, import `client` packages. Currently available client library is `Go` only.

## Installation

```
go get -u github.com/takashabe/go-pubsub
```

## Usage

### Start server

```
make build # need once at first
cmd/pubsub/pubsub
```

Options:

* file: Config file. require anything config file. (default "config/app.yaml")
* port: Running port. require unused port. (default 8080)

#### Config file format

Syntax based on the `YAML`, require `datastore` element and it configration parameters. If empty the parameters of `datastore`, used `in-memory` datastore.

Examples:

```
# MySQL
datastore:
  mysql:
    addr: "localhost:3306"
    user: pubsub
    password: ""

# Redis
datastore:
  redis:
    addr: "localhost:6379"
    db: 0

# In-memory
datasotre:
```

## Components

| Component    | Features                                                                                                                                                  |
| ------       | ------                                                                                                                                                    |
| Publisher    | * Message push to Topic                                                                                                                                   |
| Topic        | * Recieve publish Message<br/> * Save Message to datastor<br/> * Transport Message to Subscription                                                        |
| Datastore    | * Save and mutex Message<br/> * Selectable backend storage                                                                                                |
| Subscription | * Recieve Subscriber pull request<br/> * Push Message to Subscriber                                                                                       |
| Subscriber   | * Register some Subscription<br/>* Pull message from Subscription<br/>* Receive push Message from Subscription<br/> * Return ack response to Subscription |

## Message flow

### Quickstart

_When do not specify created component(topic, subscription), Default component used._

1. Publish Message
2. Pull Message
3. Subscriber return ack response

### Use specific Topic and Subscription

1. Create Topic
2. Create Subscription (optional: specify push endpoint)
3. Publish Message to specific Topic
4. Push or Pull Message from specific Subscription
5. Subscriber retrun ack response

## API

### Topic

| Method             | URL                                   | Behavior                                                                                       |
| ------             | ------                                | -----                                                                                          |
| create             | PUT:    `/topic/{name}`               | create topic                                                                                   |
| delete             | DELETE: `/topic/{name}`               | delete topic                                                                                   |
| get                | GET:    `/topic/{name}`               | get topic detail                                                                               |
| list               | GET:    `/topic/`                     | get topic list                                                                                 |
| list subscriptions | GET:    `/topic/{name}/subscriptions` | get toipc depends subscriptions                                                                |
| publish            | POST:   `/topic/{name}/publish`       | create message<br/>save message to backend storage and deliver message to depends subscription |

### Subscription

| Method             | URL                                        | Behavior                                                                                  |
| ------             | ------                                     | -----                                                                                     |
| ack                | POST:   `/subscription/{name}/ack`         | return ack response<br/>when receive ack from all depended Subscriptions, delete message. |
| create             | PUT:    `/subscription/{name}`             | create subscription                                                                       |
| delete             | DELETE: `/subscription/{name}`             | delete subscription                                                                       |
| get                | GET:    `/subscription/{name}`             | get subscription detail                                                                   |
| pull               | POST:   `/subscription/{name}/pull`        | get message                                                                               |
| modify ack config  | POST:   `/subscription/{name}/ack/modify`  | modify ack timeout                                                                        |
| modify push config | POST:   `/subscription/{name}/push/modify` | modify push config                                                                        |
| list               | GET:    `/subscription/`                   | get subscripction list                                                                    |

### Monitoring

| Method               | URL                               | Behavior                     |
| ------               | ------                            | -----                        |
| summary              | GET: `/stats`                     | pubsub metrics summary       |
| topic summary        | GET: `/stats/topic`               | topic metrics summary        |
| topic detail         | GET: `/stats/topic/{name}`        | topic metrics detail         |
| subscription summary | GET: `/stats/subscription`        | subscription metrics summary |
| subscription detail  | GET: `/stats/subscription/{name}` | subscription metrics detail  |

## TODO

* gRPC interface
* improve stats items
* authenticate
