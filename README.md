# go-message-queue

## todo

* gRPC interface
* monitoring stats
* redundancy
* authenticate

### interface

* REST API
* (option) gRPC API

### components

| Component    | Features                                                                                                                                                  |
| ------       | ------                                                                                                                                                    |
| Publisher    | * Message push to Topic                                                                                                                                   |
| Topic        | * Recieve publish Message<br/> * Save Message to datastor<br/> * Transport Message to Subscription                                                        |
| Datastore    | * Save and mutex Message<br/> * Selectable backend storage                                                                                                |
| Subscription | * Recieve Subscriber pull request<br/> * Push Message to Subscriber                                                                                       |
| Subscriber   | * Register some Subscription<br/>* Pull message from Subscription<br/>* Receive push Message from Subscription<br/> * Return ack response to Subscription |

### message flow

#### quickstart

_When do not specify created component(topic, subscription), Default component used._

1. Publish Message
2. Pull Message
3. Subscriber return ack response

#### use specific Topic and Subscription

1. Create Topic
2. Create Subscription (optional: specify push endpoint)
3. Publish Message to specific Topic
4. Push or Pull Message from specific Subscription
5. Subscriber retrun ack response

### API

#### Topic

| Method             | URL                                   | Behavior                                                                                       |
| ------             | ------                                | -----                                                                                          |
| create             | PUT:    `/topic/{name}`               | create topic                                                                                   |
| delete             | DELETE: `/topic/{name}`               | delete topic                                                                                   |
| get                | GET:    `/topic/{name}`               | get topic detail                                                                               |
| list               | GET:    `/topic/`                     | get topic list                                                                                 |
| list subscriptions | GET:    `/topic/{name}/subscriptions` | get toipc depends subscriptions                                                                |
| publish            | POST:   `/topic/{name}/publish`       | create message<br/>save message to backend storage and deliver message to depends subscription |

#### Subscription

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

### Datastore design

#### features

* select in some backend datastore
* in memory, Redis, MySQL support

#### desgin

* datastore behavior like key-value store
* when need redundancy, use Redis or MySQL
* Save component is Topic, Subscription and Message
* Message delete when all dependent subscription sent ack
