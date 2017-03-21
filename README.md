# go-message-queue

## todo

* HTTP interface
* MessgaePack interface
* set queue name
* set queue expire
* set timeout and retry
* monitoring stats
* redundancy

## desgin

#### ref. google cloud pub/sub

https://cloud.google.com/pubsub/docs/

_But go-message-queue is message queue, not pub/sub model._

#### interface

* REST API
  * ref [docs](https://cloud.google.com/pubsub/docs/reference/rest/)
* (option) MessgaePack API
* (option) gRPC API
  * this cloud pub/sub default?

#### components

| Component | Features |
| ------ | ------ |
| Publisher | * Message push to Topic |
| Topic | * Recieve publish Message<br/> * Save Message to datastor<br/> * Transport Message to Subscription |
| Datastore | * Save and mutex Message<br/> * Selectable backend storage |
| Subscription | * Recieve Subscriber pull request<br/> * Push Message to Subscriber |
| Subscriber | * Register some Subscription<br/>* Pull message from Subscription<br/>* Receive push Message from Subscription<br/> * Return ack response to Subscription|

#### message flow

1. Create Topic
2. Publish Message
3. Message save to Datastore
4. Transport Message to Subscription
5. Push or Pull Message to Subscriber
6. Subscriber ack response
7. When Subscription received ack response, delete message

#### options

# TODO: refs by https://cloud.google.com/pubsub/docs/publisher#pubsub-publish-message-protocol
#       write options Topic, Subscription...
