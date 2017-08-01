package stats

import (
	"bytes"
	"log"
	"time"

	"github.com/takashabe/go-metrics/collect"
	"github.com/takashabe/go-metrics/forward"
)

// Pooling accessor for metrics library
var (
	collector collect.Collector
	forwarder forward.MetricsWriter
)

// TopicAdapter is adapter of operation metrics for Topic
type TopicAdapter struct {
	prefix  string
	collect collect.Collector
	forward forward.MetricsWriter
}

// GetTopicAdapter return prepared TopicAdapter
func GetTopicAdapter() *TopicAdapter {
	return &TopicAdapter{
		prefix:  "topic",
		collect: collector,
		forward: forwarder,
	}
}

func (t *TopicAdapter) assembleMetricsKey(parts ...string) string {
	key := t.prefix
	for _, v := range parts {
		key = key + "." + v
	}
	return key
}

// AddTopic send metrics the topic
func (t *TopicAdapter) AddTopic(topicID string, num int) {
	now := time.Now().Unix()
	t.collect.Add(t.assembleMetricsKey("topic_num"), float64(num))
	t.collect.Gauge(t.assembleMetricsKey(topicID, "created_at"), float64(now))
}

// AddMessage send metrics the added message
func (t *TopicAdapter) AddMessage(topicID string) {
	t.collect.Add(t.assembleMetricsKey("message_count"), 1)
	t.collect.Add(t.assembleMetricsKey(topicID, "message_count"), 1)
}

// SubscriptionAdapter is adapter of operation metrics for Subscription
type SubscriptionAdapter struct {
	prefix  string
	collect collect.Collector
	forward forward.MetricsWriter
}

// GetSubscriptionAdapter return prepared SubscriptionAdapter
func GetSubscriptionAdapter() *SubscriptionAdapter {
	return &SubscriptionAdapter{
		prefix:  "subscription",
		collect: collector,
		forward: forwarder,
	}
}

func (t *SubscriptionAdapter) assembleMetricsKey(parts ...string) string {
	key := t.prefix
	for _, v := range parts {
		key = key + "." + v
	}
	return key
}

// AddSubscription send metrics the topic
func (t *SubscriptionAdapter) AddSubscription(subID string, num int) {
	now := time.Now().Unix()
	t.collect.Add(t.assembleMetricsKey("subscription_num"), float64(num))
	t.collect.Gauge(t.assembleMetricsKey(subID, "created_at"), float64(now))
}

// AddMessage send metrics the added message
func (t *SubscriptionAdapter) AddMessage(subID string) {
	t.collect.Add(t.assembleMetricsKey("message_count"), 1)
	t.collect.Add(t.assembleMetricsKey(subID, "message_count"), 1)
}

func init() {
	// NOTE: buffer size
	var buf bytes.Buffer
	collector = collect.NewSimpleCollector()
	f, err := forward.NewSimpleWriter(collector, &buf)
	if err != nil {
		log.Fatal(err)
	}
	forwarder = f
}
