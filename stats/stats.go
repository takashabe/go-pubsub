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

// buf is output buffer from the MetricsWriter
var buf = &buffer{}

// buffer is wrapped bytes.Buffer
type buffer struct {
	bytes.Buffer
}

// ReadOnce call Reset() after the return Bytes()
func (b *buffer) ReadOnce() []byte {
	defer b.Reset()
	return b.Bytes()
}

// TODO: improve holding methods for the metrics keys
func getSummaryKeys() []string {
	return []string{"topic.topic_num", "subscription.subscription_num", "topic.message_count", "subscription.message_count"}
}
func getTopicSummaryKeys() []string {
	return []string{"topic.topic_num", "topic.message_count"}
}
func getSubscriptionSummaryKeys() []string {
	return []string{"subscription.subscription_num", "subscription.message_count"}
}
func getTopicDetailKeys(id string) []string {
	adapter := GetTopicAdapter()
	return []string{
		adapter.assembleMetricsKey(id, "created_at"),
		adapter.assembleMetricsKey(id, "message_count"),
	}
}
func getSubscriptionDetailKeys(id string) []string {
	adapter := GetSubscriptionAdapter()
	return []string{
		adapter.assembleMetricsKey(id, "created_at"),
		adapter.assembleMetricsKey(id, "message_count"),
	}
}

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
func prepareMetrics() {
	// NOTE: premise that following metrics keys is Counter type
	for _, key := range getSummaryKeys() {
		collector.Add(key, 0)
	}
}

// Summary returns summary of the all stats
func Summary() ([]byte, error) {
	err := forwarder.FlushWithKeys(getSummaryKeys()...)
	if err != nil {
		return nil, err
	}
	return buf.ReadOnce(), nil
}

// TopicSummary returns summary of the topic stats
func TopicSummary() ([]byte, error) {
	err := forwarder.FlushWithKeys(getTopicSummaryKeys()...)
	if err != nil {
		return nil, err
	}
	return buf.ReadOnce(), nil
}

// SubscriptionSummary returns summary of the subscription stats
func SubscriptionSummary() ([]byte, error) {
	err := forwarder.FlushWithKeys(getSubscriptionSummaryKeys()...)
	if err != nil {
		return nil, err
	}
	return buf.ReadOnce(), nil
}

// TopicDetail returns detail of the topic stats
func TopicDetail(id string) ([]byte, error) {
	err := forwarder.FlushWithKeys(getTopicDetailKeys(id)...)
	if err != nil {
		return nil, err
	}
	return buf.ReadOnce(), nil
}

// SubscriptionDetail returns detail of the subscription stats
func SubscriptionDetail(id string) ([]byte, error) {
	err := forwarder.FlushWithKeys(getSubscriptionDetailKeys(id)...)
	if err != nil {
		return nil, err
	}
	return buf.ReadOnce(), nil
}

func init() {
	// NOTE: leak buffer size
	collector = collect.NewSimpleCollector()
	f, err := forward.NewSimpleWriter(collector, buf)
	if err != nil {
		log.Fatal(err)
	}
	forwarder = f

	prepareMetrics()
}
