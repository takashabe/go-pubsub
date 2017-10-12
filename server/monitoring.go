package server

import (
	"net/http"

	"github.com/takashabe/go-pubsub/stats"
)

// Monitoring is monitoring frontend server
type Monitoring struct{}

// Summary returns summary from all stats
func (m *Monitoring) Summary(w http.ResponseWriter, r *http.Request) {
	b, err := stats.Summary()
	if err != nil {
		Error(w, http.StatusNotFound, err, "failed to get metrics")
		return
	}
	JSON(w, http.StatusOK, b)
}

// TopicSummary returns summary from topic stats
func (m *Monitoring) TopicSummary(w http.ResponseWriter, r *http.Request) {
	b, err := stats.TopicSummary()
	if err != nil {
		Error(w, http.StatusNotFound, err, "failed to get metrics")
		return
	}
	JSON(w, http.StatusOK, b)
}

// SubscriptionSummary returns summary from subscription stats
func (m *Monitoring) SubscriptionSummary(w http.ResponseWriter, r *http.Request) {
	b, err := stats.SubscriptionSummary()
	if err != nil {
		Error(w, http.StatusNotFound, err, "failed to get metrics")
		return
	}
	JSON(w, http.StatusOK, b)
}

// TopicDetail returns detail from topic stats
func (m *Monitoring) TopicDetail(w http.ResponseWriter, r *http.Request, id string) {
	b, err := stats.TopicDetail(id)
	if err != nil {
		Error(w, http.StatusNotFound, err, "failed to get metrics")
		return
	}
	JSON(w, http.StatusOK, b)
}

// SubscriptionDetail returns detail from subscription stats
func (m *Monitoring) SubscriptionDetail(w http.ResponseWriter, r *http.Request, id string) {
	b, err := stats.SubscriptionDetail(id)
	if err != nil {
		Error(w, http.StatusNotFound, err, "failed to get metrics")
		return
	}
	JSON(w, http.StatusOK, b)
}
