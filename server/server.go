package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/pkg/errors"
	"github.com/takashabe/go-message-queue/datastore"
	"github.com/takashabe/go-message-queue/models"
	"github.com/takashabe/go-router"
)

// PrintDebugf behaves like log.Printf only in the debug env
func PrintDebugf(format string, args ...interface{}) {
	if env := os.Getenv("GO_MESSAGE_QUEUE_DEBUG"); len(env) != 0 {
		log.Printf("[DEBUG] "+format+"\n", args...)
	}
}

// ErrorResponse is Error response template
type ErrorResponse struct {
	Message string `json:"reason"`
	Error   error  `json:"-"`
}

func (e *ErrorResponse) String() string {
	return fmt.Sprintf("reason: %s, error: %v", e.Message, e.Error)
}

// Respond is response write to ResponseWriter
func Respond(w http.ResponseWriter, code int, src interface{}) {
	var body []byte
	var err error

	switch s := src.(type) {
	case string:
		body = []byte(s)
	case *ErrorResponse, ErrorResponse:
		// avoid infinite loop
		if body, err = json.Marshal(src); err != nil {
			w.Header().Set("Content-Type", "application/json; charset=UTF-8")
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("{\"reason\":\"failed to parse json\"}"))
			return
		}
	default:
		if body, err = json.Marshal(src); err != nil {
			Error(w, http.StatusInternalServerError, err, "failed to parse json")
			return
		}
	}
	w.WriteHeader(code)
	w.Write(body)
}

// Error is wrapped Respond when error response
func Error(w http.ResponseWriter, code int, err error, msg string) {
	e := &ErrorResponse{
		Message: msg,
		Error:   err,
	}
	PrintDebugf("%v", e)
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	Respond(w, code, e)
}

// JSON is wrapped Respond when success response
func JSON(w http.ResponseWriter, code int, src interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	Respond(w, code, src)
}

func routes() *router.Router {
	r := router.NewRouter()

	ts := TopicServer{}
	topicRoot := "/topic"
	r.Get(topicRoot+"/", ts.List)
	r.Get(topicRoot+"/:id", ts.Get)
	r.Get(topicRoot+"/:id/subscriptions", ts.ListSubscription)
	r.Put(topicRoot+"/:id", ts.Create)
	r.Post(topicRoot+"/:id/publish", ts.Publish)
	r.Delete(topicRoot+"/:id", ts.Delete)

	ss := SubscriptionServer{}
	subscriptionRoot := "/subscription"
	r.Get(subscriptionRoot+"/", ss.List)
	r.Get(subscriptionRoot+"/:id", ss.Get)
	r.Put(subscriptionRoot+"/:id", ss.Create)
	r.Post(subscriptionRoot+"/:id/pull", ss.Pull)
	r.Post(subscriptionRoot+"/:id/ack", ss.Ack)
	r.Post(subscriptionRoot+"/:id/ack/modify", ss.ModifyAck)
	r.Post(subscriptionRoot+"/:id/push/modify", ss.ModifyPush)
	r.Delete(subscriptionRoot+"/:id", ss.Delete)
	return r
}

// Server is topic and subscription frontend server
type Server struct {
	cfg *Config
}

// NewServer return initialized server
func NewServer(path string) (*Server, error) {
	c, err := LoadConfigFromFile(path)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load config")
	}
	return &Server{
		cfg: c,
	}, nil
}

// InitDatastore prepare datastore initialize
func (s *Server) InitDatastore() error {
	datastore.SetGlobalConfig(s.cfg.Datastore)
	if err := models.InitDatastoreTopic(); err != nil {
		return errors.Wrap(err, "failed to init datastore topic")
	}
	if err := models.InitDatastoreSubscription(); err != nil {
		return errors.Wrap(err, "failed to init datastore subscription")
	}
	if err := models.InitDatastoreMessage(); err != nil {
		return errors.Wrap(err, "failed to init datastore message")
	}
	if err := models.InitDatastoreMessageStatus(); err != nil {
		return errors.Wrap(err, "failed to init datastore message status")
	}
	return nil
}

// Run start server
func (s *Server) Run(port int) error {
	log.Println("starting server...")
	return http.ListenAndServe(fmt.Sprintf(":%d", port), routes())
}
