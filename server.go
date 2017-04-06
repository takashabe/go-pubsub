package queue

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

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

// Json is wrapped Respond when success response
func Json(w http.ResponseWriter, code int, src interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	Respond(w, code, src)
}

// TopicServer is topic frontend server
type TopicServer struct{}

// Create is create topic
func (s *TopicServer) Create(w http.ResponseWriter, r *http.Request, id string) {
	t, err := models.NewTopic(id)
	if err != nil {
		Error(w, http.StatusNotFound, err, "failed to create topic")
		return
	}
	Json(w, http.StatusCreated, t)
}

// Get is get already exist topic
func (s *TopicServer) Get(w http.ResponseWriter, r *http.Request, id string) {
	t, err := models.GetTopic(id)
	if err != nil {
		Error(w, http.StatusNotFound, err, "not found topic")
		return
	}
	Json(w, http.StatusOK, t)
}

// Delete is delete topic
func (s *TopicServer) Delete(w http.ResponseWriter, r *http.Request, id string) {
	t, err := models.GetTopic(id)
	if err != nil {
		Error(w, http.StatusNotFound, err, "topic already not exist")
		return
	}
	if err := t.Delete(); err != nil {
		Error(w, http.StatusInternalServerError, err, "failed to delete topic")
		return
	}
	Json(w, http.StatusNoContent, t)
}

func routes() *router.Router {
	r := router.NewRouter()
	s := TopicServer{}

	topicRoot := "/topic"
	r.Get(topicRoot+"/get/:id", s.Get)
	r.Put(topicRoot+"/create/:id", s.Create)
	r.Delete(topicRoot+"/delete/:id", s.Delete)

	subscriptionRoot := "/subscription"
	r.Get(subscriptionRoot+"/get", nil)
	return r
}

func main() {
	log.Println("starting server...")
	log.Fatal(http.ListenAndServe("localhost:8080", routes()))
}
