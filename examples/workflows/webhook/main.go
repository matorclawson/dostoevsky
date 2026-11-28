// Durable webhook intake: an event is acknowledged only after it is committed,
// and a redelivery of the same event does not start it again.
//
// Mirrors the webhook callback pattern in github.com/restatedev/examples (MIT),
// go/patterns-use-cases/src/webhookcallbacks.
//
// Trust: an event is accepted only with an HMAC-SHA256 of the exact body under a
// secret shared with the sender, in lowercase hex, in the X-Signature header. This program
// serves plain HTTP, so use TLS in front of it on any untrusted network. A
// captured request can be sent again; the event ID keeps it from being applied
// twice.
package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/rodriveracom/dostoevsky/workflow"
)

const name = "payment-event"
const queue = "payment-events"
const bodyLimit = 64 << 10

// inputLimit bounds the re-encoded event, which can be several times the size of
// the body because of JSON escaping. It stays well inside the store's record
// limit, so an event is never refused there and retried for ever.
const inputLimit = 128 << 10

// labelLimit is the longest run ID the store accepts.
const labelLimit = 256

type event struct {
	ID      string `json:"id"`
	Type    string `json:"type"`
	Invoice string `json:"invoice"`
}

func register(s *workflow.Store) error {
	return s.Register(name, 1, []workflow.Step{{Kind: "activity", Name: "apply", Attempts: 20}})
}

// sign returns the signature a sender puts on body.
func sign(secret, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// intake stores the fields it uses in one spelling, so a provider that resends
// the event with different spacing or extra fields is still recognized. A 4xx
// answer means that sending the same request again cannot succeed.
func intake(s *workflow.Store, secret []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, bodyLimit))
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				http.Error(w, "event too large", http.StatusRequestEntityTooLarge)
			} else {
				http.Error(w, "unreadable request", http.StatusBadRequest)
			}
			return
		}
		if !hmac.Equal([]byte(r.Header.Get("X-Signature")), []byte(sign(secret, body))) {
			http.Error(w, "bad signature", http.StatusUnauthorized)
			return
		}
		// Unmarshal refuses anything after the first JSON value.
		var e event
		if err = json.Unmarshal(body, &e); err != nil || e.ID == "" || e.Invoice == "" || len(name+"/"+e.ID) > labelLimit {
			http.Error(w, "bad event", http.StatusBadRequest)
			return
		}
		input, err := json.Marshal(e)
		if err == nil && len(input) > inputLimit {
			http.Error(w, "event too large", http.StatusRequestEntityTooLarge)
			return
		}
		if err == nil {
			_, err = s.StartOnce(name+"/"+e.ID, name, 1, queue, input, nil)
		}
		switch {
		case errors.Is(err, workflow.ErrStartConflict):
			http.Error(w, "event ID reused with other content", http.StatusConflict)
		case err != nil:
			// Not acknowledged: the provider redelivers.
			http.Error(w, "not stored", http.StatusServiceUnavailable)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}
}

// handle applies an event. apply must be idempotent for its key.
func handle(task *workflow.Task, apply func(key string, e event) error) ([]byte, string) {
	var e event
	if err := json.Unmarshal(task.Input, &e); err != nil {
		return nil, err.Error()
	}
	if err := apply(task.Run, e); err != nil {
		return nil, err.Error()
	}
	return task.Input, ""
}

func main() {
	secret := []byte(os.Getenv("WEBHOOK_SECRET"))
	if len(os.Args) != 3 || len(secret) < 16 {
		log.Fatal("usage: WEBHOOK_SECRET=at-least-16-bytes webhook DATA-DIRECTORY LISTEN-ADDRESS (for example 127.0.0.1:8080)")
	}
	s, err := workflow.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()
	if err = register(s); err != nil {
		log.Fatal(err)
	}
	go func() {
		server := &http.Server{Addr: os.Args[2], Handler: intake(s, secret), ReadTimeout: 10 * time.Second}
		log.Fatal(server.ListenAndServe())
	}()
	for {
		task, err := s.Poll(queue, time.Minute)
		if err != nil {
			log.Fatal(err)
		}
		if task == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		out, failure := handle(task, func(key string, e event) error {
			log.Println(key, e.Type, e.Invoice)
			return nil
		})
		if err = s.Finish(task.Run, task.Token, out, failure); err != nil {
			log.Print(err)
		}
	}
}
