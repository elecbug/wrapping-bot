package clientstream

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/k-p2plab/wrapping-bot/internal/protocol"
)

func TestSessionStreamsEvents(t *testing.T) {
	var mu sync.Mutex
	var events []protocol.Event
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		decoder := json.NewDecoder(r.Body)
		for {
			var event protocol.Event
			if err := decoder.Decode(&event); err != nil {
				break
			}
			mu.Lock()
			events = append(events, event)
			mu.Unlock()
		}
		_ = json.NewEncoder(w).Encode(protocol.StreamResponse{RunID: "run-1", Accepted: true})
	}))
	defer server.Close()

	session, err := New(context.Background(), Config{
		Endpoint:      server.URL,
		Token:         "secret",
		QueueSize:     8,
		FinishTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	start := protocol.Event{Version: protocol.Version, Type: protocol.EventStart, RunID: "run-1", Timestamp: time.Now(), ChannelID: "123456789012345678", Command: []string{"echo", "hello"}}
	if err := session.SendStart(start); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Writer("run-1", protocol.StreamStdout).Write([]byte("hello\n")); err != nil {
		t.Fatal(err)
	}
	exitCode := 0
	result := session.Close(protocol.Event{Version: protocol.Version, Type: protocol.EventExit, RunID: "run-1", Timestamp: time.Now(), ExitCode: &exitCode}, 2*time.Second)
	if result.Err != nil {
		t.Fatal(result.Err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) != 3 {
		t.Fatalf("events=%d: %+v", len(events), events)
	}
	if events[0].Type != protocol.EventStart || events[1].Type != protocol.EventOutput || events[2].Type != protocol.EventExit {
		t.Fatalf("unexpected order: %+v", events)
	}
}
