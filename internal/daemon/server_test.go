package daemon

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2plab/wrapping-bot/internal/protocol"
)

type captureSender struct {
	mu       sync.Mutex
	messages []string
}

func (s *captureSender) Send(_ string, content string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages = append(s.messages, content)
	return nil
}

func (s *captureSender) joined() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Join(s.messages, "\n")
}

func TestStreamRun(t *testing.T) {
	sender := &captureSender{}
	cfg := Config{
		SharedToken:       "relay-secret",
		Channels:          map[string]string{"default": "123"},
		FlushInterval:     time.Millisecond,
		MaxLogChunkBytes:  1600,
		QueueSize:         16,
		MaxConcurrentRuns: 2,
		MaxStreamBytes:    1 << 20,
		StripANSI:         true,
	}
	server := NewServer(cfg, sender, slog.New(slog.NewTextHandler(io.Discard, nil)))

	exitCode := 0
	events := []protocol.Event{
		{
			Version:          protocol.Version,
			Type:             protocol.EventStart,
			RunID:            "0123456789abcdef",
			Timestamp:        time.Now().UTC(),
			Target:           "default",
			Name:             "test run",
			Command:          []string{"./exp", "run"},
			Hostname:         "test-host",
			WorkingDirectory: "/tmp/exp",
			Environment:      map[string]string{"TOPOLOGY": "ER"},
		},
		{
			Version:   protocol.Version,
			Type:      protocol.EventOutput,
			RunID:     "0123456789abcdef",
			Timestamp: time.Now().UTC(),
			Stream:    protocol.StreamStdout,
			Data:      "hello from experiment\n",
		},
		{
			Version:   protocol.Version,
			Type:      protocol.EventExit,
			RunID:     "0123456789abcdef",
			Timestamp: time.Now().UTC().Add(time.Second),
			ExitCode:  &exitCode,
		},
	}
	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	for _, event := range events {
		if err := encoder.Encode(event); err != nil {
			t.Fatal(err)
		}
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/runs/stream", &body)
	req.Header.Set("Authorization", "Bearer relay-secret")
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var response protocol.StreamResponse
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if !response.Accepted {
		t.Fatalf("response not accepted: %+v", response)
	}
	joined := sender.joined()
	for _, expected := range []string{"Run started", "hello from experiment", "Run completed", "TOPOLOGY=ER"} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("missing %q in messages:\n%s", expected, joined)
		}
	}
}

func TestStreamUnauthorized(t *testing.T) {
	cfg := Config{
		SharedToken:       "relay-secret",
		Channels:          map[string]string{"default": "123"},
		FlushInterval:     time.Millisecond,
		MaxLogChunkBytes:  1600,
		QueueSize:         1,
		MaxConcurrentRuns: 1,
		MaxStreamBytes:    1024,
	}
	server := NewServer(cfg, &captureSender{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/runs/stream", strings.NewReader("{}\n")))
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("got %d", recorder.Code)
	}
}
