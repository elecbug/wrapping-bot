package discord

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2plab/wrapping-bot/internal/wsclient"
)

func TestGatewayIdentifiesAndBecomesConnected(t *testing.T) {
	t.Parallel()

	identified := make(chan map[string]any, 1)
	var serverURL string
	var once sync.Once

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/gateway/bot":
			if got := r.Header.Get("Authorization"); got != "Bot test-token" {
				t.Errorf("unexpected authorization header: %q", got)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"url":%q}`, "ws"+strings.TrimPrefix(serverURL, "http")+"/gateway")
		case "/gateway":
			conn, reader, err := wsclient.UpgradeForTest(w, r)
			if err != nil {
				t.Errorf("upgrade: %v", err)
				return
			}
			defer conn.Close()

			hello := []byte(`{"op":10,"d":{"heartbeat_interval":100}}`)
			if _, err := conn.Write(wsclient.ServerFrame(1, hello)); err != nil {
				t.Errorf("send hello: %v", err)
				return
			}

			for {
				opcode, payload, err := wsclient.ReadClientFrame(reader)
				if err != nil {
					if err != io.EOF {
						t.Errorf("read client frame: %v", err)
					}
					return
				}
				if opcode == 8 {
					return
				}
				var envelope map[string]any
				if err := json.Unmarshal(payload, &envelope); err != nil {
					t.Errorf("decode client payload: %v", err)
					return
				}
				op, _ := envelope["op"].(float64)
				if op == 1 {
					_, _ = conn.Write(wsclient.ServerFrame(1, []byte(`{"op":11,"d":null}`)))
					continue
				}
				if op != 2 {
					continue
				}
				once.Do(func() { identified <- envelope })
				ready := []byte(`{"op":0,"t":"READY","s":1,"d":{"session_id":"session-1","resume_gateway_url":"ws://127.0.0.1/unused"}}`)
				_, _ = conn.Write(wsclient.ServerFrame(1, ready))
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	serverURL = server.URL

	gateway, err := NewGateway(GatewayConfig{
		BotToken:   "test-token",
		APIBaseURL: server.URL,
		Status:     "online",
		Activity:   "process logs",
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		gateway.Run(ctx)
		close(done)
	}()

	select {
	case payload := <-identified:
		data, ok := payload["d"].(map[string]any)
		if !ok {
			t.Fatalf("identify data has unexpected type: %#v", payload["d"])
		}
		if got := int(data["intents"].(float64)); got != 0 {
			t.Fatalf("intents = %d, want 0", got)
		}
		presence := data["presence"].(map[string]any)
		if got := presence["status"]; got != "online" {
			t.Fatalf("presence status = %v", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for identify payload")
	}

	deadline := time.Now().Add(2 * time.Second)
	for !gateway.Connected() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !gateway.Connected() {
		t.Fatal("gateway did not become connected after READY")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("gateway did not stop after context cancellation")
	}
}
