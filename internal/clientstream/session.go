package clientstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/k-p2plab/wrapping-bot/internal/protocol"
)

const maxOutputEventBytes = 8 * 1024

type Config struct {
	Endpoint      string
	Token         string
	QueueSize     int
	FinishTimeout time.Duration
}

type Result struct {
	Response protocol.StreamResponse
	Err      error
}

type Session struct {
	events chan protocol.Event
	done   chan Result
	cancel context.CancelFunc

	sendMu  sync.Mutex
	closed  bool
	dropped atomic.Int64
}

func New(parent context.Context, cfg Config) (*Session, error) {
	if strings.TrimSpace(cfg.Endpoint) == "" {
		return nil, errors.New("relay endpoint is required")
	}
	if strings.TrimSpace(cfg.Token) == "" {
		return nil, errors.New("relay token is required")
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 1024
	}
	if cfg.FinishTimeout <= 0 {
		cfg.FinishTimeout = 15 * time.Second
	}

	ctx, cancel := context.WithCancel(parent)
	s := &Session{
		events: make(chan protocol.Event, cfg.QueueSize),
		done:   make(chan Result, 1),
		cancel: cancel,
	}
	go s.upload(ctx, cfg)
	return s, nil
}

func (s *Session) SendStart(event protocol.Event) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if s.closed {
		return errors.New("relay session is closed")
	}
	s.events <- event
	return nil
}

func (s *Session) Writer(runID, stream string) io.Writer {
	return &eventWriter{session: s, runID: runID, stream: stream}
}

func (s *Session) Close(exitEvent protocol.Event, timeout time.Duration) Result {
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	started := time.Now()
	finalSendTimeout := timeout / 3
	if finalSendTimeout > 5*time.Second {
		finalSendTimeout = 5 * time.Second
	}
	if finalSendTimeout < 250*time.Millisecond {
		finalSendTimeout = 250 * time.Millisecond
	}

	s.sendMu.Lock()
	if !s.closed {
		if dropped := s.dropped.Load(); dropped > 0 {
			notice := protocol.Event{
				Version:   protocol.Version,
				Type:      protocol.EventOutput,
				RunID:     exitEvent.RunID,
				Timestamp: time.Now().UTC(),
				Stream:    protocol.StreamSystem,
				Data:      fmt.Sprintf("[wrapping-bot client queue overflow: %d bytes omitted]\n", dropped),
			}
			select {
			case s.events <- notice:
			default:
			}
		}
		timer := time.NewTimer(finalSendTimeout)
		select {
		case s.events <- exitEvent:
		case <-timer.C:
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		close(s.events)
		s.closed = true
	}
	s.sendMu.Unlock()

	remaining := timeout - time.Since(started)
	if remaining <= 0 {
		s.cancel()
		return Result{Err: fmt.Errorf("relay did not finish within %s", timeout)}
	}
	select {
	case result := <-s.done:
		return result
	case <-time.After(remaining):
		s.cancel()
		return Result{Err: fmt.Errorf("relay did not finish within %s", timeout)}
	}
}

func (s *Session) upload(ctx context.Context, cfg Config) {
	pipeReader, pipeWriter := io.Pipe()
	url := strings.TrimRight(cfg.Endpoint, "/") + "/v1/runs/stream"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, pipeReader)
	if err != nil {
		s.done <- Result{Err: err}
		return
	}
	req.Header.Set("Authorization", "Bearer "+cfg.Token)
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("User-Agent", "wrapping-bot/1")

	responseCh := make(chan Result, 1)
	go func() {
		client := &http.Client{
			Transport: &http.Transport{
				Proxy:                 http.ProxyFromEnvironment,
				DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
				ForceAttemptHTTP2:     true,
				MaxIdleConns:          20,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   5 * time.Second,
				ExpectContinueTimeout: time.Second,
			},
		}
		resp, requestErr := client.Do(req)
		if requestErr != nil {
			responseCh <- Result{Err: requestErr}
			return
		}
		defer resp.Body.Close()

		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if readErr != nil {
			responseCh <- Result{Err: readErr}
			return
		}
		var response protocol.StreamResponse
		if len(body) > 0 {
			if err := json.Unmarshal(body, &response); err != nil {
				responseCh <- Result{Err: fmt.Errorf("decode relay response: %w", err)}
				return
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			message := response.Error
			if message == "" {
				message = strings.TrimSpace(string(body))
			}
			responseCh <- Result{Response: response, Err: fmt.Errorf("relay returned %s: %s", resp.Status, message)}
			return
		}
		responseCh <- Result{Response: response}
	}()

	encoder := json.NewEncoder(pipeWriter)
	var encodeErr error
	for event := range s.events {
		if encodeErr != nil {
			continue
		}
		if err := encoder.Encode(event); err != nil {
			encodeErr = err
		}
	}
	if encodeErr != nil {
		_ = pipeWriter.CloseWithError(encodeErr)
	} else {
		_ = pipeWriter.Close()
	}

	result := <-responseCh
	if encodeErr != nil && result.Err == nil {
		result.Err = encodeErr
	}
	s.done <- result
}

type eventWriter struct {
	session *Session
	runID   string
	stream  string
}

func (w *eventWriter) Write(p []byte) (int, error) {
	originalLen := len(p)
	for len(p) > 0 {
		n := len(p)
		if n > maxOutputEventBytes {
			n = maxOutputEventBytes
		}
		chunk := string(append([]byte(nil), p[:n]...))
		event := protocol.Event{
			Version:   protocol.Version,
			Type:      protocol.EventOutput,
			RunID:     w.runID,
			Timestamp: time.Now().UTC(),
			Stream:    w.stream,
			Data:      chunk,
		}

		w.session.sendMu.Lock()
		if w.session.closed {
			w.session.sendMu.Unlock()
			return originalLen, nil
		}
		select {
		case w.session.events <- event:
		default:
			w.session.dropped.Add(int64(n))
		}
		w.session.sendMu.Unlock()
		p = p[n:]
	}
	return originalLen, nil
}
