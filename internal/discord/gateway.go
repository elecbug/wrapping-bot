package discord

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/k-p2plab/wrapping-bot/internal/wsclient"
)

const defaultGatewayVersion = "10"

type GatewayConfig struct {
	BotToken   string
	APIBaseURL string
	GatewayURL string
	Status     string
	Activity   string
}

type Gateway struct {
	botToken   string
	apiBaseURL string
	gatewayURL string
	status     string
	activity   string
	http       *http.Client
	logger     *slog.Logger
	connected  atomic.Bool

	stateMu          sync.Mutex
	sessionID        string
	resumeGatewayURL string
	sequence         int64
}

type gatewayEnvelope struct {
	Op int             `json:"op"`
	D  json.RawMessage `json:"d"`
	S  *int64          `json:"s,omitempty"`
	T  string          `json:"t,omitempty"`
}

type gatewayBotResponse struct {
	URL string `json:"url"`
}

type gatewayHello struct {
	HeartbeatInterval float64 `json:"heartbeat_interval"`
}

type gatewayReady struct {
	SessionID        string `json:"session_id"`
	ResumeGatewayURL string `json:"resume_gateway_url"`
}

type gatewayIdentify struct {
	Token      string                    `json:"token"`
	Intents    int                       `json:"intents"`
	Properties gatewayIdentifyProperties `json:"properties"`
	Presence   gatewayPresence           `json:"presence"`
}

type gatewayIdentifyProperties struct {
	OS      string `json:"os"`
	Browser string `json:"browser"`
	Device  string `json:"device"`
}

type gatewayPresence struct {
	Since      *int64            `json:"since"`
	Activities []gatewayActivity `json:"activities"`
	Status     string            `json:"status"`
	AFK        bool              `json:"afk"`
}

type gatewayActivity struct {
	Name string `json:"name"`
	Type int    `json:"type"`
}

type gatewayResume struct {
	Token     string `json:"token"`
	SessionID string `json:"session_id"`
	Sequence  int64  `json:"seq"`
}

func NewGateway(cfg GatewayConfig, logger *slog.Logger) (*Gateway, error) {
	botToken := strings.TrimSpace(cfg.BotToken)
	if botToken == "" {
		return nil, errors.New("Discord bot token is required")
	}
	apiBaseURL := strings.TrimRight(strings.TrimSpace(cfg.APIBaseURL), "/")
	if apiBaseURL == "" {
		apiBaseURL = defaultAPIBaseURL
	}
	status := strings.ToLower(strings.TrimSpace(cfg.Status))
	if status == "" {
		status = "online"
	}
	switch status {
	case "online", "idle", "dnd", "invisible":
	default:
		return nil, fmt.Errorf("unsupported Discord presence status %q", status)
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Gateway{
		botToken:   botToken,
		apiBaseURL: apiBaseURL,
		gatewayURL: strings.TrimSpace(cfg.GatewayURL),
		status:     status,
		activity:   strings.TrimSpace(cfg.Activity),
		http:       &http.Client{Timeout: 15 * time.Second},
		logger:     logger,
		sequence:   -1,
	}, nil
}

func (g *Gateway) Connected() bool {
	return g.connected.Load()
}

func (g *Gateway) Run(ctx context.Context) {
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			g.connected.Store(false)
			return
		}
		err := g.connectAndServe(ctx)
		g.connected.Store(false)
		if ctx.Err() != nil {
			return
		}
		g.logger.Warn("Discord Gateway disconnected; reconnecting", "error", err, "retry_after", backoff)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if backoff < 30*time.Second {
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
}

func (g *Gateway) connectAndServe(ctx context.Context) error {
	endpoint, resuming, err := g.connectionURL(ctx)
	if err != nil {
		return err
	}
	conn, err := wsclient.DialContext(ctx, endpoint)
	if err != nil {
		return fmt.Errorf("connect to Discord Gateway: %w", err)
	}
	defer conn.Close()

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-connCtx.Done()
		_ = conn.Close()
	}()

	var writeMu sync.Mutex
	writeJSON := func(value any) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		return conn.WriteJSON(value)
	}

	_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	var helloEnvelope gatewayEnvelope
	if err := conn.ReadJSON(&helloEnvelope); err != nil {
		return fmt.Errorf("read Discord Gateway hello: %w", err)
	}
	if helloEnvelope.Op != 10 {
		return fmt.Errorf("expected Discord Gateway hello opcode 10, got %d", helloEnvelope.Op)
	}
	var hello gatewayHello
	if err := json.Unmarshal(helloEnvelope.D, &hello); err != nil || hello.HeartbeatInterval <= 0 {
		return errors.New("invalid Discord Gateway heartbeat interval")
	}
	_ = conn.SetReadDeadline(time.Time{})

	heartbeatErr := make(chan error, 1)
	heartbeatFailure := make(chan error, 1)
	var heartbeatACK atomic.Bool
	heartbeatACK.Store(true)
	go g.heartbeatLoop(connCtx, time.Duration(hello.HeartbeatInterval*float64(time.Millisecond)), writeJSON, &heartbeatACK, heartbeatFailure)
	go func() {
		select {
		case err := <-heartbeatFailure:
			select {
			case heartbeatErr <- err:
			default:
			}
			_ = conn.Close()
		case <-connCtx.Done():
		}
	}()

	if resuming {
		g.stateMu.Lock()
		resume := gatewayResume{Token: g.botToken, SessionID: g.sessionID, Sequence: g.sequence}
		g.stateMu.Unlock()
		if err := writeJSON(map[string]any{"op": 6, "d": resume}); err != nil {
			return fmt.Errorf("resume Discord Gateway session: %w", err)
		}
	} else {
		activities := []gatewayActivity{}
		if g.activity != "" {
			activities = append(activities, gatewayActivity{Name: g.activity, Type: 3})
		}
		identify := gatewayIdentify{
			Token:   g.botToken,
			Intents: 0,
			Properties: gatewayIdentifyProperties{
				OS:      runtime.GOOS,
				Browser: "wrapping-bot",
				Device:  "wrapping-bot",
			},
			Presence: gatewayPresence{
				Activities: activities,
				Status:     g.status,
				AFK:        false,
			},
		}
		if err := writeJSON(map[string]any{"op": 2, "d": identify}); err != nil {
			return fmt.Errorf("identify Discord Gateway session: %w", err)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-heartbeatErr:
			return err
		default:
		}

		var envelope gatewayEnvelope
		if err := conn.ReadJSON(&envelope); err != nil {
			return fmt.Errorf("read Discord Gateway event: %w", err)
		}
		if envelope.S != nil {
			g.stateMu.Lock()
			g.sequence = *envelope.S
			g.stateMu.Unlock()
		}

		switch envelope.Op {
		case 0:
			switch envelope.T {
			case "READY":
				var ready gatewayReady
				if err := json.Unmarshal(envelope.D, &ready); err != nil {
					return fmt.Errorf("decode Discord READY event: %w", err)
				}
				g.stateMu.Lock()
				g.sessionID = ready.SessionID
				g.resumeGatewayURL = ready.ResumeGatewayURL
				g.stateMu.Unlock()
				g.connected.Store(true)
				g.logger.Info("Discord Gateway ready", "presence", g.status, "activity", g.activity)
			case "RESUMED":
				g.connected.Store(true)
				g.logger.Info("Discord Gateway session resumed")
			}
		case 1:
			heartbeatACK.Store(false)
			if err := g.sendHeartbeat(writeJSON); err != nil {
				return err
			}
		case 7:
			return errors.New("Discord requested Gateway reconnect")
		case 9:
			var resumable bool
			_ = json.Unmarshal(envelope.D, &resumable)
			if !resumable {
				g.clearSession()
			}
			time.Sleep(time.Duration(1+rand.Intn(5)) * time.Second)
			return errors.New("Discord Gateway session became invalid")
		case 11:
			heartbeatACK.Store(true)
		}
	}
}

func (g *Gateway) heartbeatLoop(ctx context.Context, interval time.Duration, writeJSON func(any) error, acked *atomic.Bool, errCh chan<- error) {
	firstDelay := time.Duration(rand.Float64() * float64(interval))
	timer := time.NewTimer(firstDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
	}
	acked.Store(false)
	if err := g.sendHeartbeat(writeJSON); err != nil {
		select {
		case errCh <- err:
		default:
		}
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !acked.Swap(false) {
				select {
				case errCh <- errors.New("Discord Gateway heartbeat was not acknowledged"):
				default:
				}
				return
			}
			if err := g.sendHeartbeat(writeJSON); err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
		}
	}
}

func (g *Gateway) sendHeartbeat(writeJSON func(any) error) error {
	g.stateMu.Lock()
	sequence := g.sequence
	g.stateMu.Unlock()
	var data any
	if sequence >= 0 {
		data = sequence
	}
	if err := writeJSON(map[string]any{"op": 1, "d": data}); err != nil {
		return fmt.Errorf("send Discord Gateway heartbeat: %w", err)
	}
	return nil
}

func (g *Gateway) connectionURL(ctx context.Context) (string, bool, error) {
	g.stateMu.Lock()
	resumeURL := g.resumeGatewayURL
	hasSession := g.sessionID != ""
	g.stateMu.Unlock()
	if hasSession && resumeURL != "" {
		endpoint, err := withGatewayQuery(resumeURL)
		return endpoint, true, err
	}
	if g.gatewayURL != "" {
		endpoint, err := withGatewayQuery(g.gatewayURL)
		return endpoint, false, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, g.apiBaseURL+"/gateway/bot", nil)
	if err != nil {
		return "", false, err
	}
	req.Header.Set("Authorization", "Bot "+g.botToken)
	req.Header.Set("User-Agent", "wrapping-bot/1")
	resp, err := g.http.Do(req)
	if err != nil {
		return "", false, fmt.Errorf("get Discord Gateway URL: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", false, fmt.Errorf("Discord Gateway URL request returned %s", resp.Status)
	}
	var payload gatewayBotResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", false, fmt.Errorf("decode Discord Gateway URL: %w", err)
	}
	endpoint, err := withGatewayQuery(payload.URL)
	return endpoint, false, err
}

func withGatewayQuery(raw string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("invalid Discord Gateway URL %q", raw)
	}
	query := parsed.Query()
	query.Set("v", defaultGatewayVersion)
	query.Set("encoding", "json")
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func (g *Gateway) clearSession() {
	g.stateMu.Lock()
	defer g.stateMu.Unlock()
	g.sessionID = ""
	g.resumeGatewayURL = ""
	g.sequence = -1
}
