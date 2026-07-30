package discord

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const defaultAPIBaseURL = "https://discord.com/api/v10"

type Sender struct {
	botToken   string
	baseURL    string
	http       *http.Client
	mu         sync.Mutex
	nextSend   time.Time
	maxRetries int
}

type createMessageRequest struct {
	Content         string          `json:"content"`
	AllowedMentions allowedMentions `json:"allowed_mentions"`
	Flags           int             `json:"flags,omitempty"`
}

type allowedMentions struct {
	Parse []string `json:"parse"`
}

type rateLimitResponse struct {
	Message    string  `json:"message"`
	RetryAfter float64 `json:"retry_after"`
	Global     bool    `json:"global"`
}

func NewSender(botToken, baseURL string) (*Sender, error) {
	botToken = strings.TrimSpace(botToken)
	if botToken == "" {
		return nil, errors.New("Discord bot token is required")
	}
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		baseURL = defaultAPIBaseURL
	}
	return &Sender{
		botToken: botToken,
		baseURL:  baseURL,
		http: &http.Client{
			Timeout: 30 * time.Second,
		},
		maxRetries: 6,
	}, nil
}

func (s *Sender) Send(channelID, content string) error {
	channelID = strings.TrimSpace(channelID)
	if channelID == "" {
		return errors.New("Discord channel ID is required")
	}
	if content == "" {
		return nil
	}

	payload, err := json.Marshal(createMessageRequest{
		Content:         content,
		AllowedMentions: allowedMentions{Parse: []string{}},
		Flags:           1 << 12, // SUPPRESS_NOTIFICATIONS
	})
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var lastErr error
	for attempt := 0; attempt <= s.maxRetries; attempt++ {
		if wait := time.Until(s.nextSend); wait > 0 {
			time.Sleep(wait)
		}

		requestURL := fmt.Sprintf("%s/channels/%s/messages", s.baseURL, channelID)
		req, err := http.NewRequest(http.MethodPost, requestURL, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bot "+s.botToken)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "wrapping-bot/1")

		resp, err := s.http.Do(req)
		if err != nil {
			lastErr = err
			if attempt == s.maxRetries {
				break
			}
			time.Sleep(retryBackoff(attempt))
			continue
		}

		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 16<<10))
		_ = resp.Body.Close()
		if readErr != nil {
			return readErr
		}

		s.updateRateWindow(resp.Header)
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			var rateLimit rateLimitResponse
			_ = json.Unmarshal(body, &rateLimit)
			wait := durationFromSeconds(rateLimit.RetryAfter)
			if headerWait := parseRetryAfter(resp.Header.Get("Retry-After")); headerWait > wait {
				wait = headerWait
			}
			if wait <= 0 {
				wait = retryBackoff(attempt)
			}
			s.nextSend = time.Now().Add(wait)
			lastErr = fmt.Errorf("Discord rate limited request for %s", wait)
			continue
		}

		message := strings.TrimSpace(string(body))
		if message == "" {
			message = http.StatusText(resp.StatusCode)
		}
		lastErr = fmt.Errorf("Discord API returned %s: %s", resp.Status, message)
		if resp.StatusCode < 500 || attempt == s.maxRetries {
			break
		}
		time.Sleep(retryBackoff(attempt))
	}
	return lastErr
}

func (s *Sender) updateRateWindow(header http.Header) {
	if header.Get("X-RateLimit-Remaining") != "0" {
		return
	}
	seconds, err := strconv.ParseFloat(header.Get("X-RateLimit-Reset-After"), 64)
	if err != nil || seconds <= 0 {
		return
	}
	s.nextSend = time.Now().Add(durationFromSeconds(seconds))
}

func parseRetryAfter(raw string) time.Duration {
	seconds, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil || seconds <= 0 {
		return 0
	}
	return durationFromSeconds(seconds)
}

func durationFromSeconds(seconds float64) time.Duration {
	return time.Duration(seconds * float64(time.Second))
}

func retryBackoff(attempt int) time.Duration {
	if attempt > 5 {
		attempt = 5
	}
	return time.Duration(1<<attempt) * 250 * time.Millisecond
}
