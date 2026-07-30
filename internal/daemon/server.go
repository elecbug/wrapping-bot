package daemon

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/k-p2plab/wrapping-bot/internal/protocol"
	"github.com/k-p2plab/wrapping-bot/internal/textutil"
)

const maxOutputEventDataBytes = 64 * 1024

type Server struct {
	cfg       Config
	sender    MessageSender
	logger    *slog.Logger
	semaphore chan struct{}
	mux       *http.ServeMux
}

func NewServer(cfg Config, sender MessageSender, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Server{
		cfg:       cfg,
		sender:    sender,
		logger:    logger,
		semaphore: make(chan struct{}, cfg.MaxConcurrentRuns),
		mux:       http.NewServeMux(),
	}
	s.mux.HandleFunc("GET /healthz", s.handleHealth)
	s.mux.HandleFunc("POST /v1/runs/stream", s.handleStream)
	return s
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) HTTPServer() *http.Server {
	return &http.Server{
		Addr:              s.cfg.ListenAddr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"version": protocol.Version,
	})
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		writeJSON(w, http.StatusUnauthorized, protocol.StreamResponse{Accepted: false, Error: "unauthorized"})
		return
	}
	select {
	case s.semaphore <- struct{}{}:
		defer func() { <-s.semaphore }()
	default:
		writeJSON(w, http.StatusTooManyRequests, protocol.StreamResponse{Accepted: false, Error: "maximum concurrent runs reached"})
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.MaxStreamBytes)
	decoder := json.NewDecoder(r.Body)

	var start protocol.Event
	if err := decoder.Decode(&start); err != nil {
		writeJSON(w, http.StatusBadRequest, protocol.StreamResponse{Accepted: false, Error: "missing or invalid start event"})
		return
	}
	if err := validateStart(start); err != nil {
		writeJSON(w, http.StatusBadRequest, protocol.StreamResponse{RunID: start.RunID, Accepted: false, Error: err.Error()})
		return
	}
	channelID, ok := s.cfg.Channels[start.Target]
	if !ok {
		writeJSON(w, http.StatusBadRequest, protocol.StreamResponse{RunID: start.RunID, Accepted: false, Error: "unknown target"})
		return
	}

	if err := s.sendLong(channelID, formatStart(start)); err != nil {
		s.logger.Error("failed to send Discord start message", "run_id", start.RunID, "target", start.Target, "error", err)
		writeJSON(w, http.StatusBadGateway, protocol.StreamResponse{RunID: start.RunID, Accepted: false, Error: "failed to send Discord message"})
		return
	}

	s.logger.Info("run relay started", "run_id", start.RunID, "target", start.Target, "hostname", start.Hostname)
	batcher := NewBatcher(
		s.sender,
		channelID,
		start.RunID,
		s.cfg.FlushInterval,
		s.cfg.MaxLogChunkBytes,
		s.cfg.QueueSize,
		s.cfg.StripANSI,
	)

	var exitEvent *protocol.Event
	var streamErr error
	for {
		var event protocol.Event
		err := decoder.Decode(&event)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			streamErr = err
			break
		}
		if event.Version != protocol.Version || event.RunID != start.RunID {
			streamErr = errors.New("event version or run ID mismatch")
			break
		}

		switch event.Type {
		case protocol.EventOutput:
			if len(event.Data) > maxOutputEventDataBytes {
				event.Data = event.Data[:maxOutputEventDataBytes] + "\n[output event truncated by daemon]\n"
			}
			batcher.Add(event.Stream, event.Data)
		case protocol.EventExit:
			copyEvent := event
			exitEvent = &copyEvent
		default:
			streamErr = fmt.Errorf("unsupported event type %q", event.Type)
			goto streamDone
		}
	}

streamDone:
	droppedBytes, batchErr := batcher.Close()
	if batchErr != nil {
		s.logger.Error("Discord log relay failed", "run_id", start.RunID, "error", batchErr)
	}

	if streamErr != nil {
		_ = s.sendLong(channelID, fmt.Sprintf("⚠️ **Run relay interrupted**\nRun: `%s`\nReason: `%s`", shortRunID(start.RunID), inlineCode(streamErr.Error())))
		s.logger.Warn("run relay interrupted", "run_id", start.RunID, "error", streamErr)
		writeJSON(w, http.StatusBadRequest, protocol.StreamResponse{
			RunID:        start.RunID,
			Accepted:     false,
			DroppedBytes: droppedBytes,
			Error:        streamErr.Error(),
		})
		return
	}

	if exitEvent == nil {
		_ = s.sendLong(channelID, fmt.Sprintf("⚠️ **Run connection closed without an exit event**\nRun: `%s`", shortRunID(start.RunID)))
		s.logger.Warn("run stream closed without exit event", "run_id", start.RunID)
		writeJSON(w, http.StatusBadRequest, protocol.StreamResponse{
			RunID:        start.RunID,
			Accepted:     false,
			DroppedBytes: droppedBytes,
			Error:        "stream closed without exit event",
		})
		return
	}

	if err := s.sendLong(channelID, formatExit(start, *exitEvent, droppedBytes, batchErr)); err != nil {
		s.logger.Error("failed to send Discord exit message", "run_id", start.RunID, "error", err)
		if batchErr == nil {
			batchErr = err
		}
	}

	response := protocol.StreamResponse{
		RunID:        start.RunID,
		Accepted:     batchErr == nil,
		DroppedBytes: droppedBytes,
	}
	if batchErr != nil {
		response.Error = "one or more Discord messages failed"
	}
	status := http.StatusOK
	if batchErr != nil {
		status = http.StatusBadGateway
	}
	writeJSON(w, status, response)
	s.logger.Info("run relay completed", "run_id", start.RunID, "exit_code", valueOrDefault(exitEvent.ExitCode, -1), "dropped_bytes", droppedBytes)
}

func (s *Server) authorized(r *http.Request) bool {
	const prefix = "Bearer "
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, prefix) {
		return false
	}
	provided := sha256.Sum256([]byte(strings.TrimSpace(strings.TrimPrefix(header, prefix))))
	expected := sha256.Sum256([]byte(s.cfg.SharedToken))
	return subtle.ConstantTimeCompare(provided[:], expected[:]) == 1
}

func (s *Server) sendLong(channelID, content string) error {
	for _, part := range textutil.SplitUTF8(content, 1900) {
		if err := s.sender.Send(channelID, part); err != nil {
			return err
		}
	}
	return nil
}

func validateStart(event protocol.Event) error {
	if event.Version != protocol.Version {
		return fmt.Errorf("unsupported protocol version %d", event.Version)
	}
	if event.Type != protocol.EventStart {
		return errors.New("first event must be start")
	}
	if strings.TrimSpace(event.RunID) == "" || len(event.RunID) > 128 {
		return errors.New("invalid run ID")
	}
	if strings.TrimSpace(event.Target) == "" || len(event.Target) > 64 {
		return errors.New("invalid target")
	}
	if len(event.Command) == 0 {
		return errors.New("command is required")
	}
	if len(event.Environment) > 128 {
		return errors.New("too many environment fields")
	}
	return nil
}

func formatStart(event protocol.Event) string {
	name := event.Name
	if strings.TrimSpace(name) == "" {
		name = "wrapped process"
	}
	var builder strings.Builder
	builder.WriteString("▶️ **Run started: ")
	builder.WriteString(escapeMarkdown(name))
	builder.WriteString("**\n")
	builder.WriteString("Run: `")
	builder.WriteString(shortRunID(event.RunID))
	builder.WriteString("`\n")
	if event.Hostname != "" {
		builder.WriteString("Host: `")
		builder.WriteString(inlineCode(event.Hostname))
		builder.WriteString("`\n")
	}
	if event.WorkingDirectory != "" {
		builder.WriteString("Working directory: `")
		builder.WriteString(inlineCode(event.WorkingDirectory))
		builder.WriteString("`\n")
	}
	builder.WriteString("Command:\n```sh\n")
	builder.WriteString(textutil.EscapeCodeFence(formatCommand(event.Command, event.Shell)))
	builder.WriteString("\n```")

	if len(event.Environment) > 0 {
		keys := make([]string, 0, len(event.Environment))
		for key := range event.Environment {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		builder.WriteString("\nEnvironment:\n```text\n")
		for _, key := range keys {
			builder.WriteString(textutil.EscapeCodeFence(key))
			builder.WriteByte('=')
			builder.WriteString(textutil.EscapeCodeFence(event.Environment[key]))
			builder.WriteByte('\n')
		}
		builder.WriteString("```")
	}
	return builder.String()
}

func formatExit(start, exit protocol.Event, droppedBytes int64, relayErr error) string {
	exitCode := valueOrDefault(exit.ExitCode, -1)
	status := "✅ **Run completed**"
	if exitCode != 0 || exit.Error != "" {
		status = "❌ **Run failed**"
	}
	var builder strings.Builder
	builder.WriteString(status)
	builder.WriteString("\nRun: `")
	builder.WriteString(shortRunID(start.RunID))
	builder.WriteString("`\nExit code: `")
	builder.WriteString(strconv.Itoa(exitCode))
	builder.WriteString("`")
	if !start.Timestamp.IsZero() && !exit.Timestamp.IsZero() {
		builder.WriteString("\nDuration: `")
		builder.WriteString(exit.Timestamp.Sub(start.Timestamp).Round(time.Millisecond).String())
		builder.WriteString("`")
	}
	if exit.Error != "" {
		builder.WriteString("\nError: `")
		builder.WriteString(inlineCode(exit.Error))
		builder.WriteString("`")
	}
	if droppedBytes > 0 {
		builder.WriteString("\nDaemon-dropped output: `")
		builder.WriteString(strconv.FormatInt(droppedBytes, 10))
		builder.WriteString(" bytes`")
	}
	if relayErr != nil {
		builder.WriteString("\nRelay warning: `")
		builder.WriteString(inlineCode(relayErr.Error()))
		builder.WriteString("`")
	}
	return builder.String()
}

func formatCommand(command []string, shell bool) string {
	if shell && len(command) == 1 {
		return command[0]
	}
	parts := make([]string, 0, len(command))
	for _, arg := range command {
		parts = append(parts, shellQuote(arg))
	}
	return strings.Join(parts, " ")
}

func shellQuote(value string) string {
	if value == "" {
		return "''"
	}
	if strings.IndexFunc(value, func(r rune) bool {
		return !(r >= 'a' && r <= 'z') && !(r >= 'A' && r <= 'Z') && !(r >= '0' && r <= '9') && !strings.ContainsRune("_@%+=:,./-", r)
	}) == -1 {
		return value
	}
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func inlineCode(value string) string {
	value = strings.ReplaceAll(value, "`", "ˋ")
	value = strings.ReplaceAll(value, "\n", " ")
	value = strings.ReplaceAll(value, "\r", " ")
	return value
}

func escapeMarkdown(value string) string {
	replacer := strings.NewReplacer("\\", "\\\\", "*", "\\*", "_", "\\_", "~", "\\~", "`", "ˋ")
	return replacer.Replace(value)
}

func valueOrDefault(value *int, fallback int) int {
	if value == nil {
		return fallback
	}
	return *value
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
