package protocol

import "time"

const Version = 2

const (
	EventStart  = "start"
	EventOutput = "output"
	EventExit   = "exit"
)

const (
	StreamStdout = "stdout"
	StreamStderr = "stderr"
	StreamSystem = "system"
)

// Event is one NDJSON record in the wrapper-to-daemon streaming protocol.
type Event struct {
	Version          int               `json:"version"`
	Type             string            `json:"type"`
	RunID            string            `json:"run_id"`
	Timestamp        time.Time         `json:"timestamp"`
	ChannelID        string            `json:"channel_id,omitempty"`
	Name             string            `json:"name,omitempty"`
	Command          []string          `json:"command,omitempty"`
	Shell            bool              `json:"shell,omitempty"`
	Hostname         string            `json:"hostname,omitempty"`
	WorkingDirectory string            `json:"working_directory,omitempty"`
	Environment      map[string]string `json:"environment,omitempty"`
	Stream           string            `json:"stream,omitempty"`
	Data             string            `json:"data,omitempty"`
	ExitCode         *int              `json:"exit_code,omitempty"`
	Error            string            `json:"error,omitempty"`
}

type StreamResponse struct {
	RunID        string `json:"run_id"`
	Accepted     bool   `json:"accepted"`
	DroppedBytes int64  `json:"dropped_bytes,omitempty"`
	Error        string `json:"error,omitempty"`
}
