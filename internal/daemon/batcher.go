package daemon

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/k-p2plab/wrapping-bot/internal/protocol"
	"github.com/k-p2plab/wrapping-bot/internal/textutil"
)

type MessageSender interface {
	Send(channelID, content string) error
}

type outputRecord struct {
	stream string
	data   string
}

type Batcher struct {
	sender      MessageSender
	channelID   string
	runID       string
	flushEvery  time.Duration
	maxLogBytes int
	stripANSI   bool
	events      chan outputRecord
	done        chan struct{}
	dropped     atomic.Int64

	errMu    sync.Mutex
	firstErr error
}

func NewBatcher(sender MessageSender, channelID, runID string, flushEvery time.Duration, maxLogBytes, queueSize int, stripANSI bool) *Batcher {
	b := &Batcher{
		sender:      sender,
		channelID:   channelID,
		runID:       runID,
		flushEvery:  flushEvery,
		maxLogBytes: maxLogBytes,
		stripANSI:   stripANSI,
		events:      make(chan outputRecord, queueSize),
		done:        make(chan struct{}),
	}
	go b.run()
	return b
}

func (b *Batcher) Add(stream, data string) {
	if data == "" {
		return
	}
	select {
	case b.events <- outputRecord{stream: stream, data: data}:
	default:
		b.dropped.Add(int64(len(data)))
	}
}

func (b *Batcher) Close() (int64, error) {
	close(b.events)
	<-b.done
	b.errMu.Lock()
	defer b.errMu.Unlock()
	return b.dropped.Load(), b.firstErr
}

func (b *Batcher) run() {
	defer close(b.done)
	ticker := time.NewTicker(b.flushEvery)
	defer ticker.Stop()

	var currentStream string
	var buffer strings.Builder

	flush := func() {
		if buffer.Len() == 0 {
			return
		}
		b.sendOutput(currentStream, buffer.String())
		buffer.Reset()
	}

	for {
		select {
		case record, ok := <-b.events:
			if !ok {
				if dropped := b.dropped.Load(); dropped > 0 {
					flush()
					currentStream = protocol.StreamSystem
					buffer.WriteString(fmt.Sprintf("[wrapping-bot daemon queue overflow: %d bytes omitted]\n", dropped))
				}
				flush()
				return
			}
			stream := normalizeStream(record.stream)
			if currentStream != "" && stream != currentStream {
				flush()
			}
			currentStream = stream
			data := record.data
			if b.stripANSI {
				data = textutil.StripANSI(data)
			}
			buffer.WriteString(data)
			if buffer.Len() >= b.maxLogBytes {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (b *Batcher) sendOutput(stream, data string) {
	data = textutil.EscapeCodeFence(data)
	for _, part := range textutil.SplitUTF8(data, b.maxLogBytes) {
		content := fmt.Sprintf("**`%s` · `%s`**\n```text\n%s\n```", shortRunID(b.runID), stream, strings.TrimSuffix(part, "\n"))
		if err := b.sender.Send(b.channelID, content); err != nil {
			b.recordError(err)
		}
	}
}

func (b *Batcher) recordError(err error) {
	b.errMu.Lock()
	defer b.errMu.Unlock()
	if b.firstErr == nil {
		b.firstErr = err
	}
}

func normalizeStream(stream string) string {
	switch stream {
	case protocol.StreamStdout, protocol.StreamStderr, protocol.StreamSystem:
		return stream
	default:
		return protocol.StreamSystem
	}
}

func shortRunID(runID string) string {
	if len(runID) <= 8 {
		return runID
	}
	return runID[:8]
}
