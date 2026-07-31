package wsclient

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	opContinuation = 0x0
	opText         = 0x1
	opClose        = 0x8
	opPing         = 0x9
	opPong         = 0xA
	maxMessageSize = 16 << 20
	webSocketGUID  = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
)

type Conn struct {
	conn    net.Conn
	reader  *bufio.Reader
	writeMu sync.Mutex
}

func DialContext(ctx context.Context, rawURL string) (*Conn, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if parsed.Scheme != "ws" && parsed.Scheme != "wss" {
		return nil, fmt.Errorf("unsupported WebSocket scheme %q", parsed.Scheme)
	}
	if parsed.Host == "" {
		return nil, errors.New("WebSocket URL has no host")
	}

	host := parsed.Hostname()
	port := parsed.Port()
	if port == "" {
		if parsed.Scheme == "wss" {
			port = "443"
		} else {
			port = "80"
		}
	}
	address := net.JoinHostPort(host, port)

	dialer := &net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}
	var networkConn net.Conn
	if parsed.Scheme == "wss" {
		tlsDialer := &tls.Dialer{
			NetDialer: dialer,
			Config:    &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12},
		}
		networkConn, err = tlsDialer.DialContext(ctx, "tcp", address)
	} else {
		networkConn, err = dialer.DialContext(ctx, "tcp", address)
	}
	if err != nil {
		return nil, err
	}

	conn := &Conn{conn: networkConn, reader: bufio.NewReader(networkConn)}
	if err := conn.handshake(ctx, parsed); err != nil {
		_ = networkConn.Close()
		return nil, err
	}
	return conn, nil
}

func (c *Conn) handshake(ctx context.Context, parsed *url.URL) error {
	keyBytes := make([]byte, 16)
	if _, err := rand.Read(keyBytes); err != nil {
		return err
	}
	key := base64.StdEncoding.EncodeToString(keyBytes)

	requestURI := parsed.RequestURI()
	if requestURI == "" {
		requestURI = "/"
	}
	req := &http.Request{
		Method:     http.MethodGet,
		URL:        parsed,
		Host:       parsed.Host,
		RequestURI: requestURI,
		Header:     make(http.Header),
	}
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Sec-WebSocket-Key", key)
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("User-Agent", "wrapping-bot/1")

	deadline, hasDeadline := ctx.Deadline()
	if hasDeadline {
		_ = c.conn.SetDeadline(deadline)
	} else {
		_ = c.conn.SetDeadline(time.Now().Add(15 * time.Second))
	}
	if err := req.Write(c.conn); err != nil {
		return err
	}
	resp, err := http.ReadResponse(c.reader, req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		return fmt.Errorf("WebSocket upgrade returned %s", resp.Status)
	}
	if !headerContainsToken(resp.Header, "Connection", "upgrade") || !headerContainsToken(resp.Header, "Upgrade", "websocket") {
		return errors.New("invalid WebSocket upgrade headers")
	}
	hash := sha1.Sum([]byte(key + webSocketGUID))
	expected := base64.StdEncoding.EncodeToString(hash[:])
	if resp.Header.Get("Sec-WebSocket-Accept") != expected {
		return errors.New("invalid Sec-WebSocket-Accept header")
	}
	_ = c.conn.SetDeadline(time.Time{})
	return nil
}

func headerContainsToken(header http.Header, key, token string) bool {
	for _, value := range header.Values(key) {
		for _, part := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(part), token) {
				return true
			}
		}
	}
	return false
}

func (c *Conn) ReadJSON(value any) error {
	payload, err := c.readMessage()
	if err != nil {
		return err
	}
	return json.Unmarshal(payload, value)
}

func (c *Conn) WriteJSON(value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.writeFrame(opText, payload)
}

func (c *Conn) readMessage() ([]byte, error) {
	var message []byte
	started := false
	for {
		fin, opcode, payload, err := c.readFrame()
		if err != nil {
			return nil, err
		}
		switch opcode {
		case opPing:
			if err := c.writeFrame(opPong, payload); err != nil {
				return nil, err
			}
			continue
		case opPong:
			continue
		case opClose:
			_ = c.writeFrame(opClose, payload)
			return nil, io.EOF
		case opText:
			if started {
				return nil, errors.New("unexpected new WebSocket data frame")
			}
			started = true
			message = append(message, payload...)
		case opContinuation:
			if !started {
				return nil, errors.New("unexpected WebSocket continuation frame")
			}
			message = append(message, payload...)
		default:
			return nil, fmt.Errorf("unsupported WebSocket opcode %d", opcode)
		}
		if len(message) > maxMessageSize {
			return nil, errors.New("WebSocket message exceeds size limit")
		}
		if fin && started {
			return message, nil
		}
	}
}

func (c *Conn) readFrame() (bool, byte, []byte, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(c.reader, header); err != nil {
		return false, 0, nil, err
	}
	fin := header[0]&0x80 != 0
	opcode := header[0] & 0x0F
	masked := header[1]&0x80 != 0
	length := uint64(header[1] & 0x7F)

	switch length {
	case 126:
		var extended [2]byte
		if _, err := io.ReadFull(c.reader, extended[:]); err != nil {
			return false, 0, nil, err
		}
		length = uint64(binary.BigEndian.Uint16(extended[:]))
	case 127:
		var extended [8]byte
		if _, err := io.ReadFull(c.reader, extended[:]); err != nil {
			return false, 0, nil, err
		}
		length = binary.BigEndian.Uint64(extended[:])
		if length&(1<<63) != 0 {
			return false, 0, nil, errors.New("invalid WebSocket frame length")
		}
	}
	if length > maxMessageSize {
		return false, 0, nil, errors.New("WebSocket frame exceeds size limit")
	}

	var mask [4]byte
	if masked {
		if _, err := io.ReadFull(c.reader, mask[:]); err != nil {
			return false, 0, nil, err
		}
	}
	payload := make([]byte, int(length))
	if _, err := io.ReadFull(c.reader, payload); err != nil {
		return false, 0, nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}
	if opcode >= opClose && (!fin || len(payload) > 125) {
		return false, 0, nil, errors.New("invalid WebSocket control frame")
	}
	return fin, opcode, payload, nil
}

func (c *Conn) writeFrame(opcode byte, payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if len(payload) > maxMessageSize {
		return errors.New("WebSocket payload exceeds size limit")
	}

	header := []byte{0x80 | opcode}
	length := len(payload)
	switch {
	case length <= 125:
		header = append(header, 0x80|byte(length))
	case length <= 65535:
		header = append(header, 0x80|126, byte(length>>8), byte(length))
	default:
		header = append(header, 0x80|127)
		var extended [8]byte
		binary.BigEndian.PutUint64(extended[:], uint64(length))
		header = append(header, extended[:]...)
	}

	var mask [4]byte
	if _, err := rand.Read(mask[:]); err != nil {
		return err
	}
	header = append(header, mask[:]...)
	maskedPayload := make([]byte, length)
	for i := range payload {
		maskedPayload[i] = payload[i] ^ mask[i%4]
	}
	if _, err := c.conn.Write(header); err != nil {
		return err
	}
	_, err := c.conn.Write(maskedPayload)
	return err
}

func (c *Conn) SetReadDeadline(deadline time.Time) error {
	return c.conn.SetReadDeadline(deadline)
}

func (c *Conn) SetWriteDeadline(deadline time.Time) error {
	return c.conn.SetWriteDeadline(deadline)
}

func (c *Conn) Close() error {
	_ = c.conn.SetWriteDeadline(time.Now().Add(time.Second))
	payload := make([]byte, 2)
	binary.BigEndian.PutUint16(payload, 1000)
	_ = c.writeFrame(opClose, payload)
	return c.conn.Close()
}

// ServerFrame writes an unmasked frame. It is intended for local tests only.
func ServerFrame(opcode byte, payload []byte) []byte {
	header := []byte{0x80 | opcode}
	switch {
	case len(payload) <= 125:
		header = append(header, byte(len(payload)))
	case len(payload) <= 65535:
		header = append(header, 126, byte(len(payload)>>8), byte(len(payload)))
	default:
		header = append(header, 127)
		var extended [8]byte
		binary.BigEndian.PutUint64(extended[:], uint64(len(payload)))
		header = append(header, extended[:]...)
	}
	return append(header, payload...)
}

// ReadClientFrame reads one masked client frame. It is intended for local tests only.
func ReadClientFrame(reader *bufio.Reader) (byte, []byte, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(reader, header); err != nil {
		return 0, nil, err
	}
	opcode := header[0] & 0x0F
	masked := header[1]&0x80 != 0
	if !masked {
		return 0, nil, errors.New("client WebSocket frame is not masked")
	}
	length := uint64(header[1] & 0x7F)
	if length == 126 {
		var extended [2]byte
		if _, err := io.ReadFull(reader, extended[:]); err != nil {
			return 0, nil, err
		}
		length = uint64(binary.BigEndian.Uint16(extended[:]))
	} else if length == 127 {
		var extended [8]byte
		if _, err := io.ReadFull(reader, extended[:]); err != nil {
			return 0, nil, err
		}
		length = binary.BigEndian.Uint64(extended[:])
	}
	if length > maxMessageSize {
		return 0, nil, errors.New("client frame exceeds size limit")
	}
	var mask [4]byte
	if _, err := io.ReadFull(reader, mask[:]); err != nil {
		return 0, nil, err
	}
	payload := make([]byte, int(length))
	if _, err := io.ReadFull(reader, payload); err != nil {
		return 0, nil, err
	}
	for i := range payload {
		payload[i] ^= mask[i%4]
	}
	return opcode, payload, nil
}

func UpgradeForTest(w http.ResponseWriter, r *http.Request) (net.Conn, *bufio.Reader, error) {
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		return nil, nil, errors.New("missing Sec-WebSocket-Key")
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("HTTP server does not support hijacking")
	}
	conn, readWriter, err := hijacker.Hijack()
	if err != nil {
		return nil, nil, err
	}
	hash := sha1.Sum([]byte(key + webSocketGUID))
	accept := base64.StdEncoding.EncodeToString(hash[:])
	response := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
	if _, err := readWriter.WriteString(response); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	if err := readWriter.Flush(); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	return conn, readWriter.Reader, nil
}

func ParseCloseCode(payload []byte) int {
	if len(payload) < 2 {
		return 0
	}
	return int(binary.BigEndian.Uint16(payload[:2]))
}

func FormatClosePayload(code int, reason string) []byte {
	payload := make([]byte, 2, 2+len(reason))
	binary.BigEndian.PutUint16(payload, uint16(code))
	return append(payload, []byte(reason)...)
}

func HeaderValueLength(length int) string {
	return strconv.Itoa(length)
}
