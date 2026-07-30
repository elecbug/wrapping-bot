package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/k-p2plab/wrapping-bot/internal/clientstream"
	"github.com/k-p2plab/wrapping-bot/internal/protocol"
)

const usageText = `Usage:
  wrapping-bot [options] -- command [args...]
  wrapping-bot [options] "shell command"

Examples:
  wrapping-bot -- ./exp run
  wrapping-bot "./exp run"
  wrapping-bot --channel-id 123456789012345678 --env TOPOLOGY --env SEED -- ./exp run
`

type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }
func (s *stringList) Set(value string) error {
	value = strings.TrimSpace(value)
	if value != "" {
		*s = append(*s, value)
	}
	return nil
}

type options struct {
	endpoint      string
	token         string
	channelID     string
	name          string
	shell         bool
	envKeys       stringList
	envPrefixes   stringList
	allowSecrets  bool
	queueSize     int
	finishTimeout time.Duration
}

func main() {
	code := run(os.Args[1:])
	os.Exit(code)
}

func run(args []string) int {
	opts, commandArgs, err := parseOptions(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, "wrapping-bot:", err)
		fmt.Fprint(os.Stderr, usageText)
		return 2
	}

	runID, err := newRunID()
	if err != nil {
		fmt.Fprintln(os.Stderr, "wrapping-bot: generate run ID:", err)
		return 1
	}
	hostname, _ := os.Hostname()
	cwd, _ := os.Getwd()
	shellMode := opts.shell || shouldUseShell(commandArgs)

	session, err := clientstream.New(context.Background(), clientstream.Config{
		Endpoint:      opts.endpoint,
		Token:         opts.token,
		QueueSize:     opts.queueSize,
		FinishTimeout: opts.finishTimeout,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "wrapping-bot:", err)
		return 2
	}

	startedAt := time.Now().UTC()
	startEvent := protocol.Event{
		Version:          protocol.Version,
		Type:             protocol.EventStart,
		RunID:            runID,
		Timestamp:        startedAt,
		ChannelID:        opts.channelID,
		Name:             opts.name,
		Command:          append([]string(nil), commandArgs...),
		Shell:            shellMode,
		Hostname:         hostname,
		WorkingDirectory: cwd,
		Environment:      collectEnvironment(opts.envKeys, opts.envPrefixes, opts.allowSecrets),
	}
	if err := session.SendStart(startEvent); err != nil {
		fmt.Fprintln(os.Stderr, "wrapping-bot: initialize relay:", err)
		return 1
	}

	cmd := buildCommand(commandArgs, shellMode)
	cmd.Env = os.Environ()
	cmd.Stdin = os.Stdin
	cmd.Stdout = io.MultiWriter(os.Stdout, session.Writer(runID, protocol.StreamStdout))
	cmd.Stderr = io.MultiWriter(os.Stderr, session.Writer(runID, protocol.StreamStderr))

	exitCode := 0
	exitError := ""
	if err := cmd.Start(); err != nil {
		exitCode = 127
		exitError = err.Error()
		fmt.Fprintln(os.Stderr, "wrapping-bot: start command:", err)
	} else if err := cmd.Wait(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
		} else {
			exitCode = 1
			exitError = err.Error()
			fmt.Fprintln(os.Stderr, "wrapping-bot: wait command:", err)
		}
	}

	finishedAt := time.Now().UTC()
	exitEvent := protocol.Event{
		Version:   protocol.Version,
		Type:      protocol.EventExit,
		RunID:     runID,
		Timestamp: finishedAt,
		ExitCode:  intPointer(exitCode),
		Error:     exitError,
	}
	result := session.Close(exitEvent, opts.finishTimeout)
	if result.Err != nil {
		fmt.Fprintln(os.Stderr, "wrapping-bot: relay error:", result.Err)
	} else if result.Response.DroppedBytes > 0 {
		fmt.Fprintf(os.Stderr, "wrapping-bot: daemon omitted %d log bytes\n", result.Response.DroppedBytes)
	}
	return exitCode
}

func parseOptions(args []string) (options, []string, error) {
	defaultsKeys := parseCSV(os.Getenv("WRAPPING_BOT_ENV_KEYS"))
	defaultsPrefixes := parseCSV(os.Getenv("WRAPPING_BOT_ENV_PREFIXES"))
	if len(defaultsKeys) == 0 && len(defaultsPrefixes) == 0 {
		defaultsKeys = []string{"RUN_ID", "TOPOLOGY", "PROTOCOL", "NODES", "SEED"}
		defaultsPrefixes = []string{"EXP_", "PEERKIT_"}
	}

	opts := options{
		endpoint:      envOr("WRAPPING_BOT_ENDPOINT", "http://127.0.0.1:8080"),
		token:         strings.TrimSpace(os.Getenv("WRAPPING_BOT_TOKEN")),
		channelID:     strings.TrimSpace(os.Getenv("WRAPPING_BOT_CHANNEL_ID")),
		name:          strings.TrimSpace(os.Getenv("WRAPPING_BOT_RUN_NAME")),
		envKeys:       append(stringList(nil), defaultsKeys...),
		envPrefixes:   append(stringList(nil), defaultsPrefixes...),
		allowSecrets:  envBool("WRAPPING_BOT_ALLOW_SECRET_ENV", false),
		queueSize:     envInt("WRAPPING_BOT_CLIENT_QUEUE_SIZE", 1024),
		finishTimeout: envDuration("WRAPPING_BOT_FINISH_TIMEOUT", 15*time.Second),
	}

	fs := flag.NewFlagSet("wrapping-bot", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&opts.endpoint, "endpoint", opts.endpoint, "relay daemon base URL")
	fs.StringVar(&opts.token, "token", opts.token, "relay shared token")
	fs.StringVar(&opts.channelID, "channel-id", opts.channelID, "Discord channel ID receiving this run")
	fs.StringVar(&opts.name, "name", opts.name, "display name for this run")
	fs.BoolVar(&opts.shell, "shell", false, "always execute through /bin/sh -lc")
	fs.Var(&opts.envKeys, "env", "environment key to include; repeatable")
	fs.Var(&opts.envPrefixes, "env-prefix", "environment key prefix to include; repeatable")
	fs.BoolVar(&opts.allowSecrets, "allow-secret-env", opts.allowSecrets, "permit secret-looking environment keys")
	fs.IntVar(&opts.queueSize, "queue-size", opts.queueSize, "local asynchronous relay queue size")
	fs.DurationVar(&opts.finishTimeout, "finish-timeout", opts.finishTimeout, "maximum wait for daemon completion")
	if err := fs.Parse(args); err != nil {
		return options{}, nil, err
	}

	commandArgs := fs.Args()
	if len(commandArgs) == 0 {
		return options{}, nil, errors.New("command is required")
	}
	if strings.TrimSpace(opts.token) == "" {
		return options{}, nil, errors.New("WRAPPING_BOT_TOKEN or --token is required")
	}
	if !isDiscordChannelID(opts.channelID) {
		return options{}, nil, errors.New("WRAPPING_BOT_CHANNEL_ID or --channel-id must be a 17-20 digit Discord channel ID")
	}
	if opts.queueSize < 1 {
		return options{}, nil, errors.New("queue size must be positive")
	}
	return opts, commandArgs, nil
}

func isDiscordChannelID(value string) bool {
	if value != strings.TrimSpace(value) || len(value) < 17 || len(value) > 20 {
		return false
	}
	_, err := strconv.ParseUint(value, 10, 64)
	return err == nil
}

func buildCommand(args []string, shell bool) *exec.Cmd {
	if shell {
		command := args[0]
		if len(args) > 1 {
			parts := make([]string, 0, len(args))
			for _, arg := range args {
				parts = append(parts, shellQuote(arg))
			}
			command = strings.Join(parts, " ")
		}
		return exec.Command("/bin/sh", "-lc", command)
	}
	return exec.Command(args[0], args[1:]...)
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

func shouldUseShell(args []string) bool {
	if len(args) != 1 {
		return false
	}
	return strings.ContainsAny(args[0], " \t|&;<>()$`*?[]{}~'\"\\")
}

func collectEnvironment(keys, prefixes []string, allowSecrets bool) map[string]string {
	all := make(map[string]string)
	for _, entry := range os.Environ() {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			all[key] = value
		}
	}

	selected := make(map[string]string)
	for _, key := range keys {
		if value, ok := all[key]; ok {
			addEnvironment(selected, key, value, allowSecrets)
		}
	}
	for key, value := range all {
		for _, prefix := range prefixes {
			if prefix != "" && strings.HasPrefix(key, prefix) {
				addEnvironment(selected, key, value, allowSecrets)
				break
			}
		}
	}

	if len(selected) <= 128 {
		return selected
	}
	keysSorted := make([]string, 0, len(selected))
	for key := range selected {
		keysSorted = append(keysSorted, key)
	}
	sort.Strings(keysSorted)
	limited := make(map[string]string, 128)
	for _, key := range keysSorted[:128] {
		limited[key] = selected[key]
	}
	return limited
}

func addEnvironment(target map[string]string, key, value string, allowSecrets bool) {
	if len(key) == 0 || len(key) > 128 || (!allowSecrets && looksSensitive(key)) {
		return
	}
	if len(value) > 512 {
		value = truncateUTF8(value, 512) + "…"
	}
	target[key] = value
}

func looksSensitive(key string) bool {
	upper := strings.ToUpper(key)
	parts := strings.FieldsFunc(upper, func(r rune) bool { return r == '_' || r == '-' || r == '.' })
	for _, part := range parts {
		switch part {
		case "TOKEN", "SECRET", "PASSWORD", "PASSWD", "CREDENTIAL", "CREDENTIALS", "COOKIE", "AUTH", "AUTHORIZATION", "PRIVATEKEY", "PRIVATE":
			return true
		}
	}
	return strings.HasSuffix(upper, "_KEY") || strings.Contains(upper, "API_KEY") || strings.Contains(upper, "PRIVATE_KEY")
}

func newRunID() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func intPointer(value int) *int { return &value }

func truncateUTF8(value string, maxBytes int) string {
	if len(value) <= maxBytes {
		return value
	}
	cut := maxBytes
	for cut > 0 && (value[cut]&0xC0) == 0x80 {
		cut--
	}
	return value[:cut]
}

func parseCSV(raw string) []string {
	var values []string
	for _, value := range strings.Split(raw, ",") {
		if value = strings.TrimSpace(value); value != "" {
			values = append(values, value)
		}
	}
	return values
}

func envOr(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	value, err := strconv.Atoi(strings.TrimSpace(os.Getenv(key)))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func envBool(key string, fallback bool) bool {
	value, err := strconv.ParseBool(strings.TrimSpace(os.Getenv(key)))
	if err != nil {
		return fallback
	}
	return value
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value, err := time.ParseDuration(strings.TrimSpace(os.Getenv(key)))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}
