package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/k-p2plab/wrapping-bot/internal/protocol"
)

func TestLooksSensitive(t *testing.T) {
	for _, key := range []string{"DISCORD_BOT_TOKEN", "DB_PASSWORD", "AWS_SECRET_ACCESS_KEY", "API_KEY"} {
		if !looksSensitive(key) {
			t.Fatalf("expected %s to be sensitive", key)
		}
	}
	for _, key := range []string{"TOPOLOGY", "MONKEY_COUNT", "PUBLIC_ID"} {
		if looksSensitive(key) {
			t.Fatalf("did not expect %s to be sensitive", key)
		}
	}
}

func TestShouldUseShell(t *testing.T) {
	if !shouldUseShell([]string{"./exp run"}) {
		t.Fatal("quoted command should use shell")
	}
	if shouldUseShell([]string{"./exp", "run"}) {
		t.Fatal("argument vector should not use shell")
	}
}

func TestIsDiscordChannelID(t *testing.T) {
	for _, value := range []string{"12345678901234567", "123456789012345678", "12345678901234567890"} {
		if !isDiscordChannelID(value) {
			t.Fatalf("expected %q to be accepted", value)
		}
	}
	for _, value := range []string{"", "123", "1234567890123456x", "123456789012345678901"} {
		if isDiscordChannelID(value) {
			t.Fatalf("expected %q to be rejected", value)
		}
	}
}

func TestParseOptionsChannelIDFlagOverridesEnvironment(t *testing.T) {
	t.Setenv("WRAPPING_BOT_TOKEN", "relay-secret")
	t.Setenv("WRAPPING_BOT_CHANNEL_ID", "111111111111111111")

	opts, command, err := parseOptions([]string{
		"--channel-id", "222222222222222222",
		"--", "echo", "hello",
	})
	if err != nil {
		t.Fatal(err)
	}
	if opts.channelID != "222222222222222222" {
		t.Fatalf("channel ID=%q", opts.channelID)
	}
	if len(command) != 2 || command[0] != "echo" || command[1] != "hello" {
		t.Fatalf("command=%v", command)
	}
}

func TestResolveClientEnvPathDefaultsToWorkingDirectory(t *testing.T) {
	t.Setenv("WRAPPING_BOT_CLIENT_ENV_FILE", "")
	path, required, err := resolveClientEnvPath([]string{"--", "echo", "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if path != "client.env" || required {
		t.Fatalf("path=%q required=%v", path, required)
	}
}

func TestResolveClientEnvPathFlagOverridesEnvironment(t *testing.T) {
	t.Setenv("WRAPPING_BOT_CLIENT_ENV_FILE", "/from/environment.env")
	path, required, err := resolveClientEnvPath([]string{
		"--client-env", "/from/flag.env",
		"--", "echo", "hello",
	})
	if err != nil {
		t.Fatal(err)
	}
	if path != "/from/flag.env" || !required {
		t.Fatalf("path=%q required=%v", path, required)
	}
}

func TestLoadClientEnvPreservesExistingEnvironment(t *testing.T) {
	path := t.TempDir() + "/client.env"
	content := "# client settings\n" +
		"WRAPPING_BOT_ENDPOINT=http://relay.example:8080\n" +
		"WRAPPING_BOT_TOKEN='file-token'\n" +
		"WRAPPING_BOT_RUN_NAME=Experiment run # inline comment\n" +
		"export WRAPPING_BOT_CHANNEL_ID=123456789012345678\n" +
		"QUOTED_VALUE=\"line\\nvalue\"\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv("WRAPPING_BOT_TOKEN", "process-token")
	for _, key := range []string{
		"WRAPPING_BOT_ENDPOINT",
		"WRAPPING_BOT_RUN_NAME",
		"WRAPPING_BOT_CHANNEL_ID",
		"QUOTED_VALUE",
	} {
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Unsetenv(key) })
	}

	if err := loadClientEnv(path, true); err != nil {
		t.Fatal(err)
	}
	if got := os.Getenv("WRAPPING_BOT_TOKEN"); got != "process-token" {
		t.Fatalf("token=%q", got)
	}
	if got := os.Getenv("WRAPPING_BOT_ENDPOINT"); got != "http://relay.example:8080" {
		t.Fatalf("endpoint=%q", got)
	}
	if got := os.Getenv("WRAPPING_BOT_RUN_NAME"); got != "Experiment run" {
		t.Fatalf("run name=%q", got)
	}
	if got := os.Getenv("WRAPPING_BOT_CHANNEL_ID"); got != "123456789012345678" {
		t.Fatalf("channel ID=%q", got)
	}
	if got := os.Getenv("QUOTED_VALUE"); got != "line\nvalue" {
		t.Fatalf("quoted value=%q", got)
	}
}

func TestLoadClientEnvMissingDefaultIsIgnored(t *testing.T) {
	if err := loadClientEnv(t.TempDir()+"/missing.env", false); err != nil {
		t.Fatal(err)
	}
}

func TestRunAutomaticallyLoadsClientEnv(t *testing.T) {
	var events []protocol.Event
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/runs/stream" {
			t.Errorf("path=%q", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		if got := r.Header.Get("Authorization"); got != "Bearer relay-secret" {
			t.Errorf("authorization=%q", got)
		}
		decoder := json.NewDecoder(r.Body)
		for decoder.More() {
			var event protocol.Event
			if err := decoder.Decode(&event); err != nil {
				t.Errorf("decode event: %v", err)
				return
			}
			events = append(events, event)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(protocol.StreamResponse{Accepted: true})
	}))
	defer server.Close()

	for _, key := range []string{
		"WRAPPING_BOT_CLIENT_ENV_FILE",
		"WRAPPING_BOT_ENDPOINT",
		"WRAPPING_BOT_TOKEN",
		"WRAPPING_BOT_CHANNEL_ID",
		"WRAPPING_BOT_RUN_NAME",
	} {
		unsetEnvironmentForTest(t, key)
	}

	directory := t.TempDir()
	content := "WRAPPING_BOT_ENDPOINT=" + server.URL + "\n" +
		"WRAPPING_BOT_TOKEN=relay-secret\n" +
		"WRAPPING_BOT_CHANNEL_ID=123456789012345678\n" +
		"WRAPPING_BOT_RUN_NAME=automatic env test\n"
	if err := os.WriteFile(directory+"/client.env", []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	oldDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(directory); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(oldDirectory) })

	if code := run([]string{"--", "/bin/sh", "-c", "printf auto-loaded"}); code != 0 {
		t.Fatalf("exit code=%d", code)
	}
	if len(events) < 3 {
		t.Fatalf("events=%d", len(events))
	}
	if events[0].Type != protocol.EventStart || events[0].ChannelID != "123456789012345678" {
		t.Fatalf("start event=%+v", events[0])
	}
	if events[0].Name != "automatic env test" {
		t.Fatalf("run name=%q", events[0].Name)
	}
	if events[len(events)-1].Type != protocol.EventExit {
		t.Fatalf("last event=%+v", events[len(events)-1])
	}
}

func unsetEnvironmentForTest(t *testing.T, key string) {
	t.Helper()
	value, existed := os.LookupEnv(key)
	if err := os.Unsetenv(key); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if existed {
			_ = os.Setenv(key, value)
		} else {
			_ = os.Unsetenv(key)
		}
	})
}
