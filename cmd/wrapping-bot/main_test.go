package main

import "testing"

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
