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
