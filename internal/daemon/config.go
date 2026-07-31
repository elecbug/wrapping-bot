package daemon

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ListenAddr            string
	DiscordBotToken       string
	DiscordAPIBaseURL     string
	DiscordGatewayEnabled bool
	DiscordGatewayURL     string
	DiscordPresenceStatus string
	DiscordActivity       string
	SharedToken           string
	AllowedChannelIDs     map[string]struct{}
	FlushInterval         time.Duration
	MaxLogChunkBytes      int
	QueueSize             int
	MaxConcurrentRuns     int
	MaxStreamBytes        int64
	StripANSI             bool
}

func LoadConfigFromEnv() (Config, error) {
	cfg := Config{
		ListenAddr:            envOr("WRAPPING_BOT_LISTEN_ADDR", ":8080"),
		DiscordBotToken:       strings.TrimSpace(os.Getenv("DISCORD_BOT_TOKEN")),
		DiscordAPIBaseURL:     envOr("DISCORD_API_BASE_URL", "https://discord.com/api/v10"),
		DiscordGatewayEnabled: true,
		DiscordGatewayURL:     strings.TrimSpace(os.Getenv("DISCORD_GATEWAY_URL")),
		DiscordPresenceStatus: envOr("WRAPPING_BOT_DISCORD_STATUS", "online"),
		DiscordActivity:       envOr("WRAPPING_BOT_DISCORD_ACTIVITY", "process logs"),
		SharedToken:           strings.TrimSpace(os.Getenv("WRAPPING_BOT_SHARED_TOKEN")),
		AllowedChannelIDs:     make(map[string]struct{}),
		FlushInterval:         1500 * time.Millisecond,
		MaxLogChunkBytes:      1600,
		QueueSize:             1024,
		MaxConcurrentRuns:     32,
		MaxStreamBytes:        1 << 30,
		StripANSI:             true,
	}

	if cfg.DiscordBotToken == "" {
		return Config{}, errors.New("DISCORD_BOT_TOKEN is required")
	}
	if cfg.SharedToken == "" {
		return Config{}, errors.New("WRAPPING_BOT_SHARED_TOKEN is required")
	}
	if raw := strings.TrimSpace(os.Getenv("WRAPPING_BOT_GATEWAY_ENABLED")); raw != "" {
		enabled, parseErr := strconv.ParseBool(raw)
		if parseErr != nil {
			return Config{}, fmt.Errorf("invalid WRAPPING_BOT_GATEWAY_ENABLED: %q", raw)
		}
		cfg.DiscordGatewayEnabled = enabled
	}
	cfg.DiscordPresenceStatus = strings.ToLower(strings.TrimSpace(cfg.DiscordPresenceStatus))
	switch cfg.DiscordPresenceStatus {
	case "online", "idle", "dnd", "invisible":
	default:
		return Config{}, fmt.Errorf("invalid WRAPPING_BOT_DISCORD_STATUS: %q", cfg.DiscordPresenceStatus)
	}

	for _, channelID := range strings.Split(os.Getenv("WRAPPING_BOT_ALLOWED_CHANNEL_IDS"), ",") {
		channelID = strings.TrimSpace(channelID)
		if channelID == "" {
			continue
		}
		if !isDiscordChannelID(channelID) {
			return Config{}, fmt.Errorf("invalid channel ID in WRAPPING_BOT_ALLOWED_CHANNEL_IDS: %q", channelID)
		}
		cfg.AllowedChannelIDs[channelID] = struct{}{}
	}

	var err error
	if raw := strings.TrimSpace(os.Getenv("WRAPPING_BOT_FLUSH_INTERVAL")); raw != "" {
		cfg.FlushInterval, err = time.ParseDuration(raw)
		if err != nil || cfg.FlushInterval <= 0 {
			return Config{}, fmt.Errorf("invalid WRAPPING_BOT_FLUSH_INTERVAL: %q", raw)
		}
	}
	if cfg.MaxLogChunkBytes, err = envInt("WRAPPING_BOT_MAX_LOG_CHUNK_BYTES", cfg.MaxLogChunkBytes); err != nil {
		return Config{}, err
	}
	if cfg.QueueSize, err = envInt("WRAPPING_BOT_QUEUE_SIZE", cfg.QueueSize); err != nil {
		return Config{}, err
	}
	if cfg.MaxConcurrentRuns, err = envInt("WRAPPING_BOT_MAX_CONCURRENT_RUNS", cfg.MaxConcurrentRuns); err != nil {
		return Config{}, err
	}
	if cfg.MaxStreamBytes, err = envInt64("WRAPPING_BOT_MAX_STREAM_BYTES", cfg.MaxStreamBytes); err != nil {
		return Config{}, err
	}
	if raw := strings.TrimSpace(os.Getenv("WRAPPING_BOT_STRIP_ANSI")); raw != "" {
		cfg.StripANSI, err = strconv.ParseBool(raw)
		if err != nil {
			return Config{}, fmt.Errorf("invalid WRAPPING_BOT_STRIP_ANSI: %q", raw)
		}
	}

	if cfg.MaxLogChunkBytes < 256 || cfg.MaxLogChunkBytes > 1800 {
		return Config{}, errors.New("WRAPPING_BOT_MAX_LOG_CHUNK_BYTES must be between 256 and 1800")
	}
	if cfg.QueueSize < 1 || cfg.MaxConcurrentRuns < 1 || cfg.MaxStreamBytes < 1 {
		return Config{}, errors.New("queue size, concurrency, and stream byte limits must be positive")
	}
	return cfg, nil
}

func envOr(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %q", key, raw)
	}
	return value, nil
}

func envInt64(key string, fallback int64) (int64, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %q", key, raw)
	}
	return value, nil
}
