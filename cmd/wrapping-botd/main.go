package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/k-p2plab/wrapping-bot/internal/daemon"
	discordclient "github.com/k-p2plab/wrapping-bot/internal/discord"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		if err := healthcheck(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if err := serve(); err != nil {
		fmt.Fprintln(os.Stderr, "wrapping-botd:", err)
		os.Exit(1)
	}
}

func serve() error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg, err := daemon.LoadConfigFromEnv()
	if err != nil {
		return err
	}
	sender, err := discordclient.NewSender(cfg.DiscordBotToken, cfg.DiscordAPIBaseURL)
	if err != nil {
		return fmt.Errorf("initialize Discord client: %w", err)
	}

	server := daemon.NewServer(cfg, sender, logger).HTTPServer()
	errCh := make(chan error, 1)
	go func() {
		logger.Info("wrapping-bot daemon listening", "address", cfg.ListenAddr, "channel_selection", "client", "allowed_channel_count", len(cfg.AllowedChannelIDs))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-signalCh:
		logger.Info("shutdown signal received", "signal", sig.String())
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(ctx)
	case err := <-errCh:
		return err
	}
}

func healthcheck() error {
	url := strings.TrimSpace(os.Getenv("WRAPPING_BOT_HEALTHCHECK_URL"))
	if url == "" {
		url = "http://127.0.0.1:8080/healthz"
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("healthcheck returned %s", resp.Status)
	}
	var payload struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return err
	}
	if payload.Status != "ok" {
		return fmt.Errorf("unexpected health status %q", payload.Status)
	}
	return nil
}
