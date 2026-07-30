# wrapping-bot

`wrapping-bot` executes a local command while asynchronously mirroring its stdout and stderr to a Discord channel through a remote relay daemon.

The Discord bot token remains only on the daemon host. Experiment machines receive only a separate relay token and a target alias such as `default` or `peerkit`.

## Architecture

```text
Experiment host                                  Relay host / Docker

./wrapping-bot -- ./exp run
        │
        ├─ stdout/stderr ──> local terminal
        │
        └─ authenticated NDJSON stream ───────> wrapping-botd
                                                   │
                                                   ├─ target alias lookup
                                                   ├─ batching / ANSI removal
                                                   ├─ Discord rate-limit handling
                                                   └─ Discord channel messages
```

Two binaries are included:

- `wrapping-bot`: local command wrapper used on experiment machines.
- `wrapping-botd`: remote relay daemon intended to run as a Docker service.

## Behavior

- Mirrors both stdout and stderr locally and remotely.
- Sends a structured start message with command, host, working directory, and allowlisted environment values.
- Batches log output before sending it to Discord.
- Sends a final message containing exit code, elapsed time, and dropped-byte counters.
- Uses bounded asynchronous queues so Discord latency does not block the experiment process.
- Removes ANSI terminal escape sequences by default.
- Disables Discord mention parsing and suppresses notifications for log messages.
- Routes clients through daemon-defined target aliases instead of accepting arbitrary Discord channel IDs.

## 1. Create and invite the Discord bot

Create a Discord application and bot, then invite it to the target server. The bot needs access to the selected text channel and permission to send messages there.

This service only sends messages through Discord's HTTP API. It does not read server messages and does not require a Gateway connection.

Copy the target channel ID from Discord developer mode.

## 2. Configure the Docker relay daemon

```sh
cp .env.example .env
```

Edit `.env`:

```dotenv
DISCORD_BOT_TOKEN=your-discord-bot-token
WRAPPING_BOT_SHARED_TOKEN=a-long-random-token
WRAPPING_BOT_CHANNELS={"default":"123456789012345678","peerkit":"234567890123456789"}
```

Generate a relay token with:

```sh
openssl rand -hex 32
```

Start the service:

```sh
docker compose up -d --build
```

Check it:

```sh
curl http://127.0.0.1:8080/healthz
docker compose ps
docker compose logs -f wrapping-botd
```

## 3. Build and install the client

```sh
make build-client
sudo ./scripts/install-client.sh
```

Configure the experiment host:

```sh
cp client.env.example client.env
set -a
. ./client.env
set +a
```

The daemon's `WRAPPING_BOT_SHARED_TOKEN` and the client's `WRAPPING_BOT_TOKEN` must match.

## 4. Wrap a command

Direct argument-vector execution is preferred:

```sh
wrapping-bot -- ./exp run
```

The quoted form requested in the original design is also supported and executes through `/bin/sh -lc`:

```sh
wrapping-bot "./exp run"
```

Select another configured channel target:

```sh
wrapping-bot --target peerkit --name "ER N=1000" -- ./exp run
```

The wrapper exits with the wrapped process's exit code. A relay failure is printed to local stderr but does not replace the experiment's exit code.

## Environment metadata

The wrapper never sends the complete process environment by default. It only selects explicitly configured keys and prefixes.

Default selection when no configuration is supplied:

```text
Keys:     RUN_ID, TOPOLOGY, PROTOCOL, NODES, SEED
Prefixes: EXP_, PEERKIT_
```

Override them with environment variables:

```dotenv
WRAPPING_BOT_ENV_KEYS=RUN_ID,TOPOLOGY,GRAPH_TYPE,NODE_COUNT,SEED
WRAPPING_BOT_ENV_PREFIXES=EXPERIMENT_,PEERKIT_
```

Or add keys per invocation:

```sh
wrapping-bot --env DATASET --env-prefix TRIAL_ -- ./exp run
```

Names resembling tokens, passwords, credentials, cookies, authorization data, or private keys are rejected even when they match a prefix. `--allow-secret-env` exists for exceptional cases, but should normally remain disabled.

## Channel routing

The client sends a target alias, not a Discord channel ID:

```dotenv
WRAPPING_BOT_TARGET=peerkit
```

The daemon resolves that alias using:

```dotenv
WRAPPING_BOT_CHANNELS={"default":"123...","peerkit":"234..."}
```

This keeps channel authorization under daemon control and prevents a compromised experiment host from selecting arbitrary channels.

## Important configuration

### Daemon

| Variable | Default | Purpose |
|---|---:|---|
| `DISCORD_BOT_TOKEN` | required | Discord bot credential |
| `WRAPPING_BOT_SHARED_TOKEN` | required | Client-to-daemon bearer token |
| `WRAPPING_BOT_CHANNELS` | required | JSON alias-to-channel map |
| `WRAPPING_BOT_LISTEN_ADDR` | `:8080` | HTTP listen address |
| `WRAPPING_BOT_FLUSH_INTERVAL` | `1500ms` | Maximum batching delay |
| `WRAPPING_BOT_MAX_LOG_CHUNK_BYTES` | `1600` | Log bytes placed in one Discord message body |
| `WRAPPING_BOT_QUEUE_SIZE` | `1024` | Per-run daemon output queue |
| `WRAPPING_BOT_MAX_CONCURRENT_RUNS` | `32` | Concurrent stream limit |
| `WRAPPING_BOT_MAX_STREAM_BYTES` | `1 GiB` | Maximum request stream size |
| `WRAPPING_BOT_STRIP_ANSI` | `true` | Remove terminal color/control sequences |

### Client

| Variable | Default | Purpose |
|---|---:|---|
| `WRAPPING_BOT_ENDPOINT` | `http://127.0.0.1:8080` | Relay base URL |
| `WRAPPING_BOT_TOKEN` | required | Shared relay credential |
| `WRAPPING_BOT_TARGET` | `default` | Daemon channel alias |
| `WRAPPING_BOT_RUN_NAME` | empty | Discord display label |
| `WRAPPING_BOT_ENV_KEYS` | built-in list | Comma-separated environment keys |
| `WRAPPING_BOT_ENV_PREFIXES` | built-in list | Comma-separated prefixes |
| `WRAPPING_BOT_CLIENT_QUEUE_SIZE` | `1024` | Local async event queue |
| `WRAPPING_BOT_FINISH_TIMEOUT` | `15s` | Wait for final relay acknowledgement |

## Overload and failure semantics

The experiment process must not be coupled to Discord throughput. Therefore:

1. The client writes logs into a bounded, non-blocking queue.
2. The daemon reads the network stream into another bounded queue.
3. Discord sends are serialized and honor rate-limit responses.
4. When either queue is full, output is omitted instead of blocking the experiment.
5. The omitted byte count is reported in Discord and/or local stderr.

This design provides live observability, not lossless archival. Keep the experiment's normal file logs as the authoritative record.

## Security

- Never distribute `DISCORD_BOT_TOKEN` to experiment hosts.
- Use a long random `WRAPPING_BOT_SHARED_TOKEN`.
- Put the daemon behind TLS or a private network when crossing untrusted networks.
- Restrict the published Docker port with host firewall rules.
- Rotate the shared token if an experiment host is compromised.
- Keep environment collection allowlisted; do not enable secret environment forwarding globally.

## Development

```sh
make test
make vet
make build
```

The streaming wire format is newline-delimited JSON with `start`, `output`, and `exit` events. Protocol structures are defined in `internal/protocol/event.go`.
