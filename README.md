# Atlassian Admin API Event Log Exporter

This Go application fetches events from the Atlassian Admin API or the Bitbucket workspace audit log, processes them, and logs the results. It supports pagination, rate limiting, and state persistence.

## Features

- Fetches events from the Atlassian Admin API (organisation-level audit log)
- Fetches events from the Bitbucket Cloud workspace audit log
- Fetches audit records from Jira Cloud
- Fetches audit records from Confluence Cloud
- Supports custom date ranges for event retrieval
- Handles API rate limiting
- Logs events to console and optionally to a file
- Sends events to Graylog via GELF (UDP or TCP) for centralised log management
- Sends events to Fluent Bit via HTTP for flexible log pipeline routing
- Persists the last processed event date to resume from where it left off
- Configurable via command-line flags, environment variables, or a YAML configuration file

## Prerequisites

- Go 1.25 or higher
- **Admin source**: Atlassian Admin API Token and Organisation ID
- **Bitbucket source**: Bitbucket username, app password, and workspace slug
- **Jira source**: Jira site URL, Atlassian account email, and personal API token
- **Confluence source**: Confluence site URL, Atlassian account email, and personal API token

## Docker

Pre-built images are published to the GitHub Container Registry on every push to `main` and on every `v*` tag:

```sh
docker pull ghcr.io/tomaskovacik/atlassian_log_exporter:latest
```

Run with environment variables:

```sh
docker run --rm \
  -e ATLASSIAN_ADMIN_API_TOKEN=your_token \
  -e ATLASSIAN_ORGID=your_org_id \
  ghcr.io/tomaskovacik/atlassian_log_exporter:latest
```

Mount a config file and a state directory for persistent state:

```sh
docker run --rm \
  -v "$PWD/config.yaml:/config.yaml" \
  -v "$PWD/state:/state" \
  -w /state \
  ghcr.io/tomaskovacik/atlassian_log_exporter:latest -config /config.yaml
```

Use Docker Compose for one-shot Jira or Confluence runs with persistent state volumes:

```sh
cp config.yaml.example config.yaml
cp .env.example .env
# put your main settings in config.yaml
# use .env only for optional overrides / secrets you do not want in config.yaml

docker compose run --rm jira-log-exporter
docker compose run --rm confluence-log-exporter
```

The Compose file mounts `./config.yaml` and starts the exporter with `-config /config.yaml` by default. If you want to use a different host-side file, set `CONFIG_FILE=/path/to/config.yaml` when invoking `docker compose`. Environment variables from `.env` are still passed through, so they can fill in missing values from the config file or override secrets.

The Compose file is intended for ad-hoc or externally scheduled job runs, not `docker compose up`. For recurring exports, schedule `docker compose run --rm ...` from host cron, a systemd timer, or another external scheduler.

To build the image locally:

```sh
docker build -t atlassian_log_exporter .
```

## Installation

1. Clone the repository:

   ```sh
   git clone https://github.com/tomaskovacik/atlassian_log_exporter.git
   ```

2. Navigate to the project directory:

   ```sh
   cd atlassian_log_exporter
   ```

3. Install dependencies:

   ```sh
   go mod tidy
   ```

4. Build:

   ```sh
   go build
   ```

## Usage

Run the application with the following command:

```sh
./atlassian_log_exporter --help
```

### Flags

#### Common flags

- `-config`: (Optional) Path to a YAML configuration file
- `-source`: Log source — `admin` (default), `bitbucket`, `jira`, or `confluence`
- `-api_user_agent`: API User Agent (default `"curl/7.54.0"`)
- `-from`: (Optional) From date in RFC3339 format; overrides saved state
- `-log-to-file`: (Optional) Enable logging to file
- `-log-file`: (Optional) Path to log file (default `"log.txt"`)
- `-debug`: Enable debug mode
- `-query`: Query string to filter events
- `-sleep`: Sleep time in milliseconds between paginated requests (default `200`)
- `-gelf-enabled`: Enable GELF output to Graylog
- `-gelf-host`: Graylog server hostname or IP (env: `GELF_HOST`)
- `-gelf-port`: Graylog GELF input port (default `12201`)
- `-gelf-protocol`: Transport protocol `udp` (default) or `tcp`
- `-fluentbit-enabled`: Enable Fluent Bit HTTP output
- `-fluentbit-host`: Fluent Bit HTTP input hostname (env: `FLUENTBIT_HOST`)
- `-fluentbit-port`: Fluent Bit HTTP input port (default `9880`)
- `-fluentbit-tag`: Tag / URL path segment appended to the endpoint (env: `FLUENTBIT_TAG`; defaults to the source name)

#### Admin source flags

- `-api_token`: Atlassian Admin API Token (env: `ATLASSIAN_ADMIN_API_TOKEN`)
- `-org_id`: Organisation ID (env: `ATLASSIAN_ORGID`)

#### Bitbucket source flags

- `-workspace`: Bitbucket workspace slug (env: `BITBUCKET_WORKSPACE`)
- `-bb-username`: Bitbucket username for basic auth (env: `BITBUCKET_USERNAME`)
- `-bb-app-password`: Bitbucket app password for basic auth (env: `BITBUCKET_APP_PASSWORD`)

#### Jira source flags

- `-jira-url`: Jira site URL, e.g. `https://your-org.atlassian.net` (env: `JIRA_URL`)
- `-jira-email`: Jira account email for basic auth (env: `JIRA_EMAIL`; falls back to `-atlassian-email` / `ATLASSIAN_EMAIL`)
- `-jira-token`: Jira personal API token for basic auth (env: `JIRA_TOKEN`; falls back to `-atlassian-token` / `ATLASSIAN_TOKEN`)

#### Confluence source flags

- `-confluence-url`: Confluence site URL, e.g. `https://your-org.atlassian.net/wiki` (env: `CONFLUENCE_URL`)
- `-confluence-email`: Confluence account email for basic auth (env: `CONFLUENCE_EMAIL`; falls back to `-atlassian-email` / `ATLASSIAN_EMAIL`)
- `-confluence-token`: Confluence personal API token for basic auth (env: `CONFLUENCE_TOKEN`; falls back to `-atlassian-token` / `ATLASSIAN_TOKEN`)

#### Shared Jira / Confluence fallback flags

- `-atlassian-email`: Shared fallback email for Jira and Confluence basic auth (env: `ATLASSIAN_EMAIL`)
- `-atlassian-token`: Shared fallback token for Jira and Confluence basic auth (env: `ATLASSIAN_TOKEN`)

### Environment Variables

| Variable                    | Description                              |
|-----------------------------|------------------------------------------|
| `ATLASSIAN_ADMIN_API_TOKEN` | Atlassian Admin API Token (admin source) |
| `ATLASSIAN_ORGID`           | Atlassian Organisation ID (admin source) |
| `BITBUCKET_WORKSPACE`       | Bitbucket workspace slug                 |
| `BITBUCKET_USERNAME`        | Bitbucket username                       |
| `BITBUCKET_APP_PASSWORD`    | Bitbucket app password                   |
| `JIRA_URL`                  | Jira site URL (jira source)              |
| `JIRA_EMAIL`                | Jira account email (preferred override)  |
| `JIRA_TOKEN`                | Jira personal API token (preferred override) |
| `CONFLUENCE_URL`            | Confluence site URL (confluence source)  |
| `CONFLUENCE_EMAIL`          | Confluence account email (preferred override) |
| `CONFLUENCE_TOKEN`          | Confluence personal API token (preferred override) |
| `ATLASSIAN_EMAIL`           | Shared fallback email for Jira/Confluence |
| `ATLASSIAN_TOKEN`           | Shared fallback token for Jira/Confluence |
| `GELF_HOST`                 | Graylog GELF server hostname or IP       |
| `FLUENTBIT_HOST`            | Fluent Bit HTTP input hostname           |
| `FLUENTBIT_TAG`             | Fluent Bit tag / URL path segment        |

## Required Token Scopes / Permissions

| Source | Auth used by exporter | Audit access needed | Extra lookup access used for name enrichment | Account / role requirement |
|--------|------------------------|---------------------|----------------------------------------------|----------------------------|
| `admin` | Bearer token from Atlassian Admin API key | `read:events:admin` | `GET /users/{account_id}/manage/profile` is also called to resolve display names. Atlassian does not publish an OAuth2 scope for this endpoint; their docs state OAuth2 apps cannot access it directly. | **Organisation Admin** in Atlassian Admin. In practice this also requires [Atlassian Guard](https://www.atlassian.com/software/access) because the organisation API key is created in `admin.atlassian.com`. |
| `bitbucket` | Bitbucket username + app password | App password permission: `Workspace -> Audit logs: Read` | None | Access to the target Bitbucket workspace and an app password with audit-log read permission |
| `jira` | Basic Auth: Jira-specific email/token, or shared Atlassian fallback | Audit API scope: `read:audit-log:jira` | Name enrichment calls `GET /rest/api/3/user/bulk/migration` and `GET /rest/api/2/user?accountId=...`. In current user testing against the new `api.atlassian.com/ex/jira/<cloud_id>/...` gateway, the working scope set was `read:jira-user`, `read:user:jira`, and `read:group:jira`. | **Jira Administrator** global permission (`Administer Jira`) on the target site |
| `confluence` | Basic Auth: Confluence-specific email/token, or shared Atlassian fallback | `read:audit-log:confluence` | Name enrichment calls `GET /wiki/rest/api/user?accountId=...` and `GET /wiki/rest/api/group/by-id?id=...`. Classic scope: `read:confluence-user`. Granular scopes: `read:user:confluence`, `read:group:confluence`. | **Confluence Administrator** or **System Administrator** global permission on the target site |

> **Note**
>
> The current exporter implementation uses **Basic Auth** for Jira and Confluence, not OAuth Bearer tokens. The scope names above are the documented equivalents for those REST endpoints, but in day-to-day use the decisive factor is usually whether the Atlassian account behind the token has the required site permissions.

## Scoped API tokens for Jira and Confluence

If you want to switch from classic Atlassian API tokens to **scoped API tokens**, use the Atlassian gateway domain described in Atlassian's support guidance:

- [Scoped API tokens in Confluence Cloud](https://support.atlassian.com/confluence/kb/scoped-api-tokens-in-confluence-cloud/)
- [How to find your Atlassian Cloud site's Cloud ID](https://support.atlassian.com/jira/kb/retrieve-my-atlassian-sites-cloud-id/)

### Why this matters

Classic tokens usually work with site-local URLs such as:

```text
https://your-org.atlassian.net/
https://your-org.atlassian.net/wiki/
```

Scoped tokens do **not**. Atlassian requires the `api.atlassian.com/ex/...` gateway instead:

```text
https://api.atlassian.com/ex/jira/<cloud_id>/
https://api.atlassian.com/ex/confluence/<cloud_id>/wiki/
```

### Recommended configuration

Use **source-specific credentials** for Jira and Confluence when working with scoped tokens. The shared `atlassian_email` / `atlassian_token` fields are still supported as fallbacks, but they are not a good fit when each product uses a different scoped token.

```yaml
jira_url: "https://api.atlassian.com/ex/jira/<cloud_id>/"
jira_email: "user@example.com"
jira_token: "your-jira-scoped-token"

confluence_url: "https://api.atlassian.com/ex/confluence/<cloud_id>/wiki/"
confluence_email: "user@example.com"
confluence_token: "your-confluence-scoped-token"
```

### URL shape hints

These URL shapes are important with the current exporter implementation:

| Source | Recommended base URL | Notes |
|--------|-----------------------|-------|
| Jira | `https://api.atlassian.com/ex/jira/<cloud_id>/` | Keep the trailing slash. Jira audit fetch uses relative paths from this base. |
| Confluence | `https://api.atlassian.com/ex/confluence/<cloud_id>/wiki/` | Include both `/wiki/` and the trailing slash. Confluence audit and lookup calls depend on it. |

### Scopes for the new `api.atlassian.com/ex/...` endpoints

Use the following scope sets when switching Jira and Confluence to scoped API tokens.

| Source | Endpoint family | Scopes |
|--------|-----------------|--------|
| Jira | `https://api.atlassian.com/ex/jira/<cloud_id>/rest/api/2/auditing/record` | `read:audit-log:jira` |
| Jira | `https://api.atlassian.com/ex/jira/<cloud_id>/rest/api/3/user/bulk/migration` and `.../rest/api/2/user` | **Working set confirmed in user testing:** `read:jira-user`, `read:user:jira`, `read:group:jira` |
| Confluence | `https://api.atlassian.com/ex/confluence/<cloud_id>/wiki/rest/api/audit` | `read:audit-log:confluence` |
| Confluence | `https://api.atlassian.com/ex/confluence/<cloud_id>/wiki/rest/api/user` and `.../wiki/rest/api/group/by-id` | Classic: `read:confluence-user`. Granular: `read:user:confluence`, `read:group:confluence` |

Notes:

1. Jira lookup currently appears to work reliably with the mixed classic + granular scope set above.
2. For the full Confluence granular scope set used by audit + lookup, start with `read:audit-log:confluence`, `read:user:confluence`, and `read:group:confluence`.

### Current implementation caveats

1. `admin` and `bitbucket` do **not** use the `api.atlassian.com/ex/...` gateway.
2. Confluence scoped-token audit fetch is supported, and pagination is normalized so Atlassian's root-relative `next` links continue to work under the gateway base.
3. Jira audit fetch uses offset pagination, so it does not have the same `next`-link issue as Confluence.
4. The exporter still authenticates Jira and Confluence with **Basic Auth** (`email + token`). The difference for scoped tokens is mainly the **base URL** and the need for **separate product-specific tokens**.

## Configuration File

All settings can be provided via a YAML file instead of (or in addition to) CLI flags and environment variables. The configuration is applied in the following priority order (highest wins):

1. **CLI flags** (highest priority)
2. **YAML config file**
3. **Environment variables**
4. **Built-in defaults** (lowest priority)

Copy `config.yaml.example` to `config.yaml`, fill in the required values, and pass it with `-config`:

```sh
./atlassian_log_exporter -config config.yaml
```

CLI flags override anything set in the file:

```sh
./atlassian_log_exporter -config config.yaml -debug
```

### YAML keys

| Key               | CLI flag             | Environment variable        | Description |
|-------------------|----------------------|-----------------------------|-------------|
| `source`          | `-source`            | —                           | Log source (`admin`, `bitbucket`, `jira`, `confluence`) |
| `api_user_agent`  | `-api_user_agent`    | —                           | HTTP User-Agent header |
| `from`            | `-from`              | —                           | Start date (RFC3339) |
| `sleep`           | `-sleep`             | —                           | Sleep (ms) between requests |
| `debug`           | `-debug`             | —                           | Enable debug logging |
| `query`           | `-query`             | —                           | Filter query |
| `log_to_file`     | `-log-to-file`       | —                           | Write logs to file |
| `log_file`        | `-log-file`          | —                           | Log file path |
| `api_token`       | `-api_token`         | `ATLASSIAN_ADMIN_API_TOKEN` | Admin API token |
| `org_id`          | `-org_id`            | `ATLASSIAN_ORGID`           | Atlassian organisation ID |
| `workspace`       | `-workspace`         | `BITBUCKET_WORKSPACE`       | Bitbucket workspace slug |
| `bb_username`     | `-bb-username`       | `BITBUCKET_USERNAME`        | Bitbucket username |
| `bb_app_password` | `-bb-app-password`   | `BITBUCKET_APP_PASSWORD`    | Bitbucket app password |
| `jira_url`        | `-jira-url`          | `JIRA_URL`                  | Jira site URL |
| `jira_email`      | `-jira-email`        | `JIRA_EMAIL`                | Jira account email |
| `jira_token`      | `-jira-token`        | `JIRA_TOKEN`                | Jira personal API token |
| `confluence_url`  | `-confluence-url`    | `CONFLUENCE_URL`            | Confluence site URL |
| `confluence_email`| `-confluence-email`  | `CONFLUENCE_EMAIL`          | Confluence account email |
| `confluence_token`| `-confluence-token`  | `CONFLUENCE_TOKEN`          | Confluence personal API token |
| `atlassian_email` | `-atlassian-email`   | `ATLASSIAN_EMAIL`           | Shared fallback email for Jira / Confluence |
| `atlassian_token` | `-atlassian-token`   | `ATLASSIAN_TOKEN`           | Shared fallback token for Jira / Confluence |
| `gelf_enabled`        | `-gelf-enabled`          | —                           | Enable GELF output to Graylog |
| `gelf_host`           | `-gelf-host`             | `GELF_HOST`                 | Graylog GELF server hostname or IP |
| `gelf_port`           | `-gelf-port`             | —                           | Graylog GELF port (default 12201) |
| `gelf_protocol`       | `-gelf-protocol`         | —                           | GELF transport: `udp` (default) or `tcp` |
| `fluentbit_enabled`   | `-fluentbit-enabled`     | —                           | Enable Fluent Bit HTTP output |
| `fluentbit_host`      | `-fluentbit-host`        | `FLUENTBIT_HOST`            | Fluent Bit HTTP input hostname |
| `fluentbit_port`      | `-fluentbit-port`        | —                           | Fluent Bit HTTP input port (default 9880) |
| `fluentbit_tag`       | `-fluentbit-tag`         | `FLUENTBIT_TAG`             | Tag / URL path segment (defaults to source name) |

## Examples

### Atlassian Admin API (default)

```sh
./atlassian_log_exporter -api_token=your_api_token -org_id=your_org_id -from=2023-09-01T00:00:00Z -log-to-file -debug
```

or using environment variables:

```sh
ATLASSIAN_ADMIN_API_TOKEN=123 ATLASSIAN_ORGID=123-123-123 ./atlassian_log_exporter
```

### Bitbucket workspace audit log

```sh
./atlassian_log_exporter \
  -source=bitbucket \
  -workspace=my-workspace \
  -bb-username=my-user \
  -bb-app-password=my-app-password \
  -from=2023-09-01T00:00:00Z \
  -debug
```

or using environment variables:

```sh
BITBUCKET_WORKSPACE=my-workspace \
BITBUCKET_USERNAME=my-user \
BITBUCKET_APP_PASSWORD=my-app-password \
./atlassian_log_exporter -source=bitbucket
```

### Jira Cloud audit records

`ug:`-prefixed author keys are resolved to display names automatically using the Jira REST API — no extra flags or Atlassian Guard subscription required.

```sh
./atlassian_log_exporter \
  -source=jira \
  -jira-url=https://your-org.atlassian.net \
  -jira-email=user@example.com \
  -jira-token=your-api-token \
  -from=2023-09-01T00:00:00Z \
  -debug
```

or using environment variables:

```sh
JIRA_URL=https://your-org.atlassian.net \
JIRA_EMAIL=user@example.com \
JIRA_TOKEN=your-api-token \
./atlassian_log_exporter -source=jira
```

### Confluence Cloud audit records

```sh
./atlassian_log_exporter \
  -source=confluence \
  -confluence-url=https://your-org.atlassian.net/wiki \
  -confluence-email=user@example.com \
  -confluence-token=your-api-token \
  -from=2023-09-01T00:00:00Z \
  -debug
```

or using environment variables:

```sh
CONFLUENCE_URL=https://your-org.atlassian.net/wiki \
CONFLUENCE_EMAIL=user@example.com \
CONFLUENCE_TOKEN=your-api-token \
./atlassian_log_exporter -source=confluence
```

## State Persistence

The application saves the timestamp of the last processed event so it can resume from where it left off in subsequent runs:

- **Admin source**: `atlassian_state.json`
- **Bitbucket source**: `bitbucket_state.json`
- **Jira source**: `jira_state.json`
- **Confluence source**: `confluence_state.json`

## Error Handling

The application handles various error scenarios, including API rate limiting. When the rate limit is exceeded, it will wait for the specified time before retrying.

## Logging

Logs are output to the console by default. If the `-log-to-file` flag is set, logs will also be written to the specified file (default: `log.txt`).

## Graylog GELF output

When `-gelf-enabled` is set, every audit event is forwarded to a Graylog server as a structured [GELF 1.1](https://go2docs.graylog.org/current/getting_in_log_data/gelf.html) message over UDP (default) or TCP. Each message carries the event fields as GELF additional fields (prefixed with `_`) so they appear as dedicated columns in Graylog.

```sh
./atlassian_log_exporter \
  -source=admin \
  -api_token=your_token \
  -org_id=your_org \
  -gelf-enabled \
  -gelf-host=graylog.example.com \
  -gelf-port=12201 \
  -gelf-protocol=udp
```

Or via YAML:

```yaml
gelf_enabled: true
gelf_host: graylog.example.com
gelf_port: 12201
gelf_protocol: udp
```

## Fluent Bit HTTP output

When `-fluentbit-enabled` is set, every audit event is forwarded to a [Fluent Bit HTTP input plugin](https://docs.fluentbit.io/manual/pipeline/inputs/http) as a JSON object via HTTP POST. The Fluent Bit tag (used as the URL path segment) defaults to the source name (`admin`, `bitbucket`, `jira`, or `confluence`) and can be overridden with `-fluentbit-tag`. GELF-style leading `_` prefixes are stripped from field names so that Fluent Bit records have clean keys (e.g. `source`, `action`, `actor_name`).

```sh
./atlassian_log_exporter \
  -source=admin \
  -api_token=your_token \
  -org_id=your_org \
  -fluentbit-enabled \
  -fluentbit-host=fluentbit.example.com \
  -fluentbit-port=9880 \
  -fluentbit-tag=atlassian-audit
```

Or via YAML:

```yaml
fluentbit_enabled: true
fluentbit_host: fluentbit.example.com
fluentbit_port: 9880
fluentbit_tag: atlassian-audit   # optional; defaults to source name
```

Both GELF and Fluent Bit outputs can be enabled at the same time.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

Apache License Version 2.0, January 2004
