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
- Persists the last processed event date to resume from where it left off
- Configurable via command-line flags, environment variables, or a YAML configuration file

## Prerequisites

- Go 1.25 or higher
- **Admin source**: Atlassian Admin API Token and Organisation ID
- **Bitbucket source**: Bitbucket username, app password, and workspace slug
- **Jira source**: Jira site URL, Atlassian account email, and personal API token
- **Confluence source**: Confluence site URL, Atlassian account email, and personal API token

> **ℹ️ Forked dependency**
>
> This project uses a [custom fork of go-atlassian](https://github.com/tomaskovacik/go-atlassian) (`github.com/tomaskovacik/go-atlassian/v2 v2.11.1-kovo`) that adds `AuthorAccountID` to `AuditRecordScheme`. The `replace` directive in `go.mod` handles this automatically — no manual steps are required beyond the standard `go mod tidy`.

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

To build the image locally:

```sh
docker build -t atlassian_log_exporter .
```

## Installation

1. Clone the repository:

   ```sh
   git clone https://github.com/m1keru/atlassian_log_exporter.git
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

#### Admin source flags

- `-api_token`: Atlassian Admin API Token (env: `ATLASSIAN_ADMIN_API_TOKEN`)
- `-org_id`: Organisation ID (env: `ATLASSIAN_ORGID`)

#### Bitbucket source flags

- `-workspace`: Bitbucket workspace slug (env: `BITBUCKET_WORKSPACE`)
- `-bb-username`: Bitbucket username for basic auth (env: `BITBUCKET_USERNAME`)
- `-bb-app-password`: Bitbucket app password for basic auth (env: `BITBUCKET_APP_PASSWORD`)

#### Jira source flags

- `-jira-url`: Jira site URL, e.g. `https://your-org.atlassian.net` (env: `JIRA_URL`)
- `-atlassian-email`: Atlassian account email for basic auth (env: `ATLASSIAN_EMAIL`)
- `-atlassian-token`: Atlassian personal API token for basic auth (env: `ATLASSIAN_TOKEN`)

#### Confluence source flags

- `-confluence-url`: Confluence site URL, e.g. `https://your-org.atlassian.net/wiki` (env: `CONFLUENCE_URL`)
- `-atlassian-email`: Atlassian account email for basic auth (env: `ATLASSIAN_EMAIL`)
- `-atlassian-token`: Atlassian personal API token for basic auth (env: `ATLASSIAN_TOKEN`)

### Environment Variables

| Variable                    | Description                              |
|-----------------------------|------------------------------------------|
| `ATLASSIAN_ADMIN_API_TOKEN` | Atlassian Admin API Token (admin source) |
| `ATLASSIAN_ORGID`           | Atlassian Organisation ID (admin source) |
| `BITBUCKET_WORKSPACE`       | Bitbucket workspace slug                 |
| `BITBUCKET_USERNAME`        | Bitbucket username                       |
| `BITBUCKET_APP_PASSWORD`    | Bitbucket app password                   |
| `JIRA_URL`                  | Jira site URL (jira source)              |
| `CONFLUENCE_URL`            | Confluence site URL (confluence source)  |
| `ATLASSIAN_EMAIL`           | Atlassian account email (jira/confluence)|
| `ATLASSIAN_TOKEN`           | Atlassian personal API token (jira/confluence)|
| `GELF_HOST`                 | Graylog GELF server hostname or IP       |

## Required Token Scopes / Permissions

### Admin source — Atlassian Admin API Token

The API token must be a **service account API key** created in [Atlassian Admin](https://admin.atlassian.com) with the following OAuth scope granted:

| Scope | Description |
|-------|-------------|
| `read:audit-log:admin` | Read organisation-level audit log events |

The token owner must have the **Organisation Admin** role in the Atlassian organisation.

### Bitbucket source — App Password

The Bitbucket [App Password](https://bitbucket.org/account/settings/app-passwords/) must have the following permission enabled:

| Permission category | Required permission |
|---------------------|---------------------|
| Workspace | `Audit logs: Read` |

### Jira source — Personal API Token

Jira uses Basic Auth (email + [personal API token](https://id.atlassian.com/manage-profile/security/api-tokens)). The Atlassian account associated with the token must have the **Jira Administrator** global permission (`Administer Jira`) on the target site, as the audit log API is restricted to site administrators.

#### Author display name resolution for `ug:`-prefixed author keys

Jira Cloud audit records sometimes carry an `AuthorKey` in `ug:UUID` format (a raw Atlassian account ID). The exporter automatically resolves these IDs to display names using the Jira REST API (`GET {jiraURL}/rest/api/2/user?accountId=...`) with the same Basic Auth credentials already required for audit log access. The resolved name is emitted as `_author_display_name` in log output and GELF fields.

> **ℹ️ No Atlassian Guard subscription required**
>
> Unlike the **admin source** (organisation-level audit log), the Jira source uses only the standard Jira REST API for both audit record retrieval and author name resolution. A regular Jira Administrator personal API token is sufficient — no [Atlassian Guard](https://www.atlassian.com/software/access) (formerly Atlassian Access) subscription is needed.
>
> The admin source *does* require Guard: it authenticates with an organisation-level API key from [admin.atlassian.com](https://admin.atlassian.com), which can only be created when Guard is active on your Atlassian organisation.

### Confluence source — Personal API Token

Confluence uses Basic Auth (email + [personal API token](https://id.atlassian.com/manage-profile/security/api-tokens)). The Atlassian account associated with the token must have the **Confluence Administrator** (or **System Administrator**) global permission on the target site, as the audit log API is restricted to site administrators.

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
| `confluence_url`  | `-confluence-url`    | `CONFLUENCE_URL`            | Confluence site URL |
| `atlassian_email` | `-atlassian-email`   | `ATLASSIAN_EMAIL`           | Atlassian account email |
| `atlassian_token` | `-atlassian-token`   | `ATLASSIAN_TOKEN`           | Atlassian personal API token |
| `gelf_enabled`    | `-gelf-enabled`      | —                           | Enable GELF output to Graylog |
| `gelf_host`       | `-gelf-host`         | `GELF_HOST`                 | Graylog GELF server hostname or IP |
| `gelf_port`       | `-gelf-port`         | —                           | Graylog GELF port (default 12201) |
| `gelf_protocol`   | `-gelf-protocol`     | —                           | GELF transport: `udp` (default) or `tcp` |

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
  -atlassian-email=user@example.com \
  -atlassian-token=your-api-token \
  -from=2023-09-01T00:00:00Z \
  -debug
```

or using environment variables:

```sh
JIRA_URL=https://your-org.atlassian.net \
ATLASSIAN_EMAIL=user@example.com \
ATLASSIAN_TOKEN=your-api-token \
./atlassian_log_exporter -source=jira
```

### Confluence Cloud audit records

```sh
./atlassian_log_exporter \
  -source=confluence \
  -confluence-url=https://your-org.atlassian.net/wiki \
  -atlassian-email=user@example.com \
  -atlassian-token=your-api-token \
  -from=2023-09-01T00:00:00Z \
  -debug
```

or using environment variables:

```sh
CONFLUENCE_URL=https://your-org.atlassian.net/wiki \
ATLASSIAN_EMAIL=user@example.com \
ATLASSIAN_TOKEN=your-api-token \
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

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

Apache License Version 2.0, January 2004

