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
- Persists the last processed event date to resume from where it left off
- Configurable via command-line flags and environment variables

## Prerequisites

- Go 1.x or higher
- **Admin source**: Atlassian Admin API Token and Organisation ID
- **Bitbucket source**: Bitbucket username, app password, and workspace slug
- **Jira source**: Jira site URL, Atlassian account email, and personal API token
- **Confluence source**: Confluence site URL, Atlassian account email, and personal API token

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

- `-source`: Log source — `admin` (default), `bitbucket`, `jira`, or `confluence`
- `-api_user_agent`: API User Agent (default `"curl/7.54.0"`)
- `-from`: (Optional) From date in RFC3339 format; overrides saved state
- `-log-to-file`: (Optional) Enable logging to file
- `-log-file`: (Optional) Path to log file (default `"log.txt"`)
- `-debug`: Enable debug mode
- `-query`: Query string to filter events
- `-sleep`: Sleep time in milliseconds between paginated requests (default `200`)

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

### Confluence source — Personal API Token

Confluence uses Basic Auth (email + [personal API token](https://id.atlassian.com/manage-profile/security/api-tokens)). The Atlassian account associated with the token must have the **Confluence Administrator** (or **System Administrator**) global permission on the target site, as the audit log API is restricted to site administrators.

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

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

Apache License Version 2.0, January 2004

