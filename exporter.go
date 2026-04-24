package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ctreminiom/go-atlassian/v2/admin"
	"github.com/ctreminiom/go-atlassian/v2/bitbucket"
	"github.com/ctreminiom/go-atlassian/v2/confluence"
	jirav2 "github.com/ctreminiom/go-atlassian/v2/jira/v2"
	"github.com/ctreminiom/go-atlassian/v2/pkg/infra/models"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/Graylog2/go-gelf.v2/gelf"
	"gopkg.in/yaml.v3"
)

type SavedState struct {
	LastEventDate time.Time `json:"last_event_date"`
}

type Config struct {
	APIUserAgent   string
	APIToken       string
	From           string
	OrgID          string
	LogToFile      bool
	LogFilePath    string
	Debug          bool
	Query          string
	Sleep          int
	Source         string
	BBWorkspace    string
	BBUsername     string
	BBAppPassword  string
	JiraURL        string
	ConfluenceURL  string
	AtlassianEmail string
	AtlassianToken string
	GELFEnabled      bool
	GELFHost         string
	GELFPort         int
	GELFProtocol     string
	GELFSourceHost   string
}

// YAMLConfig mirrors Config with yaml struct tags for file-based configuration.
// Pointer types are used for booleans so that an explicit "false" in the YAML
// file can be distinguished from a field that was simply omitted.
type YAMLConfig struct {
	APIUserAgent   string `yaml:"api_user_agent"`
	APIToken       string `yaml:"api_token"`
	From           string `yaml:"from"`
	OrgID          string `yaml:"org_id"`
	LogToFile      *bool  `yaml:"log_to_file"`
	LogFilePath    string `yaml:"log_file"`
	Debug          *bool  `yaml:"debug"`
	Query          string `yaml:"query"`
	Sleep          int    `yaml:"sleep"`
	Source         string `yaml:"source"`
	BBWorkspace    string `yaml:"workspace"`
	BBUsername     string `yaml:"bb_username"`
	BBAppPassword  string `yaml:"bb_app_password"`
	JiraURL        string `yaml:"jira_url"`
	ConfluenceURL  string `yaml:"confluence_url"`
	AtlassianEmail string `yaml:"atlassian_email"`
	AtlassianToken string `yaml:"atlassian_token"`
	GELFEnabled    *bool  `yaml:"gelf_enabled"`
	GELFHost       string `yaml:"gelf_host"`
	GELFPort       int    `yaml:"gelf_port"`
	GELFProtocol   string `yaml:"gelf_protocol"`
	GELFSourceHost string `yaml:"gelf_source_host"`
}

// loadYAMLConfig reads a YAML configuration file and returns a YAMLConfig.
func loadYAMLConfig(path string) (YAMLConfig, error) {
	var cfg YAMLConfig
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	return cfg, yaml.Unmarshal(data, &cfg)
}

// mergeYAMLConfig overlays non-zero fields from override onto base and returns
// the merged result.  String fields are merged when non-empty; int fields when
// non-zero; bool pointer fields when non-nil.
func mergeYAMLConfig(base, override YAMLConfig) YAMLConfig {
	if override.APIUserAgent != "" {
		base.APIUserAgent = override.APIUserAgent
	}
	if override.APIToken != "" {
		base.APIToken = override.APIToken
	}
	if override.From != "" {
		base.From = override.From
	}
	if override.OrgID != "" {
		base.OrgID = override.OrgID
	}
	if override.LogToFile != nil {
		base.LogToFile = override.LogToFile
	}
	if override.LogFilePath != "" {
		base.LogFilePath = override.LogFilePath
	}
	if override.Debug != nil {
		base.Debug = override.Debug
	}
	if override.Query != "" {
		base.Query = override.Query
	}
	if override.Sleep != 0 {
		base.Sleep = override.Sleep
	}
	if override.Source != "" {
		base.Source = override.Source
	}
	if override.BBWorkspace != "" {
		base.BBWorkspace = override.BBWorkspace
	}
	if override.BBUsername != "" {
		base.BBUsername = override.BBUsername
	}
	if override.BBAppPassword != "" {
		base.BBAppPassword = override.BBAppPassword
	}
	if override.JiraURL != "" {
		base.JiraURL = override.JiraURL
	}
	if override.ConfluenceURL != "" {
		base.ConfluenceURL = override.ConfluenceURL
	}
	if override.AtlassianEmail != "" {
		base.AtlassianEmail = override.AtlassianEmail
	}
	if override.AtlassianToken != "" {
		base.AtlassianToken = override.AtlassianToken
	}
	if override.GELFEnabled != nil {
		base.GELFEnabled = override.GELFEnabled
	}
	if override.GELFHost != "" {
		base.GELFHost = override.GELFHost
	}
	if override.GELFPort != 0 {
		base.GELFPort = override.GELFPort
	}
	if override.GELFProtocol != "" {
		base.GELFProtocol = override.GELFProtocol
	}
	if override.GELFSourceHost != "" {
		base.GELFSourceHost = override.GELFSourceHost
	}
	return base
}

// boolVal safely dereferences a *bool, returning false for nil.
func boolVal(b *bool) bool {
	if b == nil {
		return false
	}
	return *b
}

// BitbucketAuditEvent represents a single Bitbucket workspace audit log event.
type BitbucketAuditEvent struct {
	ID      string           `json:"id"`
	Date    string           `json:"date"`
	Actor   BitbucketActor   `json:"actor"`
	Action  string           `json:"action"`
	Subject BitbucketSubject `json:"subject"`
	Context []interface{}    `json:"context"`
}

// BitbucketActor represents the user who performed the audited action.
type BitbucketActor struct {
	Type        string `json:"type"`
	DisplayName string `json:"display_name"`
	UUID        string `json:"uuid"`
	AccountID   string `json:"account_id"`
	Nickname    string `json:"nickname"`
}

// BitbucketSubject represents the target of the audited action.
type BitbucketSubject struct {
	Type        string `json:"type"`
	DisplayName string `json:"display_name"`
	UUID        string `json:"uuid"`
}

// BitbucketAuditPage represents one page of results from the Bitbucket audit log API.
type BitbucketAuditPage struct {
	Pagelen int                   `json:"pagelen"`
	Values  []BitbucketAuditEvent `json:"values"`
	Page    int                   `json:"page"`
	Size    int                   `json:"size"`
	Next    string                `json:"next"`
}

// ConfluenceAuditAuthor represents the author in a Confluence audit record.
type ConfluenceAuditAuthor struct {
	Type        string `json:"type"`
	DisplayName string `json:"displayName"`
	AccountID   string `json:"accountId"`
	AccountType string `json:"accountType"`
	Email       string `json:"email"`
}

// ConfluenceAuditObject represents the affected object in a Confluence audit record.
type ConfluenceAuditObject struct {
	Name       string `json:"name"`
	ObjectType string `json:"objectType"`
}

// ConfluenceAuditRecord represents a single Confluence audit record.
type ConfluenceAuditRecord struct {
	Author         ConfluenceAuditAuthor `json:"author"`
	RemoteAddress  string                `json:"remoteAddress"`
	CreationDate   int64                 `json:"creationDate"`
	Summary        string                `json:"summary"`
	Description    string                `json:"description"`
	Category       string                `json:"category"`
	SysAdmin       bool                  `json:"sysAdmin"`
	AffectedObject ConfluenceAuditObject `json:"affectedObject"`
}

// ConfluenceAuditLinks represents the pagination links in a Confluence audit response.
type ConfluenceAuditLinks struct {
	Next string `json:"next"`
	Self string `json:"self"`
}

// ConfluenceAuditPage represents one page of results from the Confluence audit API.
type ConfluenceAuditPage struct {
	Results []ConfluenceAuditRecord `json:"results"`
	Start   int                     `json:"start"`
	Limit   int                     `json:"limit"`
	Size    int                     `json:"size"`
	Links   ConfluenceAuditLinks    `json:"_links"`
}

func saveState(state SavedState, filename string) error {
	jsonData, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return os.WriteFile(filename, jsonData, 0644)
}

func loadState(filename string) (SavedState, error) {
	var state SavedState
	jsonData, err := os.ReadFile(filename)
	if err != nil {
		return state, err
	}
	return state, json.Unmarshal(jsonData, &state)
}

func initLogger(debug bool, logToFile bool, logFilePath string) *zap.SugaredLogger {
	config := zap.NewProductionEncoderConfig()
	config.EncodeTime = zapcore.ISO8601TimeEncoder

	consoleEncoder := zapcore.NewConsoleEncoder(config)

	level := zap.InfoLevel
	if debug {
		level = zap.DebugLevel
	}

	consoleCore := zapcore.NewCore(consoleEncoder, zapcore.Lock(os.Stdout), level)

	cores := []zapcore.Core{consoleCore}

	if logToFile {
		logFile, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			// Fall back to console-only logging if the file cannot be opened.
			fmt.Fprintf(os.Stderr, "Failed to open log file %s: %v; logging to console only\n", logFilePath, err)
		} else {
			fileEncoder := zapcore.NewJSONEncoder(config)
			fileWriter := zapcore.AddSync(logFile)
			fileCore := zapcore.NewCore(fileEncoder, fileWriter, level)
			cores = append(cores, fileCore)
		}
	}

	core := zapcore.NewTee(cores...)
	logger := zap.New(core, zap.AddCaller())

	return logger.Sugar()
}

// GELFWriter is the interface satisfied by both gelf.UDPWriter and gelf.TCPWriter.
type GELFWriter interface {
	WriteMessage(m *gelf.Message) error
	Close() error
}

// initGELF creates a GELF writer connecting to the configured Graylog host and
// port using the specified protocol (udp or tcp).  Returns nil when GELF is
// disabled.
func initGELF(config Config, log *zap.SugaredLogger) GELFWriter {
	if !config.GELFEnabled {
		return nil
	}

	addr := fmt.Sprintf("%s:%d", config.GELFHost, config.GELFPort)

	switch strings.ToLower(config.GELFProtocol) {
	case "tcp":
		w, err := gelf.NewTCPWriter(addr)
		if err != nil {
			log.Fatalf("Failed to create GELF TCP writer for %s: %v", addr, err)
		}
		log.Infof("GELF TCP output enabled, sending to %s", addr)
		return w
	default:
		w, err := gelf.NewUDPWriter(addr)
		if err != nil {
			log.Fatalf("Failed to create GELF UDP writer for %s: %v", addr, err)
		}
		log.Infof("GELF UDP output enabled, sending to %s", addr)
		return w
	}
}

// sendGELF builds a GELF 1.1 message from the supplied short description,
// timestamp, and extra fields, then writes it to the writer.  A nil writer is
// a no-op so callers do not need to guard against it.
func sendGELF(w GELFWriter, host, short string, ts time.Time, extra map[string]interface{}, log *zap.SugaredLogger) {
	if w == nil {
		return
	}

	m := &gelf.Message{
		Version:  "1.1",
		Host:     host,
		Short:    short,
		TimeUnix: float64(ts.Unix()),
		Level:    6, // informational
		Extra:    extra,
	}

	if err := w.WriteMessage(m); err != nil {
		log.Warnf("Failed to send GELF message: %v", err)
	}
}

func parseFlags() Config {
	// Pre-scan os.Args for -config / --config so we can load the YAML file
	// before registering flag defaults. We do not call flag.Parse() yet.
	configFile := ""
	for i, arg := range os.Args[1:] {
		trimmed := strings.TrimLeft(arg, "-")
		if trimmed == "config" && i+1 < len(os.Args)-1 {
			configFile = os.Args[i+2]
			break
		}
		if strings.HasPrefix(trimmed, "config=") {
			configFile = strings.SplitN(trimmed, "=", 2)[1]
			break
		}
	}

	// Start with env-var defaults, then overlay values from the YAML config
	// file so that explicit CLI flags can still override everything.
	base := YAMLConfig{
		APIUserAgent:   "curl/7.54.0",
		APIToken:       os.Getenv("ATLASSIAN_ADMIN_API_TOKEN"),
		OrgID:          os.Getenv("ATLASSIAN_ORGID"),
		LogFilePath:    "log.txt",
		Sleep:          200,
		Source:         "admin",
		BBWorkspace:    os.Getenv("BITBUCKET_WORKSPACE"),
		BBUsername:     os.Getenv("BITBUCKET_USERNAME"),
		BBAppPassword:  os.Getenv("BITBUCKET_APP_PASSWORD"),
		JiraURL:        os.Getenv("JIRA_URL"),
		ConfluenceURL:  os.Getenv("CONFLUENCE_URL"),
		AtlassianEmail: os.Getenv("ATLASSIAN_EMAIL"),
		AtlassianToken: os.Getenv("ATLASSIAN_TOKEN"),
		GELFHost:       os.Getenv("GELF_HOST"),
		GELFSourceHost: os.Getenv("GELF_SOURCE_HOST"),
		GELFProtocol:   "udp",
	}

	if configFile != "" {
		fileCfg, err := loadYAMLConfig(configFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error loading config file %s: %v\n", configFile, err)
			os.Exit(1)
		}
		// Overlay non-zero file values on top of env-var defaults.
		base = mergeYAMLConfig(base, fileCfg)
	}

	config := Config{}
	flag.StringVar(&config.APIUserAgent, "api_user_agent", base.APIUserAgent, "API User Agent")
	flag.StringVar(&config.APIToken, "api_token", base.APIToken, "Atlassian Admin API Token (admin source)")
	flag.StringVar(&config.From, "from", base.From, "(Optional) From date (RFC3339)")
	flag.StringVar(&config.OrgID, "org_id", base.OrgID, "Organization ID (admin source)")
	flag.BoolVar(&config.LogToFile, "log-to-file", boolVal(base.LogToFile), "(Optional) Enable logging to file")
	flag.StringVar(&config.LogFilePath, "log-file", base.LogFilePath, "(Optional) Path to log file [default: log.txt]")
	flag.BoolVar(&config.Debug, "debug", boolVal(base.Debug), "Enable debug mode")
	flag.StringVar(&config.Query, "query", base.Query, "Query to filter the events")
	flag.IntVar(&config.Sleep, "sleep", base.Sleep, "Sleep time milliseconds between requests")
	flag.StringVar(&config.Source, "source", base.Source, "Log source: admin, bitbucket, jira, or confluence")
	flag.StringVar(&config.BBWorkspace, "workspace", base.BBWorkspace, "Bitbucket workspace slug (bitbucket source)")
	flag.StringVar(&config.BBUsername, "bb-username", base.BBUsername, "Bitbucket username for basic auth (bitbucket source)")
	flag.StringVar(&config.BBAppPassword, "bb-app-password", base.BBAppPassword, "Bitbucket app password for basic auth (bitbucket source)")
	flag.StringVar(&config.JiraURL, "jira-url", base.JiraURL, "Jira site URL, e.g. https://your-org.atlassian.net (jira source)")
	flag.StringVar(&config.ConfluenceURL, "confluence-url", base.ConfluenceURL, "Confluence site URL, e.g. https://your-org.atlassian.net/wiki (confluence source)")
	flag.StringVar(&config.AtlassianEmail, "atlassian-email", base.AtlassianEmail, "Atlassian account email for basic auth (jira/confluence source)")
	flag.StringVar(&config.AtlassianToken, "atlassian-token", base.AtlassianToken, "Atlassian personal API token for basic auth (jira/confluence source)")
	flag.BoolVar(&config.GELFEnabled, "gelf-enabled", boolVal(base.GELFEnabled), "Enable GELF output to Graylog")
	flag.StringVar(&config.GELFHost, "gelf-host", base.GELFHost, "Graylog GELF host (env: GELF_HOST)")
	flag.IntVar(&config.GELFPort, "gelf-port", base.GELFPort, "Graylog GELF port (default 12201)")
	flag.StringVar(&config.GELFProtocol, "gelf-protocol", base.GELFProtocol, "Graylog GELF protocol: udp or tcp (default: udp)")
	flag.StringVar(&config.GELFSourceHost, "gelf-source-host", base.GELFSourceHost, "Source host field in GELF messages (env: GELF_SOURCE_HOST); defaults to gelf-host when not set")
	flag.String("config", "", "(Optional) Path to YAML configuration file")

	flag.Parse()

	// Apply GELF port default that cannot be set via a flag default (zero-value int).
	if config.GELFPort == 0 {
		config.GELFPort = 12201
	}

	// Default source host to the Graylog destination host for backward compatibility.
	if config.GELFSourceHost == "" {
		config.GELFSourceHost = config.GELFHost
	}

	if config.GELFEnabled && config.GELFHost == "" {
		fmt.Fprintln(os.Stderr, "gelf-enabled requires -gelf-host")
		flag.PrintDefaults()
		os.Exit(1)
	}

	switch config.Source {
	case "admin":
		if config.APIToken == "" || config.OrgID == "" {
			fmt.Fprintln(os.Stderr, "admin source requires -api_token and -org_id")
			flag.PrintDefaults()
			os.Exit(1)
		}
	case "bitbucket":
		if config.BBWorkspace == "" || config.BBUsername == "" || config.BBAppPassword == "" {
			fmt.Fprintln(os.Stderr, "bitbucket source requires -workspace, -bb-username, and -bb-app-password")
			flag.PrintDefaults()
			os.Exit(1)
		}
	case "jira":
		if config.JiraURL == "" || config.AtlassianEmail == "" || config.AtlassianToken == "" {
			fmt.Fprintln(os.Stderr, "jira source requires -jira-url, -atlassian-email, and -atlassian-token")
			flag.PrintDefaults()
			os.Exit(1)
		}
	case "confluence":
		if config.ConfluenceURL == "" || config.AtlassianEmail == "" || config.AtlassianToken == "" {
			fmt.Fprintln(os.Stderr, "confluence source requires -confluence-url, -atlassian-email, and -atlassian-token")
			flag.PrintDefaults()
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown source %q: must be admin, bitbucket, jira, or confluence\n", config.Source)
		flag.PrintDefaults()
		os.Exit(1)
	}

	return config
}

// UserResolver resolves Atlassian account IDs to human-readable display names.
// Resolved names are cached in-memory to avoid redundant requests within a
// single run.  The resolver is generic: the API endpoint and authentication
// strategy are supplied at construction time via buildRequest/extractName so
// that both the Atlassian Admin API and the Jira REST API are supported without
// code duplication.
type UserResolver struct {
	httpClient   *http.Client
	mu           sync.RWMutex
	cache        map[string]string
	log          *zap.SugaredLogger
	buildRequest func(id string) (*http.Request, error)
	extractName  func(data []byte) (string, error)
}

// newUserResolver creates a UserResolver that resolves account IDs via the
// Atlassian Admin User Management API
// (GET https://api.atlassian.com/users/{id}/manage/profile) using Bearer auth.
// Requires an Atlassian Guard subscription.
func newUserResolver(apiToken string, httpClient *http.Client, log *zap.SugaredLogger) *UserResolver {
	return &UserResolver{
		httpClient: httpClient,
		cache:      make(map[string]string),
		log:        log,
		buildRequest: func(id string) (*http.Request, error) {
			req, err := http.NewRequest(http.MethodGet,
				fmt.Sprintf("https://api.atlassian.com/users/%s/manage/profile", id), nil)
			if err != nil {
				return nil, err
			}
			req.Header.Set("Authorization", "Bearer "+apiToken)
			return req, nil
		},
		extractName: func(data []byte) (string, error) {
			var profile struct {
				Account struct {
					Name string `json:"name"`
				} `json:"account"`
			}
			if err := json.Unmarshal(data, &profile); err != nil {
				return "", err
			}
			return profile.Account.Name, nil
		},
	}
}

// newJiraUserResolver creates a UserResolver that resolves account IDs via the
// Jira Cloud REST API (GET {jiraURL}/rest/api/2/user?accountId=...) using Basic
// Auth.  This does not require an Atlassian Guard subscription — the same
// credentials used to fetch Jira audit records are sufficient.
func newJiraUserResolver(jiraURL, email, token string, httpClient *http.Client, log *zap.SugaredLogger) *UserResolver {
	return &UserResolver{
		httpClient: httpClient,
		cache:      make(map[string]string),
		log:        log,
		buildRequest: func(id string) (*http.Request, error) {
			u := jiraURL + "/rest/api/2/user?accountId=" + url.QueryEscape(id)
			req, err := http.NewRequest(http.MethodGet, u, nil)
			if err != nil {
				return nil, err
			}
			req.SetBasicAuth(email, token)
			return req, nil
		},
		extractName: func(data []byte) (string, error) {
			var user struct {
				DisplayName string `json:"displayName"`
			}
			if err := json.Unmarshal(data, &user); err != nil {
				return "", err
			}
			return user.DisplayName, nil
		},
	}
}

// resolve looks up the display name for an Atlassian account ID.  It strips the
// "ug:" prefix that appears in audit log actor IDs before querying the API.
// Returns an empty string when the name cannot be determined.
func (r *UserResolver) resolve(accountID string) string {
	id := strings.TrimPrefix(accountID, "ug:")
	if id == "" {
		return ""
	}

	r.mu.RLock()
	name, ok := r.cache[id]
	r.mu.RUnlock()
	if ok {
		return name
	}

	req, err := r.buildRequest(id)
	if err != nil {
		r.log.Warnf("UserResolver: failed to create request for %s: %v", id, err)
		return ""
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		r.log.Warnf("UserResolver: request failed for %s: %v", id, err)
		return ""
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		r.log.Debugf("UserResolver: unexpected status %d for account %s", resp.StatusCode, id)
		// Cache the empty result so we don't re-fetch an unresolvable account
		// on every audit record that references it (e.g. deleted/service accounts).
		r.mu.Lock()
		r.cache[id] = ""
		r.mu.Unlock()
		return ""
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		r.log.Warnf("UserResolver: read failed for %s: %v", id, err)
		return ""
	}

	name, err = r.extractName(data)
	if err != nil {
		r.log.Warnf("UserResolver: decode failed for %s: %v", id, err)
		return ""
	}

	r.mu.Lock()
	r.cache[id] = name
	r.mu.Unlock()
	return name
}

func initCloudAdmin(config Config, log *zap.SugaredLogger) (*admin.Client, error) {
	// Create a custom HTTP client with a transport that can capture response bodies
	httpClient := &http.Client{
		Transport: &responseBodyCapturingTransport{
			Transport: http.DefaultTransport,
			log:       log,
		},
	}

	cloudAdmin, err := admin.New(httpClient)
	if err != nil {
		return nil, err
	}
	cloudAdmin.Auth.SetBearerToken(config.APIToken)
	cloudAdmin.Auth.SetUserAgent(config.APIUserAgent)
	return cloudAdmin, nil
}

// Custom transport to capture response bodies
type responseBodyCapturingTransport struct {
	Transport http.RoundTripper
	log       *zap.SugaredLogger
}

func (t *responseBodyCapturingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.Transport.RoundTrip(req)
	if err != nil {
		return resp, err
	}

	// Capture the response body if it's an error response
	if resp.StatusCode >= 400 {
		bodyBytes, bodyErr := io.ReadAll(resp.Body)
		if bodyErr == nil {
			// Create a new response with the captured body so it can be read again
			resp.Body = io.NopCloser(strings.NewReader(string(bodyBytes)))
			t.log.Debugf("HTTP %d Response Body: %s", resp.StatusCode, string(bodyBytes))
		}
	}

	return resp, err
}

func fetchEvents(ctx context.Context, cloudAdmin *admin.Client, config Config, startTime time.Time, log *zap.SugaredLogger) ([]*models.OrganizationEventPageScheme, error) {
	var eventChunks []*models.OrganizationEventPageScheme
	var cursor string

	for {
		opts := &models.OrganizationEventOptScheme{
			Q:      config.Query,
			From:   startTime,
			Action: "",
		}

		events, response, err := cloudAdmin.Organization.Events(ctx, config.OrgID, opts, cursor)
		if response != nil {
			log.Debugf("Request HTTP: %v", response.Request)
		}

		if err != nil {
			if response != nil {
				log.Debugf("Response HTTP Code: %d", response.Code)
				log.Debugf("Response Headers: %v", response.Header)

				// Try to read response body for error details
				if response.Body != nil {
					bodyBytes, bodyErr := io.ReadAll(response.Body)
					if bodyErr != nil {
						log.Debugf("Error reading response body: %v", bodyErr)
					} else {
						log.Debugf("Response Body: %s", string(bodyBytes))
					}
				}

				if response.Code == 429 {
					retryAfter := handleRateLimitExceeded(response, log)
					time.Sleep(time.Duration(retryAfter) * time.Second)
					continue
				}
			}
			// Log the full error details
			log.Debugf("Full error details: %+v", err)
			log.Debugf("Error type: %T", err)
			log.Debugf("Error string: %s", err.Error())
			return nil, err
		}

		log.Debugf("Response HTTP Code: %d", response.Code)
		log.Debugf("HTTP Endpoint Used: %s", response.Endpoint)
		eventChunks = append(eventChunks, events)

		if len(events.Links.Next) == 0 {
			break
		}

		nextAsURL, err := url.Parse(events.Links.Next)
		if err != nil {
			return nil, err
		}

		cursor = nextAsURL.Query().Get("cursor")
		time.Sleep(time.Duration(config.Sleep) * time.Millisecond)
	}

	return eventChunks, nil
}

func handleRateLimitExceeded(response *models.ResponseScheme, log *zap.SugaredLogger) int {
	log.Infof("Rate limit exceeded. Retry-After: %s", response.Header.Get("X-Retry-After"))
	retryAfter := 50
	if retryAfterStr := response.Header.Get("X-Retry-After"); retryAfterStr != "" {
		var err error
		retryAfter, err = strconv.Atoi(retryAfterStr)
		if err != nil {
			log.Error("Error getting Retry-After header, set retry 50 sec", err)
		}
	} else {
		log.Debugf("X-Retry-After not found, set retry 50 sec, Headers: %v", response.Header)
	}
	return retryAfter
}

func processEvents(eventChunks []*models.OrganizationEventPageScheme, log *zap.SugaredLogger, gelfWriter GELFWriter, gelfHost string, resolver *UserResolver) {
	for _, chunk := range eventChunks {
		for _, event := range chunk.Data {
			var locationIP string
			if event.Attributes.Location != nil {
				locationIP = event.Attributes.Location.IP
			}

			var actorLink string
			if event.Attributes.Actor != nil && event.Attributes.Actor.Links != nil {
				actorLink = event.Attributes.Actor.Links.Self
			}

			var eventLink string
			if event.Links != nil {
				eventLink = event.Links.Self
			}

			var actorDisplayName string
			if resolver != nil {
				actorDisplayName = resolver.resolve(event.Attributes.Actor.ID)
			}

			log.Debugf("Event: %v", event.Attributes.Container)
			log.Info(
				"Event ID:", event.ID,
				", Event Time:", event.Attributes.Time,
				", Event Actor ID:", event.Attributes.Actor.ID,
				", Event Actor Name:", event.Attributes.Actor.Name,
				", Event Actor Display Name:", actorDisplayName,
				", Event Actor Link:", actorLink,
				", Event Action:", event.Attributes.Action,
				", Event Target:", locationIP,
				", Event Link:", eventLink,
			)

			ts, err := time.Parse(time.RFC3339, event.Attributes.Time)
			if err != nil {
				ts = time.Now().UTC()
			}
			sendGELF(gelfWriter, gelfHost,
				fmt.Sprintf("atlassian admin audit: %s", event.Attributes.Action),
				ts,
				map[string]interface{}{
					"_event_id":            event.ID,
					"_event_time":          event.Attributes.Time,
					"_actor_id":            event.Attributes.Actor.ID,
					"_actor_name":          event.Attributes.Actor.Name,
					"_actor_display_name":  actorDisplayName,
					"_actor_link":          actorLink,
					"_action":              event.Attributes.Action,
					"_location_ip":         locationIP,
					"_event_link":          eventLink,
					"_source":              "admin",
				},
				log,
			)
		}
	}
}

func initBitbucketClient(config Config, log *zap.SugaredLogger) (*bitbucket.Client, error) {
	httpClient := &http.Client{
		Transport: &responseBodyCapturingTransport{
			Transport: http.DefaultTransport,
			log:       log,
		},
	}

	bbClient, err := bitbucket.New(httpClient, "")
	if err != nil {
		return nil, err
	}
	bbClient.Auth.SetBasicAuth(config.BBUsername, config.BBAppPassword)
	bbClient.Auth.SetUserAgent(config.APIUserAgent)
	return bbClient, nil
}

func fetchBitbucketEvents(ctx context.Context, bbClient *bitbucket.Client, config Config, startTime time.Time, log *zap.SugaredLogger) ([]BitbucketAuditPage, error) {
	var pages []BitbucketAuditPage

	// Build the initial date filter using BBQL. Combine with user-supplied query if any.
	dateFilter := fmt.Sprintf(`date > "%s"`, startTime.UTC().Format(time.RFC3339))
	q := dateFilter
	if config.Query != "" {
		q = fmt.Sprintf("%s AND %s", dateFilter, config.Query)
	}

	// sort=-date returns events newest-first, matching the admin source behaviour.
	endpoint := fmt.Sprintf("2.0/workspaces/%s/workspace-log/events?q=%s&sort=-date",
		url.PathEscape(config.BBWorkspace),
		url.QueryEscape(q),
	)

	for {
		request, err := bbClient.NewRequest(ctx, http.MethodGet, endpoint, "", nil)
		if err != nil {
			return nil, err
		}

		var page BitbucketAuditPage
		response, err := bbClient.Call(request, &page)
		if response != nil {
			log.Debugf("Response HTTP Code: %d", response.Code)
			log.Debugf("HTTP Endpoint Used: %s", response.Endpoint)
		}

		if err != nil {
			if response != nil && response.Code == 429 {
				retryAfter := handleBitbucketRateLimit(response, log)
				time.Sleep(time.Duration(retryAfter) * time.Second)
				continue
			}
			if response != nil && response.Code == 404 {
				return nil, fmt.Errorf("Bitbucket workspace audit log API returned 404 for workspace %q: this endpoint requires an Atlassian Guard (formerly Atlassian Access) license. Original error: %w", config.BBWorkspace, err)
			}
			return nil, err
		}

		pages = append(pages, page)

		if page.Next == "" {
			break
		}

		// The Next field is an absolute URL; passing it directly to NewRequest
		// works because url.ResolveReference returns the absolute URL unchanged.
		endpoint = page.Next
		time.Sleep(time.Duration(config.Sleep) * time.Millisecond)
	}

	return pages, nil
}

func handleBitbucketRateLimit(response *models.ResponseScheme, log *zap.SugaredLogger) int {
	log.Infof("Rate limit exceeded. Retry-After: %s", response.Header.Get("Retry-After"))
	retryAfter := 50
	if retryAfterStr := response.Header.Get("Retry-After"); retryAfterStr != "" {
		var err error
		retryAfter, err = strconv.Atoi(retryAfterStr)
		if err != nil {
			log.Error("Error parsing Retry-After header, defaulting to 50 sec", err)
		}
	} else {
		log.Debugf("Retry-After not found, defaulting to 50 sec, Headers: %v", response.Header)
	}
	return retryAfter
}

func processBitbucketEvents(pages []BitbucketAuditPage, log *zap.SugaredLogger, gelfWriter GELFWriter, gelfHost string) {
	for _, page := range pages {
		for _, event := range page.Values {
			log.Info(
				"Event ID:", event.ID,
				", Event Date:", event.Date,
				", Event Actor UUID:", event.Actor.UUID,
				", Event Actor Name:", event.Actor.DisplayName,
				", Event Actor Account:", event.Actor.AccountID,
				", Event Action:", event.Action,
				", Subject Type:", event.Subject.Type,
				", Subject Name:", event.Subject.DisplayName,
			)

			ts, err := time.Parse(time.RFC3339, event.Date)
			if err != nil {
				ts = time.Now().UTC()
			}
			sendGELF(gelfWriter, gelfHost,
				fmt.Sprintf("bitbucket audit: %s", event.Action),
				ts,
				map[string]interface{}{
					"_event_id":      event.ID,
					"_event_date":    event.Date,
					"_actor_uuid":    event.Actor.UUID,
					"_actor_name":    event.Actor.DisplayName,
					"_actor_account": event.Actor.AccountID,
					"_action":        event.Action,
					"_subject_type":  event.Subject.Type,
					"_subject_name":  event.Subject.DisplayName,
					"_source":        "bitbucket",
				},
				log,
			)
		}
	}
}

func runAdminSource(ctx context.Context, config Config, log *zap.SugaredLogger, gelfWriter GELFWriter) {
	stateFilename := "atlassian_state.json"
	state, err := loadState(stateFilename)
	if err != nil {
		log.Errorf("Error loading state: %v. Starting from beginning.", err)
		state = SavedState{
			LastEventDate: time.Now().AddDate(0, -1, 0).UTC(),
		}
	}
	startTime := state.LastEventDate.Add(time.Second)

	if config.From != "" {
		startTime, err = time.Parse(time.RFC3339, config.From)
		if err != nil {
			log.Fatalf("Invalid from date: %v", err)
		}
	}

	cloudAdmin, err := initCloudAdmin(config, log)
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("Get event from %s", startTime)

	eventChunks, err := fetchEvents(ctx, cloudAdmin, config, startTime, log)
	if err != nil {
		log.Fatal(err)
	}

	if len(eventChunks) == 0 || len(eventChunks[0].Data) == 0 {
		log.Debugf("No events found")
		return
	}

	state.LastEventDate, err = time.Parse(time.RFC3339, eventChunks[0].Data[0].Attributes.Time)
	if err != nil {
		log.Errorf("Error getting last event time: %v", err)
	}

	processEvents(eventChunks, log, gelfWriter, config.GELFSourceHost, newUserResolver(config.APIToken, &http.Client{}, log))

	log.Debugf("Last event time: %v, eventChunks[0].Data[0].Attributes.Time: %s", state.LastEventDate, eventChunks[0].Data[0].Attributes.Time)
	if err = saveState(state, stateFilename); err != nil {
		log.Errorf("Error saving state: %v", err)
	}
}

func runBitbucketSource(ctx context.Context, config Config, log *zap.SugaredLogger, gelfWriter GELFWriter) {
	stateFilename := "bitbucket_state.json"
	state, err := loadState(stateFilename)
	if err != nil {
		log.Errorf("Error loading state: %v. Starting from beginning.", err)
		state = SavedState{
			LastEventDate: time.Now().AddDate(0, -1, 0).UTC(),
		}
	}
	startTime := state.LastEventDate.Add(time.Second)

	if config.From != "" {
		startTime, err = time.Parse(time.RFC3339, config.From)
		if err != nil {
			log.Fatalf("Invalid from date: %v", err)
		}
	}

	bbClient, err := initBitbucketClient(config, log)
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("Get Bitbucket audit events from %s", startTime)

	pages, err := fetchBitbucketEvents(ctx, bbClient, config, startTime, log)
	if err != nil {
		log.Fatal(err)
	}

	if len(pages) == 0 || len(pages[0].Values) == 0 {
		log.Debugf("No Bitbucket audit events found")
		return
	}

	// Events are sorted newest-first (sort=-date); the first event of the first
	// page is the most recent, which we save as the checkpoint.
	state.LastEventDate, err = time.Parse(time.RFC3339, pages[0].Values[0].Date)
	if err != nil {
		log.Errorf("Error parsing last event date %q: %v", pages[0].Values[0].Date, err)
	}

	processBitbucketEvents(pages, log, gelfWriter, config.GELFSourceHost)

	log.Debugf("Last event time: %v", state.LastEventDate)
	if err = saveState(state, stateFilename); err != nil {
		log.Errorf("Error saving state: %v", err)
	}
}

func initJiraClient(config Config, log *zap.SugaredLogger) (*jirav2.Client, error) {
	httpClient := &http.Client{
		Transport: &responseBodyCapturingTransport{
			Transport: http.DefaultTransport,
			log:       log,
		},
	}

	jiraClient, err := jirav2.New(httpClient, config.JiraURL)
	if err != nil {
		return nil, err
	}
	jiraClient.Auth.SetBasicAuth(config.AtlassianEmail, config.AtlassianToken)
	jiraClient.Auth.SetUserAgent(config.APIUserAgent)
	return jiraClient, nil
}

func fetchJiraAuditRecords(ctx context.Context, jiraClient *jirav2.Client, config Config, startTime time.Time, log *zap.SugaredLogger) ([]*models.AuditRecordPageScheme, error) {
	const pageSize = 1000
	var pages []*models.AuditRecordPageScheme
	offset := 0

	for {
		opts := &models.AuditRecordGetOptions{
			Filter: config.Query,
			From:   startTime,
		}

		page, response, err := jiraClient.Audit.Get(ctx, opts, offset, pageSize)
		if response != nil {
			log.Debugf("Response HTTP Code: %d", response.Code)
			log.Debugf("HTTP Endpoint Used: %s", response.Endpoint)
		}

		if err != nil {
			if response != nil && response.Code == 429 {
				retryAfter := handleRateLimitExceeded(response, log)
				time.Sleep(time.Duration(retryAfter) * time.Second)
				continue
			}
			return nil, err
		}

		pages = append(pages, page)

		if offset+pageSize >= page.Total {
			break
		}

		offset += pageSize
		time.Sleep(time.Duration(config.Sleep) * time.Millisecond)
	}

	return pages, nil
}

func processJiraAuditRecords(pages []*models.AuditRecordPageScheme, log *zap.SugaredLogger, gelfWriter GELFWriter, gelfHost string, resolver *UserResolver) {
	for _, page := range pages {
		for _, record := range page.Records {
			objectName := ""
			objectType := ""
			if record.ObjectItem != nil {
				objectName = record.ObjectItem.Name
				objectType = record.ObjectItem.TypeName
			}

			var authorDisplayName string
			if resolver != nil && record.AuthorAccountID != "" {
				authorDisplayName = resolver.resolve(record.AuthorAccountID)
			}

			log.Info(
				"Record ID:", record.ID,
				", Created:", record.Created,
				", Author:", record.AuthorAccountID,
				", Author Display Name:", authorDisplayName,
				", Summary:", record.Summary,
				", Category:", record.Category,
				", Remote Address:", record.RemoteAddress,
				", Object:", objectName,
				", Object Type:", objectType,
			)

			ts, err := time.Parse("2006-01-02T15:04:05.999-0700", record.Created)
			if err != nil {
				ts = time.Now().UTC()
			}
			sendGELF(gelfWriter, gelfHost,
				fmt.Sprintf("jira audit: %s", record.Summary),
				ts,
				map[string]interface{}{
					"_record_id":           record.ID,
					"_created":             record.Created,
					"_author":              record.AuthorAccountID,
					"_author_display_name": authorDisplayName,
					"_summary":             record.Summary,
					"_category":            record.Category,
					"_remote_address":      record.RemoteAddress,
					"_object":              objectName,
					"_object_type":         objectType,
					"_source":              "jira",
				},
				log,
			)
		}
	}
}

func runJiraSource(ctx context.Context, config Config, log *zap.SugaredLogger, gelfWriter GELFWriter) {
	stateFilename := "jira_state.json"
	state, err := loadState(stateFilename)
	if err != nil {
		log.Errorf("Error loading state: %v. Starting from beginning.", err)
		state = SavedState{
			LastEventDate: time.Now().AddDate(0, -1, 0).UTC(),
		}
	}
	startTime := state.LastEventDate.Add(time.Second)

	if config.From != "" {
		startTime, err = time.Parse(time.RFC3339, config.From)
		if err != nil {
			log.Fatalf("Invalid from date: %v", err)
		}
	}

	jiraClient, err := initJiraClient(config, log)
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("Get Jira audit records from %s", startTime)

	pages, err := fetchJiraAuditRecords(ctx, jiraClient, config, startTime, log)
	if err != nil {
		log.Fatal(err)
	}

	if len(pages) == 0 || len(pages[0].Records) == 0 {
		log.Debugf("No Jira audit records found")
		return
	}

	// Records are returned newest-first; pick the first record of the first page as checkpoint.
	state.LastEventDate, err = time.Parse("2006-01-02T15:04:05.999-0700", pages[0].Records[0].Created)
	if err != nil {
		log.Errorf("Error parsing last record date %q: %v", pages[0].Records[0].Created, err)
	}

	jiraResolver := newJiraUserResolver(config.JiraURL, config.AtlassianEmail, config.AtlassianToken, &http.Client{}, log)
	processJiraAuditRecords(pages, log, gelfWriter, config.GELFSourceHost, jiraResolver)

	log.Debugf("Last event time: %v", state.LastEventDate)
	if err = saveState(state, stateFilename); err != nil {
		log.Errorf("Error saving state: %v", err)
	}
}

func initConfluenceClient(config Config, log *zap.SugaredLogger) (*confluence.Client, error) {
	httpClient := &http.Client{
		Transport: &responseBodyCapturingTransport{
			Transport: http.DefaultTransport,
			log:       log,
		},
	}

	confluenceClient, err := confluence.New(httpClient, config.ConfluenceURL)
	if err != nil {
		return nil, err
	}
	confluenceClient.Auth.SetBasicAuth(config.AtlassianEmail, config.AtlassianToken)
	confluenceClient.Auth.SetUserAgent(config.APIUserAgent)
	return confluenceClient, nil
}

func fetchConfluenceAuditRecords(ctx context.Context, confluenceClient *confluence.Client, config Config, startTime time.Time, log *zap.SugaredLogger) ([]ConfluenceAuditPage, error) {
	const pageSize = 1000
	var pages []ConfluenceAuditPage

	// Confluence audit API accepts dates as YYYY-MM-DD strings.
	startDate := startTime.UTC().Format("2006-01-02")

	params := url.Values{}
	params.Set("startDate", startDate)
	params.Set("limit", strconv.Itoa(pageSize))
	if config.Query != "" {
		params.Set("searchString", config.Query)
	}

	endpoint := fmt.Sprintf("rest/api/audit?%s", params.Encode())

	for {
		request, err := confluenceClient.NewRequest(ctx, http.MethodGet, endpoint, "", nil)
		if err != nil {
			return nil, err
		}

		var page ConfluenceAuditPage
		response, err := confluenceClient.Call(request, &page)
		if response != nil {
			log.Debugf("Response HTTP Code: %d", response.Code)
			log.Debugf("HTTP Endpoint Used: %s", response.Endpoint)
		}

		if err != nil {
			if response != nil && response.Code == 429 {
				retryAfter := handleRateLimitExceeded(response, log)
				time.Sleep(time.Duration(retryAfter) * time.Second)
				continue
			}
			return nil, err
		}

		pages = append(pages, page)

		if page.Links.Next == "" {
			break
		}

		endpoint = page.Links.Next
		time.Sleep(time.Duration(config.Sleep) * time.Millisecond)
	}

	return pages, nil
}

func processConfluenceAuditRecords(pages []ConfluenceAuditPage, log *zap.SugaredLogger, gelfWriter GELFWriter, gelfHost string) {
	for _, page := range pages {
		for _, record := range page.Results {
			createdMs := time.UnixMilli(record.CreationDate).UTC()
			log.Info(
				"Created:", createdMs.Format(time.RFC3339),
				", Author:", record.Author.DisplayName,
				", Author Account:", record.Author.AccountID,
				", Summary:", record.Summary,
				", Category:", record.Category,
				", Remote Address:", record.RemoteAddress,
				", Object:", record.AffectedObject.Name,
				", Object Type:", record.AffectedObject.ObjectType,
			)

			sendGELF(gelfWriter, gelfHost,
				fmt.Sprintf("confluence audit: %s", record.Summary),
				createdMs,
				map[string]interface{}{
					"_created":        createdMs.Format(time.RFC3339),
					"_author":         record.Author.DisplayName,
					"_author_account": record.Author.AccountID,
					"_summary":        record.Summary,
					"_category":       record.Category,
					"_remote_address": record.RemoteAddress,
					"_object":         record.AffectedObject.Name,
					"_object_type":    record.AffectedObject.ObjectType,
					"_source":         "confluence",
				},
				log,
			)
		}
	}
}

func runConfluenceSource(ctx context.Context, config Config, log *zap.SugaredLogger, gelfWriter GELFWriter) {
	stateFilename := "confluence_state.json"
	state, err := loadState(stateFilename)
	if err != nil {
		log.Errorf("Error loading state: %v. Starting from beginning.", err)
		state = SavedState{
			LastEventDate: time.Now().AddDate(0, -1, 0).UTC(),
		}
	}
	startTime := state.LastEventDate.Add(time.Second)

	if config.From != "" {
		startTime, err = time.Parse(time.RFC3339, config.From)
		if err != nil {
			log.Fatalf("Invalid from date: %v", err)
		}
	}

	confluenceClient, err := initConfluenceClient(config, log)
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("Get Confluence audit records from %s", startTime)

	pages, err := fetchConfluenceAuditRecords(ctx, confluenceClient, config, startTime, log)
	if err != nil {
		log.Fatal(err)
	}

	if len(pages) == 0 || len(pages[0].Results) == 0 {
		log.Debugf("No Confluence audit records found")
		return
	}

	// Records are returned newest-first; pick the first record of the first page as checkpoint.
	state.LastEventDate = time.UnixMilli(pages[0].Results[0].CreationDate).UTC()

	processConfluenceAuditRecords(pages, log, gelfWriter, config.GELFSourceHost)

	log.Debugf("Last event time: %v", state.LastEventDate)
	if err = saveState(state, stateFilename); err != nil {
		log.Errorf("Error saving state: %v", err)
	}
}

func main() {
	config := parseFlags()
	log := initLogger(config.Debug, config.LogToFile, config.LogFilePath)
	defer log.Sync()

	gelfWriter := initGELF(config, log)
	if gelfWriter != nil {
		defer gelfWriter.Close()
	}

	ctx := context.Background()

	switch config.Source {
	case "admin":
		runAdminSource(ctx, config, log, gelfWriter)
	case "bitbucket":
		runBitbucketSource(ctx, config, log, gelfWriter)
	case "jira":
		runJiraSource(ctx, config, log, gelfWriter)
	case "confluence":
		runConfluenceSource(ctx, config, log, gelfWriter)
	}
}
