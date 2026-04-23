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
	"time"

	"github.com/ctreminiom/go-atlassian/admin"
	"github.com/ctreminiom/go-atlassian/bitbucket"
	"github.com/ctreminiom/go-atlassian/confluence"
	jirav2 "github.com/ctreminiom/go-atlassian/jira/v2"
	"github.com/ctreminiom/go-atlassian/pkg/infra/models"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type SavedState struct {
	LastEventDate time.Time `json:"last_event_date"`
}

type Config struct {
	APIUserAgent    string
	APIToken        string
	From            string
	OrgID           string
	LogToFile       bool
	LogFilePath     string
	Debug           bool
	Query           string
	Sleep           int
	Source          string
	BBWorkspace     string
	BBUsername      string
	BBAppPassword   string
	JiraURL         string
	ConfluenceURL   string
	AtlassianEmail  string
	AtlassianToken  string
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
	Author          ConfluenceAuditAuthor `json:"author"`
	RemoteAddress   string                `json:"remoteAddress"`
	CreationDate    int64                 `json:"creationDate"`
	Summary         string                `json:"summary"`
	Description     string                `json:"description"`
	Category        string                `json:"category"`
	SysAdmin        bool                  `json:"sysAdmin"`
	AffectedObject  ConfluenceAuditObject `json:"affectedObject"`
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

func parseFlags() Config {
	config := Config{}
	flag.StringVar(&config.APIUserAgent, "api_user_agent", "curl/7.54.0", "API User Agent")
	flag.StringVar(&config.APIToken, "api_token", os.Getenv("ATLASSIAN_ADMIN_API_TOKEN"), "Atlassian Admin API Token (admin source)")
	flag.StringVar(&config.From, "from", "", "(Optional) From date (RFC3339)")
	flag.StringVar(&config.OrgID, "org_id", os.Getenv("ATLASSIAN_ORGID"), "Organization ID (admin source)")
	flag.BoolVar(&config.LogToFile, "log-to-file", false, "(Optional) Enable logging to file")
	flag.StringVar(&config.LogFilePath, "log-file", "log.txt", "(Optional) Path to log file [default: log.txt]")
	flag.BoolVar(&config.Debug, "debug", false, "Enable debug mode")
	flag.StringVar(&config.Query, "query", "", "Query to filter the events")
	flag.IntVar(&config.Sleep, "sleep", 200, "Sleep time milliseconds between requests")
	flag.StringVar(&config.Source, "source", "admin", "Log source: admin, bitbucket, jira, or confluence")
	flag.StringVar(&config.BBWorkspace, "workspace", os.Getenv("BITBUCKET_WORKSPACE"), "Bitbucket workspace slug (bitbucket source)")
	flag.StringVar(&config.BBUsername, "bb-username", os.Getenv("BITBUCKET_USERNAME"), "Bitbucket username for basic auth (bitbucket source)")
	flag.StringVar(&config.BBAppPassword, "bb-app-password", os.Getenv("BITBUCKET_APP_PASSWORD"), "Bitbucket app password for basic auth (bitbucket source)")
	flag.StringVar(&config.JiraURL, "jira-url", os.Getenv("JIRA_URL"), "Jira site URL, e.g. https://your-org.atlassian.net (jira source)")
	flag.StringVar(&config.ConfluenceURL, "confluence-url", os.Getenv("CONFLUENCE_URL"), "Confluence site URL, e.g. https://your-org.atlassian.net/wiki (confluence source)")
	flag.StringVar(&config.AtlassianEmail, "atlassian-email", os.Getenv("ATLASSIAN_EMAIL"), "Atlassian account email for basic auth (jira/confluence source)")
	flag.StringVar(&config.AtlassianToken, "atlassian-token", os.Getenv("ATLASSIAN_TOKEN"), "Atlassian personal API token for basic auth (jira/confluence source)")

	flag.Parse()

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

func processEvents(eventChunks []*models.OrganizationEventPageScheme, log *zap.SugaredLogger) {
	for _, chunk := range eventChunks {
		for _, event := range chunk.Data {
			var locationIP string
			if event.Attributes.Location != nil {
				locationIP = event.Attributes.Location.IP
			}

			log.Debugf("Event: %v", event.Attributes.Container)
			log.Info(
				"Event ID:", event.ID,
				", Event Time:", event.Attributes.Time,
				", Event Actor ID:", event.Attributes.Actor.ID,
				", Event Actor Name:", event.Attributes.Actor.Name,
				", Event Actor Link:", event.Attributes.Actor.Links.Self,
				", Event Action:", event.Attributes.Action,
				", Event Target:", locationIP,
				", Event Link:", event.Links.Self,
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

func processBitbucketEvents(pages []BitbucketAuditPage, log *zap.SugaredLogger) {
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
		}
	}
}

func runAdminSource(ctx context.Context, config Config, log *zap.SugaredLogger) {
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

	processEvents(eventChunks, log)

	log.Debugf("Last event time: %v, eventChunks[0].Data[0].Attributes.Time: %s", state.LastEventDate, eventChunks[0].Data[0].Attributes.Time)
	if err = saveState(state, stateFilename); err != nil {
		log.Errorf("Error saving state: %v", err)
	}
}

func runBitbucketSource(ctx context.Context, config Config, log *zap.SugaredLogger) {	stateFilename := "bitbucket_state.json"
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

	processBitbucketEvents(pages, log)

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

func processJiraAuditRecords(pages []*models.AuditRecordPageScheme, log *zap.SugaredLogger) {
	for _, page := range pages {
		for _, record := range page.Records {
			objectName := ""
			objectType := ""
			if record.ObjectItem != nil {
				objectName = record.ObjectItem.Name
				objectType = record.ObjectItem.TypeName
			}
			log.Info(
				"Record ID:", record.ID,
				", Created:", record.Created,
				", Author:", record.AuthorKey,
				", Summary:", record.Summary,
				", Category:", record.Category,
				", Remote Address:", record.RemoteAddress,
				", Object:", objectName,
				", Object Type:", objectType,
			)
		}
	}
}

func runJiraSource(ctx context.Context, config Config, log *zap.SugaredLogger) {
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

	// Records are returned oldest-first; pick the last record of the last page as checkpoint.
	lastPage := pages[len(pages)-1]
	lastRecord := lastPage.Records[len(lastPage.Records)-1]
	state.LastEventDate, err = time.Parse("2006-01-02T15:04:05.999-0700", lastRecord.Created)
	if err != nil {
		log.Errorf("Error parsing last record date %q: %v", lastRecord.Created, err)
	}

	processJiraAuditRecords(pages, log)

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

func processConfluenceAuditRecords(pages []ConfluenceAuditPage, log *zap.SugaredLogger) {
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
		}
	}
}

func runConfluenceSource(ctx context.Context, config Config, log *zap.SugaredLogger) {
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

	// Records are returned oldest-first; pick the last record of the last page as checkpoint.
	lastPage := pages[len(pages)-1]
	lastRecord := lastPage.Results[len(lastPage.Results)-1]
	state.LastEventDate = time.UnixMilli(lastRecord.CreationDate).UTC()

	processConfluenceAuditRecords(pages, log)

	log.Debugf("Last event time: %v", state.LastEventDate)
	if err = saveState(state, stateFilename); err != nil {
		log.Errorf("Error saving state: %v", err)
	}
}

func main() {
	config := parseFlags()
	log := initLogger(config.Debug, config.LogToFile, config.LogFilePath)
	defer log.Sync()

	ctx := context.Background()

	switch config.Source {
	case "admin":
		runAdminSource(ctx, config, log)
	case "bitbucket":
		runBitbucketSource(ctx, config, log)
	case "jira":
		runJiraSource(ctx, config, log)
	case "confluence":
		runConfluenceSource(ctx, config, log)
	}
}
