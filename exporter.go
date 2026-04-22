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

	"github.com/m1keru/go-atlassian/admin"
	"github.com/m1keru/go-atlassian/pkg/infra/models"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type SavedState struct {
	LastEventDate time.Time `json:"last_event_date"`
}

type Config struct {
	APIUserAgent string
	APIToken     string
	From         string
	OrgID        string
	LogToFile    bool
	LogFilePath  string
	Debug        bool
	Query        string
	Sleep        int
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
	flag.StringVar(&config.APIToken, "api_token", os.Getenv("ATLASSIAN_ADMIN_API_TOKEN"), "Atlassian Admin API Token")
	flag.StringVar(&config.From, "from", "", "(Optional) From date (RFC3339)")
	flag.StringVar(&config.OrgID, "org_id", os.Getenv("ATLASSIAN_ORGID"), "Organization ID")
	flag.BoolVar(&config.LogToFile, "log-to-file", false, "(Optional) Enable logging to file")
	flag.StringVar(&config.LogFilePath, "log-file", "log.txt", "(Optional) Path to log file [default: log.txt]")
	flag.BoolVar(&config.Debug, "debug", false, "Enable debug mode")
	flag.StringVar(&config.Query, "query", "", "Query to filter the events")
	flag.IntVar(&config.Sleep, "sleep", 200, "Sleep time milliseconds between requests")

	flag.Parse()

	if config.APIToken == "" || config.OrgID == "" {
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

func main() {
	config := parseFlags()
	log := initLogger(config.Debug, config.LogToFile, config.LogFilePath)
	defer log.Sync()

	ctx := context.Background()

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

	if len(eventChunks) != 0 && len(eventChunks[0].Data) != 0 {
		state.LastEventDate, err = time.Parse(time.RFC3339, eventChunks[0].Data[0].Attributes.Time)
		if err != nil {
			log.Errorf("Error getting last event time: %v", err)
		}
	} else {
		log.Debugf("No events found")
		os.Exit(0)
	}

	processEvents(eventChunks, log)

	log.Debugf("Last event time: %v, eventChunks[0].Data[0].Attributes.Time: %s", state.LastEventDate, eventChunks[0].Data[0].Attributes.Time)
	err = saveState(state, stateFilename)
	if err != nil {
		log.Errorf("Error saving state: %v", err)
	}
}
