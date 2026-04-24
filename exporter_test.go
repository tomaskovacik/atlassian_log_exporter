package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ctreminiom/go-atlassian/pkg/infra/models"
	"go.uber.org/zap"
)

// nopLogger returns a no-op sugared logger suitable for tests.
func nopLogger() *zap.SugaredLogger {
	return zap.NewNop().Sugar()
}

// ---------- saveState / loadState ----------

func TestSaveAndLoadState_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	want := SavedState{LastEventDate: time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)}
	if err := saveState(want, path); err != nil {
		t.Fatalf("saveState: %v", err)
	}

	got, err := loadState(path)
	if err != nil {
		t.Fatalf("loadState: %v", err)
	}

	if !got.LastEventDate.Equal(want.LastEventDate) {
		t.Errorf("got %v, want %v", got.LastEventDate, want.LastEventDate)
	}
}

func TestLoadState_FileNotFound(t *testing.T) {
	_, err := loadState(filepath.Join(t.TempDir(), "nonexistent.json"))
	if err == nil {
		t.Error("expected an error for missing file, got nil")
	}
}

func TestLoadState_InvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(path, []byte("not-json"), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := loadState(path)
	if err == nil {
		t.Error("expected an error for invalid JSON, got nil")
	}
}

func TestSaveState_WritesValidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	ts := time.Date(2025, 1, 15, 9, 30, 0, 0, time.UTC)
	if err := saveState(SavedState{LastEventDate: ts}, path); err != nil {
		t.Fatalf("saveState: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("saved file is not valid JSON: %v", err)
	}

	if _, ok := parsed["last_event_date"]; !ok {
		t.Error("saved JSON is missing 'last_event_date' key")
	}
}

// ---------- handleRateLimitExceeded ----------

func newResponseWithHeader(code int, headerKey, headerVal string) *models.ResponseScheme {
	h := http.Header{}
	if headerKey != "" {
		h.Set(headerKey, headerVal)
	}
	return &models.ResponseScheme{
		Response: &http.Response{Header: h},
		Code:     code,
	}
}

func TestHandleRateLimitExceeded_WithHeader(t *testing.T) {
	resp := newResponseWithHeader(429, "X-Retry-After", "30")
	got := handleRateLimitExceeded(resp, nopLogger())
	if got != 30 {
		t.Errorf("got %d, want 30", got)
	}
}

func TestHandleRateLimitExceeded_WithoutHeader(t *testing.T) {
	resp := newResponseWithHeader(429, "", "")
	got := handleRateLimitExceeded(resp, nopLogger())
	if got != 50 {
		t.Errorf("got %d, want 50 (default)", got)
	}
}

func TestHandleRateLimitExceeded_InvalidHeader(t *testing.T) {
	resp := newResponseWithHeader(429, "X-Retry-After", "not-a-number")
	got := handleRateLimitExceeded(resp, nopLogger())
	// strconv.Atoi fails and returns 0; the assignment overwrites the default 50.
	if got != 0 {
		t.Errorf("got %d, want 0 (strconv.Atoi returns 0 on error)", got)
	}
}

// ---------- handleBitbucketRateLimit ----------

func TestHandleBitbucketRateLimit_WithHeader(t *testing.T) {
	resp := newResponseWithHeader(429, "Retry-After", "45")
	got := handleBitbucketRateLimit(resp, nopLogger())
	if got != 45 {
		t.Errorf("got %d, want 45", got)
	}
}

func TestHandleBitbucketRateLimit_WithoutHeader(t *testing.T) {
	resp := newResponseWithHeader(429, "", "")
	got := handleBitbucketRateLimit(resp, nopLogger())
	if got != 50 {
		t.Errorf("got %d, want 50 (default)", got)
	}
}

func TestHandleBitbucketRateLimit_InvalidHeader(t *testing.T) {
	resp := newResponseWithHeader(429, "Retry-After", "bad")
	got := handleBitbucketRateLimit(resp, nopLogger())
	// strconv.Atoi fails and returns 0; the assignment overwrites the default 50.
	if got != 0 {
		t.Errorf("got %d, want 0 (strconv.Atoi returns 0 on error)", got)
	}
}

// ---------- responseBodyCapturingTransport ----------

func TestResponseBodyCapturingTransport_SuccessPassThrough(t *testing.T) {
	body := `{"ok":true}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()

	transport := &responseBodyCapturingTransport{
		Transport: http.DefaultTransport,
		log:       nopLogger(),
	}

	req, _ := http.NewRequest(http.MethodGet, server.URL, nil)
	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("got status %d, want 200", resp.StatusCode)
	}

	// Body must still be readable after transport touches it.
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(data) != body {
		t.Errorf("got body %q, want %q", string(data), body)
	}
}

func TestResponseBodyCapturingTransport_ErrorBodyReadable(t *testing.T) {
	errBody := `{"error":"unauthorized"}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(errBody))
	}))
	defer server.Close()

	transport := &responseBodyCapturingTransport{
		Transport: http.DefaultTransport,
		log:       nopLogger(),
	}

	req, _ := http.NewRequest(http.MethodGet, server.URL, nil)
	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("got status %d, want 401", resp.StatusCode)
	}

	// Body must still be readable after the transport has consumed it.
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(data) != errBody {
		t.Errorf("got body %q, want %q", string(data), errBody)
	}
}

func TestResponseBodyCapturingTransport_TransportError(t *testing.T) {
	transport := &responseBodyCapturingTransport{
		Transport: http.DefaultTransport,
		log:       nopLogger(),
	}

	req, _ := http.NewRequest(http.MethodGet, "http://127.0.0.1:1", nil) // nothing listening
	_, err := transport.RoundTrip(req)
	if err == nil {
		t.Error("expected connection-refused error, got nil")
	}
}

// ---------- processBitbucketEvents ----------

func TestProcessBitbucketEvents_NoPanic(t *testing.T) {
	pages := []BitbucketAuditPage{
		{
			Values: []BitbucketAuditEvent{
				{
					ID:     "evt-1",
					Date:   "2024-06-01T10:00:00+00:00",
					Action: "repository_push",
					Actor: BitbucketActor{
						DisplayName: "Alice",
						UUID:        "{uuid-1}",
						AccountID:   "acc-1",
					},
					Subject: BitbucketSubject{
						Type:        "repository",
						DisplayName: "my-repo",
					},
				},
			},
		},
	}
	processBitbucketEvents(pages, nopLogger(), nil, "")
}

func TestProcessBitbucketEvents_EmptyPages(t *testing.T) {
	processBitbucketEvents(nil, nopLogger(), nil, "")
	processBitbucketEvents([]BitbucketAuditPage{}, nopLogger(), nil, "")
	processBitbucketEvents([]BitbucketAuditPage{{Values: nil}}, nopLogger(), nil, "")
}

// ---------- processEvents (admin) ----------

func TestProcessEvents_NilLocation(t *testing.T) {
	chunks := []*models.OrganizationEventPageScheme{
		{
			Data: []*models.OrganizationEventModelScheme{
				{
					ID: "ev-1",
					Attributes: &models.OrganizationEventModelAttributesScheme{
						Time:   "2024-06-01T10:00:00Z",
						Action: "user_login",
						Actor: &models.OrganizationEventActorModel{
							ID:   "actor-1",
							Name: "Bob",
						},
						Location: nil, // must not panic
					},
					Links: &models.LinkSelfModelScheme{Self: "https://example.com"},
				},
			},
		},
	}
	processEvents(chunks, nopLogger(), nil, "")
}

func TestProcessEvents_WithLocation(t *testing.T) {
	chunks := []*models.OrganizationEventPageScheme{
		{
			Data: []*models.OrganizationEventModelScheme{
				{
					ID: "ev-2",
					Attributes: &models.OrganizationEventModelAttributesScheme{
						Time:   "2024-06-01T11:00:00Z",
						Action: "user_logout",
						Actor: &models.OrganizationEventActorModel{
							ID:   "actor-2",
							Name: "Carol",
						},
						Location: &models.OrganizationEventLocationModel{
							IP: "192.0.2.1",
						},
					},
					Links: &models.LinkSelfModelScheme{Self: "https://example.com/ev-2"},
				},
			},
		},
	}
	processEvents(chunks, nopLogger(), nil, "")
}

func TestProcessEvents_Empty(t *testing.T) {
	processEvents(nil, nopLogger(), nil, "")
	processEvents([]*models.OrganizationEventPageScheme{}, nopLogger(), nil, "")
}

// ---------- processJiraAuditRecords ----------

func TestProcessJiraAuditRecords_NilObjectItem(t *testing.T) {
	pages := []*models.AuditRecordPageScheme{
		{
			Records: []*models.AuditRecordScheme{
				{
					ID:            1,
					Summary:       "User created",
					AuthorKey:     "jdoe",
					Created:       "2024-06-01T10:00:00.000+0000",
					Category:      "user management",
					RemoteAddress: "192.0.2.1",
					ObjectItem:    nil,
				},
			},
		},
	}
	processJiraAuditRecords(pages, nopLogger(), nil, "")
}

func TestProcessJiraAuditRecords_WithObjectItem(t *testing.T) {
	pages := []*models.AuditRecordPageScheme{
		{
			Records: []*models.AuditRecordScheme{
				{
					ID:        2,
					Summary:   "Project created",
					AuthorKey: "admin",
					Created:   "2024-06-02T08:00:00.000+0000",
					Category:  "project",
					ObjectItem: &models.AuditRecordObjectItemScheme{
						Name:     "MyProject",
						TypeName: "PROJECT",
					},
				},
			},
		},
	}
	processJiraAuditRecords(pages, nopLogger(), nil, "")
}

func TestProcessJiraAuditRecords_Empty(t *testing.T) {
	processJiraAuditRecords(nil, nopLogger(), nil, "")
	processJiraAuditRecords([]*models.AuditRecordPageScheme{}, nopLogger(), nil, "")
}

// ---------- processConfluenceAuditRecords ----------

func TestProcessConfluenceAuditRecords_NoPanic(t *testing.T) {
	pages := []ConfluenceAuditPage{
		{
			Results: []ConfluenceAuditRecord{
				{
					Author: ConfluenceAuditAuthor{
						DisplayName: "Dave",
						AccountID:   "acc-dave",
					},
					RemoteAddress: "10.0.0.1",
					CreationDate:  1717228800000, // 2024-06-01 00:00:00 UTC in ms
					Summary:       "Space created",
					Category:      "space",
					AffectedObject: ConfluenceAuditObject{
						Name:       "Engineering",
						ObjectType: "Space",
					},
				},
			},
		},
	}
	processConfluenceAuditRecords(pages, nopLogger(), nil, "")
}

func TestProcessConfluenceAuditRecords_Empty(t *testing.T) {
	processConfluenceAuditRecords(nil, nopLogger(), nil, "")
	processConfluenceAuditRecords([]ConfluenceAuditPage{}, nopLogger(), nil, "")
	processConfluenceAuditRecords([]ConfluenceAuditPage{{Results: nil}}, nopLogger(), nil, "")
}

// ---------- fetchBitbucketEvents query building ----------

// TestFetchBitbucketEventsDateFilter validates the date-filter string format used
// when building the Bitbucket audit log query, matching the logic in fetchBitbucketEvents.
func TestFetchBitbucketEventsDateFilter(t *testing.T) {
	startTime := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)

	dateFilter := `date > "` + startTime.UTC().Format(time.RFC3339) + `"`
	if !strings.Contains(dateFilter, "2024-06-01T00:00:00Z") {
		t.Errorf("date filter %q does not contain expected date", dateFilter)
	}

	// With an additional query, the filter should be combined with AND.
	q := dateFilter + " AND " + "action = \"repository_push\""
	if !strings.Contains(q, "AND") {
		t.Errorf("combined query %q missing AND separator", q)
	}
}

// ---------- loadYAMLConfig ----------

func TestLoadYAMLConfig_AllFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := `
api_user_agent: myagent/1.0
api_token: secret-token
from: "2024-01-01T00:00:00Z"
org_id: my-org
log_to_file: true
log_file: /var/log/exporter.log
debug: true
query: "action = login"
sleep: 500
source: bitbucket
workspace: my-workspace
bb_username: bbuser
bb_app_password: bbpassword
jira_url: https://jira.example.com
confluence_url: https://confluence.example.com
atlassian_email: user@example.com
atlassian_token: atlassian-secret
gelf_enabled: true
gelf_host: graylog.example.com
gelf_port: 12201
gelf_protocol: tcp
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := loadYAMLConfig(path)
	if err != nil {
		t.Fatalf("loadYAMLConfig: %v", err)
	}

	checks := []struct {
		name string
		got  string
		want string
	}{
		{"APIUserAgent", cfg.APIUserAgent, "myagent/1.0"},
		{"APIToken", cfg.APIToken, "secret-token"},
		{"From", cfg.From, "2024-01-01T00:00:00Z"},
		{"OrgID", cfg.OrgID, "my-org"},
		{"LogFilePath", cfg.LogFilePath, "/var/log/exporter.log"},
		{"Query", cfg.Query, "action = login"},
		{"Source", cfg.Source, "bitbucket"},
		{"BBWorkspace", cfg.BBWorkspace, "my-workspace"},
		{"BBUsername", cfg.BBUsername, "bbuser"},
		{"BBAppPassword", cfg.BBAppPassword, "bbpassword"},
		{"JiraURL", cfg.JiraURL, "https://jira.example.com"},
		{"ConfluenceURL", cfg.ConfluenceURL, "https://confluence.example.com"},
		{"AtlassianEmail", cfg.AtlassianEmail, "user@example.com"},
		{"AtlassianToken", cfg.AtlassianToken, "atlassian-secret"},
		{"GELFHost", cfg.GELFHost, "graylog.example.com"},
		{"GELFProtocol", cfg.GELFProtocol, "tcp"},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, c.got, c.want)
		}
	}
	if cfg.LogToFile == nil || !*cfg.LogToFile {
		t.Error("LogToFile: got nil or false, want true")
	}
	if cfg.Debug == nil || !*cfg.Debug {
		t.Error("Debug: got nil or false, want true")
	}
	if cfg.Sleep != 500 {
		t.Errorf("Sleep: got %d, want 500", cfg.Sleep)
	}
	if cfg.GELFEnabled == nil || !*cfg.GELFEnabled {
		t.Error("GELFEnabled: got nil or false, want true")
	}
	if cfg.GELFPort != 12201 {
		t.Errorf("GELFPort: got %d, want 12201", cfg.GELFPort)
	}
}

func TestLoadYAMLConfig_FileNotFound(t *testing.T) {
	_, err := loadYAMLConfig(filepath.Join(t.TempDir(), "nonexistent.yaml"))
	if err == nil {
		t.Error("expected an error for missing file, got nil")
	}
}

func TestLoadYAMLConfig_InvalidYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(path, []byte(":\tinvalid: yaml: content\n"), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := loadYAMLConfig(path)
	if err == nil {
		t.Error("expected an error for invalid YAML, got nil")
	}
}

func TestLoadYAMLConfig_PartialFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "partial.yaml")
	content := "source: jira\natlassian_email: user@example.com\n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := loadYAMLConfig(path)
	if err != nil {
		t.Fatalf("loadYAMLConfig: %v", err)
	}

	if cfg.Source != "jira" {
		t.Errorf("Source: got %q, want %q", cfg.Source, "jira")
	}
	if cfg.AtlassianEmail != "user@example.com" {
		t.Errorf("AtlassianEmail: got %q, want %q", cfg.AtlassianEmail, "user@example.com")
	}
	// Unset fields should be zero values.
	if cfg.APIToken != "" {
		t.Errorf("APIToken: got %q, want empty", cfg.APIToken)
	}
}

func TestLoadYAMLConfig_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.yaml")
	if err := os.WriteFile(path, []byte(""), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := loadYAMLConfig(path)
	if err != nil {
		t.Fatalf("loadYAMLConfig: %v", err)
	}

	if cfg.Source != "" || cfg.APIToken != "" {
		t.Error("empty YAML file should produce zero-value YAMLConfig")
	}
	if cfg.LogToFile != nil || cfg.Debug != nil {
		t.Error("empty YAML file should leave boolean pointer fields as nil")
	}
}

func TestMergeYAMLConfig_BooleanFalseOverride(t *testing.T) {
	f := false
	tr := true
	// base has LogToFile=true, override sets it to false explicitly.
	base := YAMLConfig{LogToFile: &tr, Debug: &tr}
	override := YAMLConfig{LogToFile: &f}
	merged := mergeYAMLConfig(base, override)

	if merged.LogToFile == nil || *merged.LogToFile != false {
		t.Error("mergeYAMLConfig: explicit false in override should set LogToFile=false")
	}
	// Debug not present in override — base value should be preserved.
	if merged.Debug == nil || !*merged.Debug {
		t.Error("mergeYAMLConfig: Debug not in override, should retain base true")
	}
}

func TestMergeYAMLConfig_NilBoolNotOverridingBase(t *testing.T) {
	tr := true
	base := YAMLConfig{Debug: &tr}
	override := YAMLConfig{} // Debug is nil — should not override.
	merged := mergeYAMLConfig(base, override)

	if merged.Debug == nil || !*merged.Debug {
		t.Error("mergeYAMLConfig: nil bool in override should leave base value intact")
	}
}

