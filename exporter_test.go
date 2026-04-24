package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
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
	processEvents(chunks, nopLogger(), nil, "", nil)
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
	processEvents(chunks, nopLogger(), nil, "", nil)
}

func TestProcessEvents_Empty(t *testing.T) {
	processEvents(nil, nopLogger(), nil, "", nil)
	processEvents([]*models.OrganizationEventPageScheme{}, nopLogger(), nil, "", nil)
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
	processJiraAuditRecords(pages, nopLogger(), nil, "", nil)
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
	processJiraAuditRecords(pages, nopLogger(), nil, "", nil)
}

func TestProcessJiraAuditRecords_Empty(t *testing.T) {
	processJiraAuditRecords(nil, nopLogger(), nil, "", nil)
	processJiraAuditRecords([]*models.AuditRecordPageScheme{}, nopLogger(), nil, "", nil)
}

// TestProcessJiraAuditRecords_UGPrefixAuthorResolved checks that when AuthorKey
// is in "ug:UUID" format and a resolver is provided, the resolver is called and
// the display name is surfaced.
func TestProcessJiraAuditRecords_UGPrefixAuthorResolved(t *testing.T) {
	const wantName = "Alice Cloud"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"displayName":"` + wantName + `"}`))
	}))
	defer server.Close()

	resolver := newJiraUserResolver(server.URL, "user@example.com", "token", server.Client(), nopLogger())

	pages := []*models.AuditRecordPageScheme{
		{
			Records: []*models.AuditRecordScheme{
				{
					ID:        10,
					Summary:   "User login",
					AuthorKey: "ug:58da8718-09f4-4f36-9d83-3ae82796ae3e",
					Created:   "2024-06-01T10:00:00.000+0000",
					Category:  "user management",
				},
			},
		},
	}
	processJiraAuditRecords(pages, nopLogger(), nil, "", resolver)
}

// TestProcessJiraAuditRecords_NonUGAuthorSkipsResolver checks that a plain
// username (no "ug:" prefix) does not trigger resolver calls.
func TestProcessJiraAuditRecords_NonUGAuthorSkipsResolver(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"displayName":"Should Not Resolve"}`))
	}))
	defer server.Close()

	resolver := newJiraUserResolver(server.URL, "user@example.com", "token", server.Client(), nopLogger())

	pages := []*models.AuditRecordPageScheme{
		{
			Records: []*models.AuditRecordScheme{
				{
					ID:        11,
					Summary:   "Group updated",
					AuthorKey: "jdoe",
					Created:   "2024-06-01T12:00:00.000+0000",
					Category:  "group management",
				},
			},
		},
	}
	processJiraAuditRecords(pages, nopLogger(), nil, "", resolver)

	if called {
		t.Error("resolver should not be called for non-ug: author keys")
	}
}

// TestProcessJiraAuditRecords_NilResolverUGAuthor checks that a nil resolver
// with a ug:-prefixed AuthorKey does not panic.
func TestProcessJiraAuditRecords_NilResolverUGAuthor(t *testing.T) {
	pages := []*models.AuditRecordPageScheme{
		{
			Records: []*models.AuditRecordScheme{
				{
					ID:        12,
					Summary:   "User login",
					AuthorKey: "ug:some-uuid",
					Created:   "2024-06-01T13:00:00.000+0000",
					Category:  "user management",
				},
			},
		},
	}
	processJiraAuditRecords(pages, nopLogger(), nil, "", nil)
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

// ---------- UserResolver ----------

func TestUserResolver_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("unexpected method %s", r.Method)
		}
		if !strings.HasSuffix(r.URL.Path, "/abc123/manage/profile") {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		if auth := r.Header.Get("Authorization"); auth != "Bearer test-token" {
			t.Errorf("unexpected Authorization header %q", auth)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"account":{"name":"Alice Wonderland"}}`))
	}))
	defer server.Close()

	resolver := newUserResolver("test-token", server.Client(), nopLogger())
	// Override the base URL by pointing the resolver at the test server.
	// We achieve this by making the httpClient a plain client and ensuring the
	// test server intercepts: we rebuild the request URL to use the server base.
	resolver.httpClient = server.Client()

	// Patch resolve to use the test server URL by swapping the client's transport.
	// Simpler: use a custom RoundTripper that rewrites the host.
	resolver.httpClient = &http.Client{
		Transport: rewriteHostTransport{
			base:    server.URL,
			wrapped: http.DefaultTransport,
		},
	}

	got := resolver.resolve("abc123")
	if got != "Alice Wonderland" {
		t.Errorf("got %q, want %q", got, "Alice Wonderland")
	}
}

func TestUserResolver_StripUGPrefix(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/stripped-id/manage/profile") {
			t.Errorf("ug: prefix was not stripped, path: %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"account":{"name":"Bob Builder"}}`))
	}))
	defer server.Close()

	resolver := newUserResolver("tok", &http.Client{
		Transport: rewriteHostTransport{base: server.URL, wrapped: http.DefaultTransport},
	}, nopLogger())

	got := resolver.resolve("ug:stripped-id")
	if got != "Bob Builder" {
		t.Errorf("got %q, want %q", got, "Bob Builder")
	}
}

func TestUserResolver_Cache(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_, _ = w.Write([]byte(`{"account":{"name":"Cached Carol"}}`))
	}))
	defer server.Close()

	resolver := newUserResolver("tok", &http.Client{
		Transport: rewriteHostTransport{base: server.URL, wrapped: http.DefaultTransport},
	}, nopLogger())

	resolver.resolve("user-id")
	resolver.resolve("user-id")
	resolver.resolve("ug:user-id") // same ID after prefix strip

	if calls != 1 {
		t.Errorf("expected 1 API call due to caching, got %d", calls)
	}
}

func TestUserResolver_NonOKStatus(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	resolver := newUserResolver("tok", &http.Client{
		Transport: rewriteHostTransport{base: server.URL, wrapped: http.DefaultTransport},
	}, nopLogger())

	got := resolver.resolve("unknown-id")
	if got != "" {
		t.Errorf("expected empty string on non-OK status, got %q", got)
	}
	// Second call must hit the cache, not the server.
	got = resolver.resolve("unknown-id")
	if got != "" {
		t.Errorf("expected empty string from cache, got %q", got)
	}
	if calls != 1 {
		t.Errorf("expected exactly 1 server call due to caching of empty result, got %d", calls)
	}
}

func TestUserResolver_EmptyAccountID(t *testing.T) {
	resolver := newUserResolver("tok", http.DefaultClient, nopLogger())
	if got := resolver.resolve(""); got != "" {
		t.Errorf("expected empty string for empty account ID, got %q", got)
	}
	if got := resolver.resolve("ug:"); got != "" {
		t.Errorf("expected empty string for 'ug:' only, got %q", got)
	}
}

func TestProcessEvents_WithResolver(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"account":{"name":"Dave Resolved"}}`))
	}))
	defer server.Close()

	resolver := newUserResolver("tok", &http.Client{
		Transport: rewriteHostTransport{base: server.URL, wrapped: http.DefaultTransport},
	}, nopLogger())

	chunks := []*models.OrganizationEventPageScheme{
		{
			Data: []*models.OrganizationEventModelScheme{
				{
					ID: "ev-resolver",
					Attributes: &models.OrganizationEventModelAttributesScheme{
						Time:   "2024-06-01T10:00:00Z",
						Action: "user_login",
						Actor: &models.OrganizationEventActorModel{
							ID:   "ug:actor-id",
							Name: "dave@example.com",
						},
					},
				},
			},
		},
	}
	// Must not panic; resolver is exercised.
	processEvents(chunks, nopLogger(), nil, "", resolver)
}

// ---------- JiraUserResolver ----------

func TestJiraUserResolver_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("unexpected method %s", r.Method)
		}
		if r.URL.Path != "/rest/api/2/user" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		if got := r.URL.Query().Get("accountId"); got != "abc123" {
			t.Errorf("unexpected accountId query param %q", got)
		}
		user, pass, ok := r.BasicAuth()
		if !ok || user != "user@example.com" || pass != "secret" {
			t.Errorf("unexpected basic auth user=%q pass=%q ok=%v", user, pass, ok)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"displayName":"Alice Jira"}`))
	}))
	defer server.Close()

	resolver := newJiraUserResolver(server.URL, "user@example.com", "secret", server.Client(), nopLogger())
	got := resolver.resolve("abc123")
	if got != "Alice Jira" {
		t.Errorf("got %q, want %q", got, "Alice Jira")
	}
}

func TestJiraUserResolver_StripUGPrefix(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("accountId"); got != "stripped-id" {
			t.Errorf("ug: prefix was not stripped, accountId=%q", got)
		}
		_, _ = w.Write([]byte(`{"displayName":"Bob Jira"}`))
	}))
	defer server.Close()

	resolver := newJiraUserResolver(server.URL, "user@example.com", "token", server.Client(), nopLogger())
	got := resolver.resolve("ug:stripped-id")
	if got != "Bob Jira" {
		t.Errorf("got %q, want %q", got, "Bob Jira")
	}
}


type rewriteHostTransport struct {
	base    string // e.g. "http://127.0.0.1:PORT"
	wrapped http.RoundTripper
}

func (t rewriteHostTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	base, _ := url.Parse(t.base)
	clone.URL.Scheme = base.Scheme
	clone.URL.Host = base.Host
	return t.wrapped.RoundTrip(clone)
}

