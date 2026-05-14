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

	"github.com/ctreminiom/go-atlassian/v2/pkg/infra/models"
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
	processBitbucketEvents(pages, nopLogger(), nil, "", nil)
}

func TestProcessBitbucketEvents_EmptyPages(t *testing.T) {
	processBitbucketEvents(nil, nopLogger(), nil, "", nil)
	processBitbucketEvents([]BitbucketAuditPage{}, nopLogger(), nil, "", nil)
	processBitbucketEvents([]BitbucketAuditPage{{Values: nil}}, nopLogger(), nil, "", nil)
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
	processEvents(chunks, nopLogger(), nil, "", nil, nil)
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
	processEvents(chunks, nopLogger(), nil, "", nil, nil)
}

func TestProcessEvents_Empty(t *testing.T) {
	processEvents(nil, nopLogger(), nil, "", nil, nil)
	processEvents([]*models.OrganizationEventPageScheme{}, nopLogger(), nil, "", nil, nil)
}

// ---------- processJiraAuditRecords ----------

func TestProcessJiraAuditRecords_NilObjectItem(t *testing.T) {
	pages := []*models.AuditRecordPageScheme{
		{
			Records: []*models.AuditRecordScheme{
				{
					ID:              1,
					Summary:         "User created",
					AuthorAccountID: "jdoe-account-id",
					Created:         "2024-06-01T10:00:00.000+0000",
					Category:        "user management",
					RemoteAddress:   "192.0.2.1",
					ObjectItem:      nil,
				},
			},
		},
	}
	processJiraAuditRecords(pages, nopLogger(), nil, "", nil, nil, nil)
}

func TestProcessJiraAuditRecords_WithObjectItem(t *testing.T) {
	pages := []*models.AuditRecordPageScheme{
		{
			Records: []*models.AuditRecordScheme{
				{
					ID:              2,
					Summary:         "Project created",
					AuthorAccountID: "admin-account-id",
					Created:         "2024-06-02T08:00:00.000+0000",
					Category:        "project",
					ObjectItem: &models.AuditRecordObjectItemScheme{
						Name:     "MyProject",
						TypeName: "PROJECT",
					},
				},
			},
		},
	}
	processJiraAuditRecords(pages, nopLogger(), nil, "", nil, nil, nil)
}

func TestProcessJiraAuditRecords_Empty(t *testing.T) {
	processJiraAuditRecords(nil, nopLogger(), nil, "", nil, nil, nil)
	processJiraAuditRecords([]*models.AuditRecordPageScheme{}, nopLogger(), nil, "", nil, nil, nil)
}

// TestProcessJiraAuditRecords_AuthorAccountIDResolved checks that when AuthorAccountID
// is populated and a resolver is provided, the resolver is called and the display
// name is surfaced.
func TestProcessJiraAuditRecords_AuthorAccountIDResolved(t *testing.T) {
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
					ID:              10,
					Summary:         "User login",
					AuthorAccountID: "58da8718-09f4-4f36-9d83-3ae82796ae3e",
					Created:         "2024-06-01T10:00:00.000+0000",
					Category:        "user management",
				},
			},
		},
	}
	processJiraAuditRecords(pages, nopLogger(), nil, "", resolver, nil, nil)
}

// TestProcessJiraAuditRecords_EmptyAuthorAccountIDSkipsResolver checks that an
// empty AuthorAccountID does not trigger resolver calls.
func TestProcessJiraAuditRecords_EmptyAuthorAccountIDSkipsResolver(t *testing.T) {
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
					ID:       11,
					Summary:  "Group updated",
					Created:  "2024-06-01T12:00:00.000+0000",
					Category: "group management",
				},
			},
		},
	}
	processJiraAuditRecords(pages, nopLogger(), nil, "", resolver, nil, nil)

	if called {
		t.Error("resolver should not be called when AuthorAccountID is empty")
	}
}

// TestProcessJiraAuditRecords_NilResolverAuthorAccountID checks that a nil resolver
// with a populated AuthorAccountID does not panic.
func TestProcessJiraAuditRecords_NilResolverAuthorAccountID(t *testing.T) {
	pages := []*models.AuditRecordPageScheme{
		{
			Records: []*models.AuditRecordScheme{
				{
					ID:              12,
					Summary:         "User login",
					AuthorAccountID: "some-account-id",
					Created:         "2024-06-01T13:00:00.000+0000",
					Category:        "user management",
				},
			},
		},
	}
	processJiraAuditRecords(pages, nopLogger(), nil, "", nil, nil, nil)
}

// TestProcessJiraAuditRecords_UgUUIDAuthorUsesMigrationResolver verifies that a
// ug:UUID AuthorAccountID is routed to the migration resolver, not the regular one.
func TestProcessJiraAuditRecords_UgUUIDAuthorUsesMigrationResolver(t *testing.T) {
	migrationCalls, regularCalls := 0, 0

	migrationSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		migrationCalls++
		_, _ = w.Write([]byte(`[{"key":"ug:ug-uuid-author","accountId":"acc-123","username":"alice_migration"}]`))
	}))
	defer migrationSrv.Close()

	regularSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		regularCalls++
		_, _ = w.Write([]byte(`{"displayName":"ShouldNotBeUsed"}`))
	}))
	defer regularSrv.Close()

	migrationResolver := newJiraBulkMigrationUserResolver(migrationSrv.URL, "u", "t", migrationSrv.Client(), nopLogger())
	regularResolver := newJiraUserResolver(regularSrv.URL, "u", "t", regularSrv.Client(), nopLogger())

	pages := []*models.AuditRecordPageScheme{{
		Records: []*models.AuditRecordScheme{{
			ID:              20,
			Summary:         "User login",
			AuthorAccountID: "ug:ug-uuid-author",
			Created:         "2024-06-01T10:00:00.000+0000",
			Category:        "user management",
		}},
	}}
	processJiraAuditRecords(pages, nopLogger(), nil, "", regularResolver, migrationResolver, nil)

	if migrationCalls == 0 {
		t.Error("migration resolver should have been called for ug:UUID author")
	}
	if regularCalls != 0 {
		t.Errorf("regular resolver should not be called for ug:UUID author, got %d calls", regularCalls)
	}
}

// TestProcessJiraAuditRecords_UgUUIDInChangedValues verifies that ug:UUID values in
// ChangedFrom/ChangedTo are resolved via the migration resolver.
func TestProcessJiraAuditRecords_UgUUIDInChangedValues(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_, _ = w.Write([]byte(`[{"key":"ug:cv-user","accountId":"acc-cv","username":"cv_user"}]`))
	}))
	defer server.Close()

	migrationResolver := newJiraBulkMigrationUserResolver(server.URL, "u", "t", server.Client(), nopLogger())

	pages := []*models.AuditRecordPageScheme{{
		Records: []*models.AuditRecordScheme{{
			ID:      30,
			Summary: "Assignee changed",
			Created: "2024-06-01T10:00:00.000+0000",
			ChangedValues: []*models.AuditRecordChangedValueScheme{
				{FieldName: "assignee", ChangedFrom: "ug:old-user-uuid", ChangedTo: "ug:new-user-uuid"},
			},
		}},
	}}
	processJiraAuditRecords(pages, nopLogger(), nil, "", nil, migrationResolver, nil)

	// Two distinct IDs → two API calls (cache misses).
	if calls == 0 {
		t.Error("migration resolver should have been called for ug:UUID in ChangedValues")
	}
}

// TestProcessJiraAuditRecords_UgUUIDInObjectItemID verifies that a ug:UUID in
// ObjectItem.ID is resolved via the migration resolver.
func TestProcessJiraAuditRecords_UgUUIDInObjectItemID(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_, _ = w.Write([]byte(`[{"key":"ug:obj-user","accountId":"acc-obj","username":"obj_user"}]`))
	}))
	defer server.Close()

	migrationResolver := newJiraBulkMigrationUserResolver(server.URL, "u", "t", server.Client(), nopLogger())

	pages := []*models.AuditRecordPageScheme{{
		Records: []*models.AuditRecordScheme{{
			ID:      40,
			Summary: "User modified",
			Created: "2024-06-01T10:00:00.000+0000",
			ObjectItem: &models.AuditRecordObjectItemScheme{
				ID:       "ug:obj-user",
				Name:     "obj.user@example.com",
				TypeName: "USER",
			},
		}},
	}}
	processJiraAuditRecords(pages, nopLogger(), nil, "", nil, migrationResolver, nil)

	if calls == 0 {
		t.Error("migration resolver should have been called for ug:UUID in ObjectItem.ID")
	}
}

// TestProcessJiraAuditRecords_UgUUIDInAssociatedItems verifies that a ug:UUID in
// AssociatedItems[*].ID is resolved via the migration resolver.
func TestProcessJiraAuditRecords_UgUUIDInAssociatedItems(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_, _ = w.Write([]byte(`[{"key":"ug:assoc-user","accountId":"acc-assoc","username":"assoc_user"}]`))
	}))
	defer server.Close()

	migrationResolver := newJiraBulkMigrationUserResolver(server.URL, "u", "t", server.Client(), nopLogger())

	pages := []*models.AuditRecordPageScheme{{
		Records: []*models.AuditRecordScheme{{
			ID:      50,
			Summary: "Group membership changed",
			Created: "2024-06-01T10:00:00.000+0000",
			AssociatedItems: []*models.AuditRecordAssociatedItemScheme{
				{ID: "ug:assoc-user", Name: "assoc.user@example.com", TypeName: "USER"},
			},
		}},
	}}
	processJiraAuditRecords(pages, nopLogger(), nil, "", nil, migrationResolver, nil)

	if calls == 0 {
		t.Error("migration resolver should have been called for ug:UUID in AssociatedItems ID")
	}
}

// TestProcessJiraAuditRecords_UgUUIDInAssocItemsName verifies that a ug:UUID in
// AssociatedItems[*].Name is also resolved via the migration resolver.  This
// mirrors the real Jira API behaviour where both id and name echo the same ug:UUID.
func TestProcessJiraAuditRecords_UgUUIDInAssocItemsName(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_, _ = w.Write([]byte(`[{"key":"ug:name-user","accountId":"acc-name","username":"name_user"}]`))
	}))
	defer server.Close()

	migrationResolver := newJiraBulkMigrationUserResolver(server.URL, "u", "t", server.Client(), nopLogger())

	pages := []*models.AuditRecordPageScheme{{
		Records: []*models.AuditRecordScheme{{
			ID:      51,
			Summary: "User added to group",
			Created: "2024-06-01T10:00:00.000+0000",
			AssociatedItems: []*models.AuditRecordAssociatedItemScheme{
				// Both id and name carry the ug:UUID (as Jira sometimes returns).
				{ID: "ug:name-user", Name: "ug:name-user", TypeName: "USER"},
			},
		}},
	}}
	processJiraAuditRecords(pages, nopLogger(), nil, "", nil, migrationResolver, nil)

	if calls == 0 {
		t.Error("migration resolver should have been called for ug:UUID in AssociatedItems Name")
	}
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
	processConfluenceAuditRecords(pages, nopLogger(), nil, "", nil, nil, nil)
}

func TestProcessConfluenceAuditRecords_Empty(t *testing.T) {
	processConfluenceAuditRecords(nil, nopLogger(), nil, "", nil, nil, nil)
	processConfluenceAuditRecords([]ConfluenceAuditPage{}, nopLogger(), nil, "", nil, nil, nil)
	processConfluenceAuditRecords([]ConfluenceAuditPage{{Results: nil}}, nopLogger(), nil, "", nil, nil, nil)
}

func TestProcessConfluenceAuditRecords_WithChangedValues(t *testing.T) {
	pages := []ConfluenceAuditPage{{
		Results: []ConfluenceAuditRecord{{
			Author:       ConfluenceAuditAuthor{DisplayName: "Alice", AccountID: "acc-alice"},
			CreationDate: 1717228800000,
			Summary:      "Space permissions updated",
			Category:     "permissions",
			ChangedValues: []ConfluenceChangedValue{
				{Name: "View", OldValue: "false", NewValue: "true"},
				{Name: "Edit", OldValue: "true", NewValue: "false"},
			},
		}},
	}}
	// Must not panic and must process both changed values.
	processConfluenceAuditRecords(pages, nopLogger(), nil, "", nil, nil, nil)
}

func TestProcessConfluenceAuditRecords_WithAssociatedObjects(t *testing.T) {
	pages := []ConfluenceAuditPage{{
		Results: []ConfluenceAuditRecord{{
			Author:       ConfluenceAuditAuthor{DisplayName: "Bob", AccountID: "acc-bob"},
			CreationDate: 1717228800000,
			Summary:      "Page moved",
			Category:     "content",
			AssociatedObjects: []ConfluenceAssociatedObject{
				{Name: "Engineering", ObjectType: "Space"},
				{Name: "My Page", ObjectType: "Page"},
			},
		}},
	}}
	processConfluenceAuditRecords(pages, nopLogger(), nil, "", nil, nil, nil)
}

func TestProcessConfluenceAuditRecords_ChangedValuesAndAssociatedObjects(t *testing.T) {
	pages := []ConfluenceAuditPage{{
		Results: []ConfluenceAuditRecord{{
			Author:       ConfluenceAuditAuthor{DisplayName: "Carol", AccountID: "acc-carol"},
			CreationDate: 1717228800000,
			Summary:      "User added to space",
			Category:     "permissions",
			AffectedObject: ConfluenceAuditObject{Name: "Engineering", ObjectType: "Space"},
			ChangedValues: []ConfluenceChangedValue{
				{Name: "Role", OldValue: "", NewValue: "contributor"},
			},
			AssociatedObjects: []ConfluenceAssociatedObject{
				{Name: "carol@example.com", ObjectType: "User"},
			},
		}},
	}}
	processConfluenceAuditRecords(pages, nopLogger(), nil, "", nil, nil, nil)
}

// ---------- parseGroupUserName ----------

func TestParseGroupUserName_ValidPattern(t *testing.T) {
	groupID, userID := parseGroupUserName("e3d93ec5-456e-4f9f-8a75-17196dd84e00; User: 712020:bf43f3d5-be22-4709-8312-1af251228d9d")
	if groupID != "e3d93ec5-456e-4f9f-8a75-17196dd84e00" {
		t.Errorf("groupID: got %q, want UUID", groupID)
	}
	if userID != "712020:bf43f3d5-be22-4709-8312-1af251228d9d" {
		t.Errorf("userID: got %q, want account ID", userID)
	}
}

func TestParseGroupUserName_NoPattern(t *testing.T) {
	groupID, userID := parseGroupUserName("Engineering")
	if groupID != "" || userID != "" {
		t.Errorf("expected empty strings for plain name, got %q %q", groupID, userID)
	}
}

func TestParseGroupUserName_EmptyString(t *testing.T) {
	groupID, userID := parseGroupUserName("")
	if groupID != "" || userID != "" {
		t.Errorf("expected empty strings for empty input, got %q %q", groupID, userID)
	}
}

// ---------- ConfluenceGroupResolver ----------

func TestConfluenceGroupResolver_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rest/api/group/by-id" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		if got := r.URL.Query().Get("id"); got != "e3d93ec5-0000-0000-0000-000000000000" {
			t.Errorf("unexpected id param %q", got)
		}
		_, _ = w.Write([]byte(`{"id":"e3d93ec5-0000-0000-0000-000000000000","name":"engineering-team","type":"group"}`))
	}))
	defer server.Close()

	resolver := newConfluenceGroupResolver(server.URL, "u", "t", server.Client(), nopLogger())
	got := resolver.resolve("e3d93ec5-0000-0000-0000-000000000000")
	if got != "engineering-team" {
		t.Errorf("got %q, want %q", got, "engineering-team")
	}
}

func TestConfluenceGroupResolver_Cache(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_, _ = w.Write([]byte(`{"name":"cached-group"}`))
	}))
	defer server.Close()

	resolver := newConfluenceGroupResolver(server.URL, "u", "t", server.Client(), nopLogger())
	resolver.resolve("group-uuid")
	resolver.resolve("group-uuid")
	if calls != 1 {
		t.Errorf("expected 1 API call due to caching, got %d", calls)
	}
}

// ---------- ConfluenceUserResolver ----------

func TestConfluenceUserResolver_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rest/api/user" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		if got := r.URL.Query().Get("accountId"); got != "712020:abc" {
			t.Errorf("unexpected accountId param %q", got)
		}
		_, _ = w.Write([]byte(`{"accountId":"712020:abc","displayName":"Diana Rybanska"}`))
	}))
	defer server.Close()

	resolver := newConfluenceUserResolver(server.URL, "u", "t", server.Client(), nopLogger())
	got := resolver.resolve("712020:abc")
	if got != "Diana Rybanska" {
		t.Errorf("got %q, want %q", got, "Diana Rybanska")
	}
}

// ---------- processConfluenceAuditRecords with resolvers ----------

func TestProcessConfluenceAuditRecords_GroupUserResolution(t *testing.T) {
	const groupName = "engineering-team"
	const userName = "Diana Rybanska"
	groupCalls, userCalls := 0, 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/rest/api/group/by-id":
			groupCalls++
			_, _ = w.Write([]byte(`{"name":"` + groupName + `"}`))
		case "/rest/api/user":
			userCalls++
			_, _ = w.Write([]byte(`{"displayName":"` + userName + `"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	groupResolver := newConfluenceGroupResolver(server.URL, "u", "t", server.Client(), nopLogger())
	userResolver := newConfluenceUserResolver(server.URL, "u", "t", server.Client(), nopLogger())

	pages := []ConfluenceAuditPage{{
		Results: []ConfluenceAuditRecord{{
			Author:       ConfluenceAuditAuthor{DisplayName: "Admin"},
			CreationDate: 1717228800000,
			Summary:      "User removed from group",
			Category:     "group management",
			AffectedObject: ConfluenceAuditObject{
				Name:       "e3d93ec5-456e-4f9f-8a75-17196dd84e00; User: 712020:bf43f3d5-be22-4709-8312-1af251228d9d",
				ObjectType: "Group",
			},
		}},
	}}
	processConfluenceAuditRecords(pages, nopLogger(), nil, "", groupResolver, userResolver, nil)

	if groupCalls == 0 {
		t.Error("group resolver should have been called")
	}
	if userCalls == 0 {
		t.Error("user resolver should have been called")
	}
}

func TestProcessConfluenceAuditRecords_PlainAffectedObjectSkipsResolvers(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	groupResolver := newConfluenceGroupResolver(server.URL, "u", "t", server.Client(), nopLogger())
	userResolver := newConfluenceUserResolver(server.URL, "u", "t", server.Client(), nopLogger())

	pages := []ConfluenceAuditPage{{
		Results: []ConfluenceAuditRecord{{
			Author:         ConfluenceAuditAuthor{DisplayName: "Admin"},
			CreationDate:   1717228800000,
			Summary:        "Space created",
			AffectedObject: ConfluenceAuditObject{Name: "Engineering", ObjectType: "Space"},
		}},
	}}
	processConfluenceAuditRecords(pages, nopLogger(), nil, "", groupResolver, userResolver, nil)

	if called {
		t.Error("resolvers should not be called when affectedObject.name has no '; User: ' pattern")
	}
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
fluentbit_enabled: true
fluentbit_host: fluentbit.example.com
fluentbit_port: 9880
fluentbit_tag: atlassian
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
		{"FluentBitHost", cfg.FluentBitHost, "fluentbit.example.com"},
		{"FluentBitTag", cfg.FluentBitTag, "atlassian"},
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
	if cfg.FluentBitEnabled == nil || !*cfg.FluentBitEnabled {
		t.Error("FluentBitEnabled: got nil or false, want true")
	}
	if cfg.FluentBitPort != 9880 {
		t.Errorf("FluentBitPort: got %d, want 9880", cfg.FluentBitPort)
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
	processEvents(chunks, nopLogger(), nil, "", resolver, nil)
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

func TestJiraUserResolver_CleanAccountID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("accountId"); got != "stripped-id" {
			t.Errorf("unexpected accountId=%q, want %q", got, "stripped-id")
		}
		_, _ = w.Write([]byte(`{"displayName":"Bob Jira"}`))
	}))
	defer server.Close()

	resolver := newJiraUserResolver(server.URL, "user@example.com", "token", server.Client(), nopLogger())
	got := resolver.resolve("stripped-id")
	if got != "Bob Jira" {
		t.Errorf("got %q, want %q", got, "Bob Jira")
	}
}


// ---------- JiraBulkMigrationUserResolver ----------

// TestJiraBulkMigrationResolver_UsernamePresent verifies the happy path for
// Jira Server/DC where the bulk migration API returns a non-empty username.
func TestJiraBulkMigrationResolver_UsernamePresent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"key":"ug:abc","accountId":"acc-abc","username":"jdoe"}]`))
	}))
	defer server.Close()

	resolver := newJiraBulkMigrationUserResolver(server.URL, "u", "t", server.Client(), nopLogger())
	got := resolver.resolve("ug:abc")
	if got != "jdoe" {
		t.Errorf("got %q, want %q", got, "jdoe")
	}
}

// TestJiraBulkMigrationResolver_EmptyUsername_FallsBackToUserAPI verifies the
// Jira Cloud path: bulk migration returns an empty username but a valid accountId,
// so the resolver must do a second call to the Jira user API to get the displayName.
func TestJiraBulkMigrationResolver_EmptyUsername_FallsBackToUserAPI(t *testing.T) {
	const wantName = "Alice Cloud"
	const accountID = "cloud-account-id"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/rest/api/3/user/bulk/migration":
			_, _ = w.Write([]byte(`[{"key":"ug:cloud-uuid","accountId":"` + accountID + `","username":""}]`))
		case "/rest/api/2/user":
			if r.URL.Query().Get("accountId") == accountID {
				_, _ = w.Write([]byte(`{"displayName":"` + wantName + `"}`))
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	resolver := newJiraBulkMigrationUserResolver(server.URL, "u", "t", server.Client(), nopLogger())
	got := resolver.resolve("ug:cloud-uuid")
	if got != wantName {
		t.Errorf("got %q, want %q", got, wantName)
	}
}

// TestJiraBulkMigrationResolver_EmptyResponse returns empty string when no users found.
func TestJiraBulkMigrationResolver_EmptyResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[]`))
	}))
	defer server.Close()

	resolver := newJiraBulkMigrationUserResolver(server.URL, "u", "t", server.Client(), nopLogger())
	got := resolver.resolve("ug:nonexistent")
	if got != "" {
		t.Errorf("got %q, want empty string", got)
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

// ---------- FluentBitClient ----------

// TestInitFluentBit_Disabled verifies that initFluentBit returns nil when disabled.
func TestInitFluentBit_Disabled(t *testing.T) {
	cfg := Config{FluentBitEnabled: false}
	client := initFluentBit(cfg, nopLogger())
	if client != nil {
		t.Error("expected nil FluentBitClient when disabled")
	}
}

// TestInitFluentBit_DefaultTag checks that the source name is used as the tag
// when FluentBitTag is not set.
func TestInitFluentBit_DefaultTag(t *testing.T) {
	cfg := Config{
		FluentBitEnabled: true,
		FluentBitHost:    "localhost",
		FluentBitPort:    9880,
		Source:           "jira",
	}
	client := initFluentBit(cfg, nopLogger())
	if client == nil {
		t.Fatal("expected non-nil FluentBitClient")
	}
	if !strings.HasSuffix(client.url, "/jira") {
		t.Errorf("expected URL to end with /jira, got %q", client.url)
	}
}

// TestInitFluentBit_CustomTag checks that an explicit FluentBitTag is used.
func TestInitFluentBit_CustomTag(t *testing.T) {
	cfg := Config{
		FluentBitEnabled: true,
		FluentBitHost:    "localhost",
		FluentBitPort:    9880,
		Source:           "admin",
		FluentBitTag:     "my-tag",
	}
	client := initFluentBit(cfg, nopLogger())
	if client == nil {
		t.Fatal("expected non-nil FluentBitClient")
	}
	if !strings.HasSuffix(client.url, "/my-tag") {
		t.Errorf("expected URL to end with /my-tag, got %q", client.url)
	}
}

// TestSendFluentBit_NilClient verifies that a nil client is a no-op.
func TestSendFluentBit_NilClient(t *testing.T) {
	// Should not panic.
	sendFluentBit(nil, map[string]interface{}{"_key": "value"}, nopLogger())
}

// TestSendFluentBit_PostsJSON verifies that the client POSTs well-formed JSON
// with underscore-stripped field names.
func TestSendFluentBit_PostsJSON(t *testing.T) {
	var receivedBody []byte
	var receivedContentType string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedContentType = r.Header.Get("Content-Type")
		var err error
		receivedBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := &FluentBitClient{
		httpClient: server.Client(),
		url:        server.URL + "/test-tag",
		log:        nopLogger(),
	}

	sendFluentBit(client, map[string]interface{}{
		"_source": "admin",
		"_action": "user_login",
	}, nopLogger())

	if receivedContentType != "application/json" {
		t.Errorf("Content-Type: got %q, want application/json", receivedContentType)
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(receivedBody, &payload); err != nil {
		t.Fatalf("body is not valid JSON: %v", err)
	}

	if v, ok := payload["source"]; !ok || v != "admin" {
		t.Errorf("expected payload[\"source\"] = \"admin\", got %v", payload["source"])
	}
	if v, ok := payload["action"]; !ok || v != "user_login" {
		t.Errorf("expected payload[\"action\"] = \"user_login\", got %v", payload["action"])
	}
	// Underscore-prefixed keys must not appear.
	if _, ok := payload["_source"]; ok {
		t.Error("field \"_source\" must be stripped to \"source\"")
	}
}

// TestSendFluentBit_UnexpectedStatus verifies a warning is logged on non-2xx.
func TestSendFluentBit_UnexpectedStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := &FluentBitClient{
		httpClient: server.Client(),
		url:        server.URL + "/tag",
		log:        nopLogger(),
	}

	// Must not panic.
	sendFluentBit(client, map[string]interface{}{"_key": "val"}, nopLogger())
}


