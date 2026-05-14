package main

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func tempAuditLogger(t *testing.T) (*AuditLogger, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.jsonl")
	al, err := NewAuditLogger(path)
	if err != nil {
		t.Fatalf("NewAuditLogger: %v", err)
	}
	t.Cleanup(func() { al.Close() })
	return al, path
}

func sampleRecord(eventType string, ts time.Time) AuditRecord {
	return AuditRecord{
		EventID:       "id-1",
		EventType:     eventType,
		CorrelationID: "corr-1",
		Source:        "test-service",
		Timestamp:     ts,
		ReceivedAt:    ts,
	}
}

func TestAuditLogger_WritesJSONLines(t *testing.T) {
	al, path := tempAuditLogger(t)
	now := time.Now().UTC().Truncate(time.Second)
	if err := al.Write(sampleRecord("test.event", now)); err != nil {
		t.Fatal(err)
	}
	al.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(lines))
	}
	var rec AuditRecord
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if rec.EventType != "test.event" {
		t.Errorf("EventType = %q", rec.EventType)
	}
}

func TestAuditLogger_MultipleRecords(t *testing.T) {
	al, path := tempAuditLogger(t)
	now := time.Now().UTC()
	for i := 0; i < 5; i++ {
		if err := al.Write(sampleRecord("event", now)); err != nil {
			t.Fatal(err)
		}
	}
	al.Close()

	data, _ := os.ReadFile(path)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 5 {
		t.Fatalf("expected 5 lines, got %d", len(lines))
	}
}

func TestAuditLogger_Rotation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.jsonl")
	al, err := NewAuditLogger(path)
	if err != nil {
		t.Fatal(err)
	}
	defer al.Close()

	// Write a record then simulate a day change by manipulating al.date.
	now := time.Now().UTC()
	if err := al.Write(sampleRecord("before.rotate", now)); err != nil {
		t.Fatal(err)
	}

	yesterday := now.AddDate(0, 0, -1).Format("2006-01-02")
	al.mu.Lock()
	al.date = yesterday
	al.mu.Unlock()

	// Write triggers rotation.
	if err := al.Write(sampleRecord("after.rotate", now)); err != nil {
		t.Fatal(err)
	}

	// Rotated file must exist.
	rotated := filepath.Join(dir, "audit-"+yesterday+".jsonl")
	if _, err := os.Stat(rotated); os.IsNotExist(err) {
		t.Fatalf("rotated file %s not found", rotated)
	}

	// Current file must have only the post-rotation record.
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	var count int
	for scanner.Scan() {
		count++
	}
	if count != 1 {
		t.Errorf("expected 1 record in new log, got %d", count)
	}
}

func TestAuditLogger_PurgeOld(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.jsonl")

	// Pre-create an old rotated file (31 days ago).
	old := time.Now().UTC().AddDate(0, 0, -31).Format("2006-01-02")
	oldPath := filepath.Join(dir, "audit-"+old+".jsonl")
	if err := os.WriteFile(oldPath, []byte{}, 0o640); err != nil {
		t.Fatal(err)
	}

	al, err := NewAuditLogger(path)
	if err != nil {
		t.Fatal(err)
	}
	defer al.Close()

	// Simulate rotation to trigger purge.
	yesterday := time.Now().UTC().AddDate(0, 0, -1).Format("2006-01-02")
	al.mu.Lock()
	al.date = yesterday
	al.mu.Unlock()

	if err := al.Write(sampleRecord("trigger.purge", time.Now().UTC())); err != nil {
		t.Fatal(err)
	}

	// Give purge goroutine time to run.
	time.Sleep(50 * time.Millisecond)

	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Error("expected old file to be deleted")
	}
}

// HTTP handler tests.

func TestAuditHTTP_NoFilter(t *testing.T) {
	al, _ := tempAuditLogger(t)
	now := time.Now().UTC()
	_ = al.Write(sampleRecord("a.b", now))
	_ = al.Write(sampleRecord("c.d", now))
	al.file.Sync() //nolint — flush

	req := httptest.NewRequest(http.MethodGet, "/audit", nil)
	rr := httptest.NewRecorder()
	al.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	lines := strings.Split(strings.TrimSpace(rr.Body.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d: %s", len(lines), rr.Body.String())
	}
}

func TestAuditHTTP_FilterByType(t *testing.T) {
	al, _ := tempAuditLogger(t)
	now := time.Now().UTC()
	_ = al.Write(sampleRecord("a.b", now))
	_ = al.Write(sampleRecord("c.d", now))
	al.file.Sync()

	req := httptest.NewRequest(http.MethodGet, "/audit?type=a.b", nil)
	rr := httptest.NewRecorder()
	al.ServeHTTP(rr, req)

	lines := strings.Split(strings.TrimSpace(rr.Body.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected 1 filtered line, got %d", len(lines))
	}
	var rec AuditRecord
	_ = json.Unmarshal([]byte(lines[0]), &rec)
	if rec.EventType != "a.b" {
		t.Errorf("got EventType %q", rec.EventType)
	}
}

func TestAuditHTTP_FilterByTimeRange(t *testing.T) {
	al, _ := tempAuditLogger(t)
	base := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	_ = al.Write(sampleRecord("e1", base))
	_ = al.Write(sampleRecord("e2", base.Add(2*time.Hour)))
	_ = al.Write(sampleRecord("e3", base.Add(4*time.Hour)))
	al.file.Sync()

	from := base.Add(1 * time.Hour).Format(time.RFC3339)
	to := base.Add(3 * time.Hour).Format(time.RFC3339)
	req := httptest.NewRequest(http.MethodGet, "/audit?from="+from+"&to="+to, nil)
	rr := httptest.NewRecorder()
	al.ServeHTTP(rr, req)

	lines := strings.Split(strings.TrimSpace(rr.Body.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected 1 record in range, got %d: %s", len(lines), rr.Body.String())
	}
	var rec AuditRecord
	_ = json.Unmarshal([]byte(lines[0]), &rec)
	if rec.EventType != "e2" {
		t.Errorf("got EventType %q, want e2", rec.EventType)
	}
}

func TestAuditHTTP_InvalidTimeParam(t *testing.T) {
	al, _ := tempAuditLogger(t)

	req := httptest.NewRequest(http.MethodGet, "/audit?from=not-a-time", nil)
	rr := httptest.NewRecorder()
	al.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
}

func TestAuditHTTP_MethodNotAllowed(t *testing.T) {
	al, _ := tempAuditLogger(t)

	req := httptest.NewRequest(http.MethodPost, "/audit", nil)
	rr := httptest.NewRecorder()
	al.ServeHTTP(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", rr.Code)
	}
}
