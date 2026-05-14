package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	defaultAuditLogPath   = "/tmp/health-esb/audit.jsonl"
	auditLogRetentionDays = 30
)

// AuditLogger writes AuditRecords as newline-delimited JSON and handles
// daily rotation with configurable retention.
type AuditLogger struct {
	mu   sync.Mutex
	file *os.File
	path string // full path to the active file, e.g. /var/log/health-esb/audit.jsonl
	date string // date of the currently open file in "2006-01-02" format
}

// NewAuditLogger opens (or creates) the audit log at path and returns
// an AuditLogger ready for use. The directory is created if it does not exist.
func NewAuditLogger(path string) (*AuditLogger, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("audit log dir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o640)
	if err != nil {
		return nil, fmt.Errorf("open audit log: %w", err)
	}
	al := &AuditLogger{
		file: f,
		path: path,
		date: time.Now().UTC().Format("2006-01-02"),
	}
	return al, nil
}

// Write appends rec as a JSON line to the audit log. If the calendar date has
// changed since the file was opened, the current file is rotated first.
func (al *AuditLogger) Write(rec AuditRecord) error {
	al.mu.Lock()
	defer al.mu.Unlock()

	today := time.Now().UTC().Format("2006-01-02")
	if today != al.date {
		if err := al.rotate(today); err != nil {
			return fmt.Errorf("rotate audit log: %w", err)
		}
		al.date = today
	}

	line, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal audit record: %w", err)
	}
	line = append(line, '\n')
	_, err = al.file.Write(line)
	return err
}

// rotate closes the current file, renames it to audit-<YYYY-MM-DD>.jsonl
// using the previous date, opens a new file, and purges files beyond retention.
// Must be called with al.mu held.
func (al *AuditLogger) rotate(newDate string) error {
	if err := al.file.Close(); err != nil {
		return fmt.Errorf("close current log: %w", err)
	}

	dir := filepath.Dir(al.path)
	rotated := filepath.Join(dir, "audit-"+al.date+".jsonl")
	if err := os.Rename(al.path, rotated); err != nil {
		return fmt.Errorf("rename log file: %w", err)
	}

	f, err := os.OpenFile(al.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o640)
	if err != nil {
		return fmt.Errorf("open new log file: %w", err)
	}
	al.file = f
	al.date = newDate

	go al.purgeOld() // run cleanup in background; errors are non-fatal
	return nil
}

// purgeOld deletes rotated audit log files older than auditLogRetentionDays.
func (al *AuditLogger) purgeOld() {
	dir := filepath.Dir(al.path)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	cutoff := time.Now().UTC().AddDate(0, 0, -auditLogRetentionDays)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, "audit-") || !strings.HasSuffix(name, ".jsonl") {
			continue
		}
		dateStr := strings.TrimSuffix(strings.TrimPrefix(name, "audit-"), ".jsonl")
		t, err := time.Parse("2006-01-02", dateStr)
		if err != nil {
			continue
		}
		if t.Before(cutoff) {
			_ = os.Remove(filepath.Join(dir, name))
		}
	}
}

// Close flushes and closes the underlying file.
func (al *AuditLogger) Close() error {
	al.mu.Lock()
	defer al.mu.Unlock()
	return al.file.Close()
}

// ServeHTTP implements http.Handler for GET /audit.
//
// Query parameters:
//
//	from=<RFC3339>     — include records with Timestamp >= from
//	to=<RFC3339>       — include records with Timestamp <= to
//	type=<event_type>  — include records with EventType == type (case-insensitive)
//
// Matching records are streamed as newline-delimited JSON.
func (al *AuditLogger) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	var fromT, toT time.Time
	var parseErr string

	if v := q.Get("from"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			parseErr = "invalid 'from' parameter: " + err.Error()
		}
		fromT = t
	}
	if v := q.Get("to"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			parseErr = "invalid 'to' parameter: " + err.Error()
		}
		toT = t
	}
	if parseErr != "" {
		http.Error(w, parseErr, http.StatusBadRequest)
		return
	}

	filterType := strings.ToLower(q.Get("type"))

	// Open the current log file for reading. We hold only a path reference;
	// the writer uses the file descriptor independently.
	al.mu.Lock()
	readPath := al.path
	al.mu.Unlock()

	f, err := os.Open(readPath) //nolint:gosec — path comes from config, not user input
	if err != nil {
		if os.IsNotExist(err) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		http.Error(w, "cannot open audit log", http.StatusInternalServerError)
		return
	}
	defer f.Close()

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var rec AuditRecord
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			continue
		}
		if !fromT.IsZero() && rec.Timestamp.Before(fromT) {
			continue
		}
		if !toT.IsZero() && rec.Timestamp.After(toT) {
			continue
		}
		if filterType != "" && strings.ToLower(rec.EventType) != filterType {
			continue
		}
		_, _ = w.Write(scanner.Bytes())
		_, _ = w.Write([]byte("\n"))
	}
}

// auditLogPath returns the configured audit log path from the environment,
// falling back to the default.
func auditLogPath() string {
	if v := os.Getenv("AUDIT_LOG_PATH"); v != "" {
		return v
	}
	return defaultAuditLogPath
}

// rotatedFiles returns a sorted list of rotated audit log file names in dir.
func rotatedFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), "audit-") && strings.HasSuffix(e.Name(), ".jsonl") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names, nil
}
