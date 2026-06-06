package main

import (
	"database/sql"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"

	_ "modernc.org/sqlite"
)

const (
	dbFileName     = "index.db"
	maxRetries     = 3
	maxRecentItems = 30

	archiveQueued   = "queued"
	archiveCrawling = "crawling"
	archiveIndexing = "indexing"
	archiveDone     = "done"
	archiveFailed   = "failed"

	submissionDocument = "document"
	submissionArchive  = "archive"

	settingReviewEnabled = "review_enabled"
)

// IndexEntry holds extracted metadata for a single CID.
type IndexEntry struct {
	CID        string    `json:"cid"`
	Title      string    `json:"title"`
	BroadField string    `json:"broad_field"`
	SubTopic   string    `json:"sub_topic"`
	Keywords   []string  `json:"keywords"`
	IndexedAt  time.Time `json:"indexed_at"`
	Archives   []string  `json:"archives,omitempty"` // archive CIDs this doc belongs to
}

// FieldCount pairs a label with the number of documents carrying it.
type FieldCount struct {
	Field string `json:"field"`
	Count int    `json:"count"`
}

// ArchiveRef is a lightweight archive reference for search results.
type ArchiveRef struct {
	CID    string `json:"cid"`
	Name   string `json:"name,omitempty"`
	Status string `json:"status,omitempty"`
}

// Archive is a directory CID whose documents are indexed together.
type Archive struct {
	CID         string       `json:"cid"`
	Name        string       `json:"name,omitempty"`
	Owner       string       `json:"owner,omitempty"`
	Status      string       `json:"status"` // crawling | indexing | done | failed
	DocCIDs     []string     `json:"doc_cids,omitempty"`
	DocCount    int          `json:"doc_count"`
	Indexed     int          `json:"indexed"`
	Failed      int          `json:"failed"`
	TopKeywords []string     `json:"top_keywords,omitempty"`
	BroadFields []FieldCount `json:"broad_fields,omitempty"`
	Error       string       `json:"error,omitempty"`
	SubmittedAt time.Time    `json:"submitted_at"`
	IndexedAt   time.Time    `json:"indexed_at,omitempty"`
}

// FailedDoc is a permanently failed document shown in the UI for manual retry.
type FailedDoc struct {
	CID     string    `json:"cid"`
	Count   int       `json:"count"`
	Reason  string    `json:"reason,omitempty"`
	LastTry time.Time `json:"last_try"`
}

// Submission is a CID held for admin review when review mode is on.
type Submission struct {
	CID         string    `json:"cid"`
	Kind        string    `json:"kind"` // document | archive
	Owner       string    `json:"owner,omitempty"`
	SubmittedAt time.Time `json:"submitted_at"`
}

type RecentSearch struct {
	Keyword     string `json:"keyword"`
	ResultCount int    `json:"result_count"`
	Timestamp   int64  `json:"timestamp"`
}

type KeywordSuggestion struct {
	Keyword  string `json:"keyword"`
	CIDCount int    `json:"cid_count"`
}

type StoreStats struct {
	Indexed        int    `json:"indexed"`
	Pending        int    `json:"pending"`
	Failed         int    `json:"failed"`
	Skipped        int    `json:"skipped"`
	UniqueKeywords int    `json:"unique_keywords"`
	Archives       int    `json:"archives"`
	Queued         int    `json:"queued"`
	Enabled        bool   `json:"enabled"`
	Indexing       bool   `json:"indexing"`
	Model          string `json:"model"`
	APIBase        string `json:"api_base"`
}

// Store persists the index in SQLite with an FTS5 full-text index.
// Writes are serialized through wmu; reads use WAL concurrency.
type Store struct {
	db  *sql.DB
	dir string

	wmu sync.Mutex // serializes write transactions (SQLite single-writer)

	mu            sync.Mutex // protects the in-memory fields below
	recent        []RecentSearch
	skipped       int
	pending       int
	reviewEnabled bool
}

// NewStore opens the SQLite database in dir and applies the schema.
func NewStore(dir string) *Store {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		slog.Error("failed to create data directory", "dir", dir, "error", err)
		os.Exit(1)
	}
	path := filepath.Join(dir, dbFileName)
	dsn := "file:" + path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)&_pragma=synchronous(1)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		slog.Error("failed to open database", "path", path, "error", err)
		os.Exit(1)
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(16)

	s := &Store{db: db, dir: dir, recent: make([]RecentSearch, 0, maxRecentItems)}
	if err := s.initSchema(); err != nil {
		slog.Error("failed to initialize schema", "error", err)
		os.Exit(1)
	}
	s.reviewEnabled = s.loadReviewEnabled()

	var docs, kws, arch int
	db.QueryRow("SELECT COUNT(*) FROM documents").Scan(&docs)
	db.QueryRow("SELECT COUNT(DISTINCT label) FROM labels").Scan(&kws)
	db.QueryRow("SELECT COUNT(*) FROM archives").Scan(&arch)
	slog.Info("opened index database", "path", path, "documents", docs, "keywords", kws, "archives", arch)
	return s
}

func (s *Store) initSchema() error {
	_, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS documents (
    cid         TEXT PRIMARY KEY,
    title       TEXT NOT NULL DEFAULT '',
    broad_field TEXT NOT NULL DEFAULT '',
    sub_topic   TEXT NOT NULL DEFAULT '',
    keywords    TEXT NOT NULL DEFAULT '[]',
    indexed_at  INTEGER NOT NULL DEFAULT 0
);

CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(
    cid UNINDEXED, title, broad_field, sub_topic, keywords,
    tokenize = 'unicode61'
);

CREATE TABLE IF NOT EXISTS labels (
    label   TEXT NOT NULL,
    doc_cid TEXT NOT NULL,
    PRIMARY KEY (label, doc_cid)
);
CREATE INDEX IF NOT EXISTS idx_labels_doc ON labels(doc_cid);

CREATE TABLE IF NOT EXISTS archives (
    cid          TEXT PRIMARY KEY,
    name         TEXT NOT NULL DEFAULT '',
    owner        TEXT NOT NULL DEFAULT '',
    status       TEXT NOT NULL DEFAULT '',
    doc_count    INTEGER NOT NULL DEFAULT 0,
    indexed      INTEGER NOT NULL DEFAULT 0,
    failed       INTEGER NOT NULL DEFAULT 0,
    top_keywords TEXT NOT NULL DEFAULT '[]',
    broad_fields TEXT NOT NULL DEFAULT '[]',
    error        TEXT NOT NULL DEFAULT '',
    submitted_at INTEGER NOT NULL DEFAULT 0,
    indexed_at   INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS archive_docs (
    archive_cid TEXT NOT NULL,
    doc_cid     TEXT NOT NULL,
    position    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (archive_cid, doc_cid)
);
CREATE INDEX IF NOT EXISTS idx_archive_docs_doc ON archive_docs(doc_cid);

CREATE TABLE IF NOT EXISTS failures (
    cid      TEXT PRIMARY KEY,
    count    INTEGER NOT NULL DEFAULT 0,
    last_try INTEGER NOT NULL DEFAULT 0,
    reason   TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS submissions (
    cid          TEXT PRIMARY KEY,
    kind         TEXT NOT NULL DEFAULT '',
    owner        TEXT NOT NULL DEFAULT '',
    submitted_at INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS denylist (
    cid      TEXT PRIMARY KEY,
    added_at INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL DEFAULT ''
);
`)
	return err
}

// write runs fn inside a transaction while holding the writer lock.
func (s *Store) write(fn func(*sql.Tx) error) error {
	s.wmu.Lock()
	defer s.wmu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// SaveAll checkpoints the WAL. Persistence is otherwise transactional.
func (s *Store) SaveAll() {
	if _, err := s.db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		slog.Debug("wal checkpoint failed", "error", err)
	}
}

func nano(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func fromNano(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}

func marshalJSON(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "[]"
	}
	return string(b)
}

func parseStrings(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	json.Unmarshal([]byte(s), &out)
	return out
}

func parseFields(s string) []FieldCount {
	if s == "" {
		return nil
	}
	var out []FieldCount
	json.Unmarshal([]byte(s), &out)
	return out
}

// ftsMatch builds an FTS5 MATCH expression: each word becomes a prefix term,
// ANDed together. Non-alphanumeric characters are stripped.
func ftsMatch(query string) string {
	var b strings.Builder
	for _, term := range strings.Fields(query) {
		var tok strings.Builder
		for _, r := range term {
			if unicode.IsLetter(r) || unicode.IsDigit(r) {
				tok.WriteRune(r)
			}
		}
		if tok.Len() == 0 {
			continue
		}
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(tok.String())
		b.WriteByte('*')
	}
	return b.String()
}

// placeholders returns "?,?,..." with n entries.
func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.Repeat(",?", n)[1:]
}

// topCounted returns the n highest-frequency keys, ties broken alphabetically.
func topCounted(counts map[string]int, n int) []string {
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sortByCountThenKey(keys, counts)
	if len(keys) > n {
		keys = keys[:n]
	}
	return keys
}

func topFields(counts map[string]int) []FieldCount {
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sortByCountThenKey(keys, counts)
	out := make([]FieldCount, 0, len(keys))
	for _, k := range keys {
		out = append(out, FieldCount{Field: k, Count: counts[k]})
	}
	return out
}

func sortByCountThenKey(keys []string, counts map[string]int) {
	sort.Slice(keys, func(i, j int) bool {
		if counts[keys[i]] != counts[keys[j]] {
			return counts[keys[i]] > counts[keys[j]]
		}
		return keys[i] < keys[j]
	})
}
