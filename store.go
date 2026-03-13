package main

import (
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	indexFileName    = "keyword_index.json"
	failuresFileName = "keyword_failures.json"
	cidListFileName  = "cids.txt"
	maxRetries       = 3
	maxRecentItems   = 30
)

// IndexEntry holds the extracted metadata for a single CID.
type IndexEntry struct {
	CID           string    `json:"cid"`
	Title         string    `json:"title"`
	BroadField    string    `json:"broad_field"`
	SubTopic      string    `json:"sub_topic"`
	ResearchNiche string    `json:"research_niche"`
	Keywords      []string  `json:"keywords"`
	IndexedAt     time.Time `json:"indexed_at"`
}

type failureRecord struct {
	Count   int       `json:"count"`
	LastTry time.Time `json:"last_try"`
	Reason  string    `json:"reason,omitempty"`
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
	TotalCIDs      int    `json:"total_cids"`
	Failed         int    `json:"failed"`
	Skipped        int    `json:"skipped"`
	UniqueKeywords int    `json:"unique_keywords"`
	Enabled        bool   `json:"enabled"`
	Indexing       bool   `json:"indexing"`
	Model          string `json:"model"`
	APIBase        string `json:"api_base"`
}

// Store manages the keyword index, failure records, and search on disk.
// All methods are safe for concurrent use.
type Store struct {
	mu          sync.RWMutex
	entries     map[string]*IndexEntry
	keywordCIDs map[string]map[string]struct{} // lowercase keyword → set of CID keys
	failures    map[string]*failureRecord
	recent      []RecentSearch
	knownCIDs   map[string]struct{} // all CIDs ever submitted for indexing
	skipped     int
	pending     int
	dir         string
}

func NewStore(dir string) *Store {
	s := &Store{
		entries:     make(map[string]*IndexEntry),
		keywordCIDs: make(map[string]map[string]struct{}),
		failures:    make(map[string]*failureRecord),
		recent:      make([]RecentSearch, 0, maxRecentItems),
		knownCIDs:   make(map[string]struct{}),
		dir:         dir,
	}
	s.loadIndex()
	s.loadFailures()
	s.loadCIDList()
	return s
}

// Pending returns the subset of cids that haven't been indexed yet and
// haven't permanently failed. It also sets the internal pending counter
// so Stats() reflects the current queue size.
func (s *Store) Pending(cids []string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	var pending []string
	for _, c := range cids {
		if s.entries[c] != nil {
			continue
		}
		if f, ok := s.failures[c]; ok && f.Count >= maxRetries {
			continue
		}
		pending = append(pending, c)
	}
	s.pending = len(pending)
	return pending
}

// Add stores an indexed entry, updates keyword maps, and persists to disk.
func (s *Store) Add(entry *IndexEntry) {
	s.mu.Lock()
	s.entries[entry.CID] = entry
	delete(s.failures, entry.CID)
	s.addToKeywordIndex(entry.CID, entry)
	if s.pending > 0 {
		s.pending--
	}
	s.mu.Unlock()
	s.saveIndex()
}

func (s *Store) addToKeywordIndex(key string, entry *IndexEntry) {
	labels := make([]string, 0, len(entry.Keywords)+3)
	labels = append(labels, entry.Keywords...)
	for _, l := range []string{entry.BroadField, entry.SubTopic, entry.ResearchNiche} {
		if l != "" {
			labels = append(labels, l)
		}
	}
	for _, kw := range labels {
		kw = strings.ToLower(strings.TrimSpace(kw))
		if kw == "" {
			continue
		}
		if s.keywordCIDs[kw] == nil {
			s.keywordCIDs[kw] = make(map[string]struct{})
		}
		s.keywordCIDs[kw][key] = struct{}{}
	}
}

// RecordFailure increments the failure counter for a CID.
func (s *Store) RecordFailure(cid, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	f, ok := s.failures[cid]
	if !ok {
		f = &failureRecord{}
		s.failures[cid] = f
	}
	f.Count++
	f.LastTry = time.Now()
	f.Reason = reason

	if f.Count >= maxRetries {
		if s.pending > 0 {
			s.pending--
		}
		slog.Warn("permanently failed", "cid", cid, "reason", reason)
	}
}

// RecordSkip marks a CID as processed but not indexed (e.g. non-PDF).
func (s *Store) RecordSkip() {
	s.mu.Lock()
	s.skipped++
	if s.pending > 0 {
		s.pending--
	}
	s.mu.Unlock()
}

// Stats returns summary statistics.
func (s *Store) Stats() StoreStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	permFailed := 0
	for _, f := range s.failures {
		if f.Count >= maxRetries {
			permFailed++
		}
	}

	return StoreStats{
		Indexed:        len(s.entries),
		Pending:        s.pending,
		TotalCIDs:      len(s.knownCIDs),
		Failed:         permFailed,
		Skipped:        s.skipped,
		UniqueKeywords: len(s.keywordCIDs),
		Enabled:        len(s.entries) > 0,
	}
}

// Search returns entries matching the query. Multiple words use AND logic.
func (s *Store) Search(query string) []IndexEntry {
	query = strings.ToLower(strings.TrimSpace(query))
	if query == "" {
		return nil
	}

	terms := strings.Fields(query)
	s.mu.RLock()
	defer s.mu.RUnlock()

	var matchSet map[string]struct{}
	for _, term := range terms {
		termMatches := make(map[string]struct{})
		for kw, cids := range s.keywordCIDs {
			if strings.Contains(kw, term) {
				for c := range cids {
					termMatches[c] = struct{}{}
				}
			}
		}
		for key, entry := range s.entries {
			haystack := strings.ToLower(entry.Title + " " + entry.BroadField + " " + entry.SubTopic + " " + entry.ResearchNiche)
			if strings.Contains(haystack, term) {
				termMatches[key] = struct{}{}
			}
		}
		if matchSet == nil {
			matchSet = termMatches
		} else {
			for c := range matchSet {
				if _, ok := termMatches[c]; !ok {
					delete(matchSet, c)
				}
			}
		}
	}

	results := make([]IndexEntry, 0, len(matchSet))
	for c := range matchSet {
		if entry, ok := s.entries[c]; ok {
			results = append(results, *entry)
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].IndexedAt.After(results[j].IndexedAt)
	})
	return results
}

// Suggest returns keyword suggestions matching the given prefix.
func (s *Store) Suggest(prefix string) []KeywordSuggestion {
	prefix = strings.ToLower(strings.TrimSpace(prefix))

	s.mu.RLock()
	defer s.mu.RUnlock()

	var suggestions []KeywordSuggestion
	for kw, cids := range s.keywordCIDs {
		if prefix == "" || strings.Contains(kw, prefix) {
			suggestions = append(suggestions, KeywordSuggestion{
				Keyword:  kw,
				CIDCount: len(cids),
			})
		}
	}

	sort.Slice(suggestions, func(i, j int) bool {
		if suggestions[i].CIDCount != suggestions[j].CIDCount {
			return suggestions[i].CIDCount > suggestions[j].CIDCount
		}
		return suggestions[i].Keyword < suggestions[j].Keyword
	})

	if len(suggestions) > 20 {
		suggestions = suggestions[:20]
	}
	return suggestions
}

// RecordSearch adds a search to the recent searches list.
func (s *Store) RecordSearch(keyword string, resultCount int) {
	if resultCount == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, sr := range s.recent {
		if strings.EqualFold(sr.Keyword, keyword) {
			s.recent = append(s.recent[:i], s.recent[i+1:]...)
			break
		}
	}

	s.recent = append(s.recent, RecentSearch{
		Keyword:     strings.ToLower(keyword),
		ResultCount: resultCount,
		Timestamp:   time.Now().Unix(),
	})
	if len(s.recent) > maxRecentItems {
		s.recent = s.recent[len(s.recent)-maxRecentItems:]
	}
}

// GetRecentSearches returns recent searches, newest first.
func (s *Store) GetRecentSearches() []RecentSearch {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]RecentSearch, len(s.recent))
	for i, sr := range s.recent {
		result[len(s.recent)-1-i] = sr
	}
	return result
}

// SaveAll persists both the index and failures to disk.
func (s *Store) SaveAll() {
	s.saveIndex()
	s.saveFailures()
}

// AppendCIDs merges new CIDs into the persistent list and saves to disk.
// Returns the count of CIDs that were actually new.
func (s *Store) AppendCIDs(cids []string) int {
	s.mu.Lock()
	added := 0
	for _, c := range cids {
		if _, exists := s.knownCIDs[c]; !exists {
			s.knownCIDs[c] = struct{}{}
			added++
		}
	}
	s.mu.Unlock()

	if added > 0 {
		s.saveCIDList()
	}
	return added
}

// AllCIDs returns all CIDs in the persistent list.
func (s *Store) AllCIDs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]string, 0, len(s.knownCIDs))
	for c := range s.knownCIDs {
		out = append(out, c)
	}
	return out
}

func (s *Store) cidListPath() string {
	return filepath.Join(s.dir, cidListFileName)
}

func (s *Store) indexPath() string {
	return filepath.Join(s.dir, indexFileName)
}

func (s *Store) failuresPath() string {
	return filepath.Join(s.dir, failuresFileName)
}

func (s *Store) loadIndex() {
	data, err := os.ReadFile(s.indexPath())
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Warn("failed to read index", "path", s.indexPath(), "error", err)
		}
		return
	}

	var entries map[string]*IndexEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		slog.Warn("failed to parse index", "path", s.indexPath(), "error", err)
		return
	}
	for cidStr, e := range entries {
		if e.CID == "" {
			e.CID = cidStr
		}
		s.entries[cidStr] = e
		s.addToKeywordIndex(cidStr, e)
	}
	slog.Info("loaded existing index", "entries", len(entries), "keywords", len(s.keywordCIDs))
}

func (s *Store) loadFailures() {
	data, err := os.ReadFile(s.failuresPath())
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Warn("failed to read failures", "path", s.failuresPath(), "error", err)
		}
		return
	}

	var records map[string]*failureRecord
	if err := json.Unmarshal(data, &records); err != nil {
		slog.Warn("failed to parse failures", "path", s.failuresPath(), "error", err)
		return
	}
	for cidStr, f := range records {
		s.failures[cidStr] = f
	}
	slog.Info("loaded failure records", "entries", len(records))
}

func (s *Store) loadCIDList() {
	data, err := os.ReadFile(s.cidListPath())
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Warn("failed to read CID list", "path", s.cidListPath(), "error", err)
		}
		return
	}
	count := 0
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		s.knownCIDs[line] = struct{}{}
		count++
	}
	if count > 0 {
		slog.Info("loaded persistent CID list", "cids", count)
	}
}

func (s *Store) saveCIDList() {
	s.mu.RLock()
	lines := make([]string, 0, len(s.knownCIDs))
	for c := range s.knownCIDs {
		lines = append(lines, c)
	}
	s.mu.RUnlock()

	sort.Strings(lines)
	content := strings.Join(lines, "\n") + "\n"
	atomicWrite(s.cidListPath(), []byte(content))
}

func (s *Store) saveIndex() {
	s.mu.Lock()
	data, err := json.MarshalIndent(s.entries, "", "  ")
	s.mu.Unlock()
	if err != nil {
		slog.Error("failed to marshal index", "error", err)
		return
	}
	atomicWrite(s.indexPath(), data)
}

func (s *Store) saveFailures() {
	s.mu.Lock()
	permanent := make(map[string]*failureRecord)
	for cidStr, f := range s.failures {
		if f.Count >= maxRetries {
			permanent[cidStr] = f
		}
	}
	s.mu.Unlock()

	if len(permanent) == 0 {
		return
	}
	data, err := json.MarshalIndent(permanent, "", "  ")
	if err != nil {
		slog.Error("failed to marshal failures", "error", err)
		return
	}
	atomicWrite(s.failuresPath(), data)
}

func atomicWrite(path string, data []byte) {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		slog.Error("write failed", "path", tmp, "error", err)
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		slog.Error("rename failed", "path", path, "error", err)
	}
}
