package main

import (
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	indexFileName       = "keyword_index.json"
	failuresFileName    = "keyword_failures.json"
	archivesFileName    = "archives.json"
	submissionsFileName = "submissions.json"
	denylistFileName    = "denylist.json"
	settingsFileName    = "settings.json"
	maxRetries          = 3
	maxRecentItems      = 30

	autoSaveInterval = 2 * time.Second

	archiveTopKeywords = 12

	archiveQueued   = "queued"
	archiveCrawling = "crawling"
	archiveIndexing = "indexing"
	archiveDone     = "done"
	archiveFailed   = "failed"

	submissionDocument = "document"
	submissionArchive  = "archive"
)

// IndexEntry holds the extracted metadata for a single CID.
type IndexEntry struct {
	CID        string    `json:"cid"`
	Title      string    `json:"title"`
	BroadField string    `json:"broad_field"`
	SubTopic   string    `json:"sub_topic"`
	Keywords   []string  `json:"keywords"`
	IndexedAt  time.Time `json:"indexed_at"`
	Archives   []string  `json:"archives,omitempty"` // archive CIDs this doc belongs to
}

// FieldCount is a label paired with the number of member documents carrying it.
type FieldCount struct {
	Field string `json:"field"`
	Count int    `json:"count"`
}

// ArchiveRef is a lightweight reference to an archive that contains a document,
// used to surface archive membership in search results.
type ArchiveRef struct {
	CID    string `json:"cid"`
	Name   string `json:"name,omitempty"`
	Status string `json:"status,omitempty"`
}

// Archive is a directory CID whose contained documents are indexed together.
// Once all members are processed it carries aggregated labels for browsing.
type Archive struct {
	CID         string       `json:"cid"`
	Name        string       `json:"name,omitempty"`
	Owner       string       `json:"owner,omitempty"`
	Status      string       `json:"status"` // crawling | indexing | done | failed
	DocCIDs     []string     `json:"doc_cids"`
	DocCount    int          `json:"doc_count"`
	Indexed     int          `json:"indexed"`
	Failed      int          `json:"failed"`
	TopKeywords []string     `json:"top_keywords,omitempty"`
	BroadFields []FieldCount `json:"broad_fields,omitempty"`
	Error       string       `json:"error,omitempty"`
	SubmittedAt time.Time    `json:"submitted_at"`
	IndexedAt   time.Time    `json:"indexed_at,omitempty"`
}

type failureRecord struct {
	Count   int       `json:"count"`
	LastTry time.Time `json:"last_try"`
	Reason  string    `json:"reason,omitempty"`
}

// FailedDoc is a permanently-failed document surfaced to the UI so a user can
// inspect the reason and manually retrigger indexing.
type FailedDoc struct {
	CID     string    `json:"cid"`
	Count   int       `json:"count"`
	Reason  string    `json:"reason,omitempty"`
	LastTry time.Time `json:"last_try"`
}

// Submission is a CID parked for admin review when review mode is enabled. It
// is not added to the archives list or indexed until approved.
type Submission struct {
	CID         string    `json:"cid"`
	Kind        string    `json:"kind"` // document | archive
	Owner       string    `json:"owner,omitempty"`
	SubmittedAt time.Time `json:"submitted_at"`
}

// persistedSettings holds admin-configurable settings saved to settings.json.
type persistedSettings struct {
	ReviewEnabled bool `json:"review_enabled"`
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

// Store manages the keyword index, failure records, and search on disk.
// All methods are safe for concurrent use.
type Store struct {
	mu            sync.RWMutex
	entries       map[string]*IndexEntry
	keywordCIDs   map[string]map[string]struct{} // lowercase keyword -> set of CID keys
	failures      map[string]*failureRecord
	archives      map[string]*Archive
	submissions   map[string]*Submission
	denylist      map[string]time.Time
	reviewEnabled bool
	recent        []RecentSearch
	skipped       int
	pending       int
	dir           string

	indexDirty       atomic.Bool // index has unsaved changes
	failuresDirty    atomic.Bool // failures have unsaved changes
	archivesDirty    atomic.Bool // archives have unsaved changes
	submissionsDirty atomic.Bool // pending submissions have unsaved changes
	denylistDirty    atomic.Bool // denylist has unsaved changes
	settingsDirty    atomic.Bool // settings have unsaved changes
}

func NewStore(dir string) *Store {
	s := &Store{
		entries:     make(map[string]*IndexEntry),
		keywordCIDs: make(map[string]map[string]struct{}),
		failures:    make(map[string]*failureRecord),
		archives:    make(map[string]*Archive),
		submissions: make(map[string]*Submission),
		denylist:    make(map[string]time.Time),
		recent:      make([]RecentSearch, 0, maxRecentItems),
		dir:         dir,
	}
	s.loadIndex()
	s.loadFailures()
	s.loadArchives()
	s.loadSubmissions()
	s.loadDenylist()
	s.loadSettings()
	go s.autoSaveLoop()
	return s
}

// autoSaveLoop periodically flushes dirty state to disk, coalescing the many
// writes produced during indexing into at most one write per interval.
func (s *Store) autoSaveLoop() {
	ticker := time.NewTicker(autoSaveInterval)
	defer ticker.Stop()
	for range ticker.C {
		if s.indexDirty.CompareAndSwap(true, false) {
			s.saveIndex()
		}
		if s.failuresDirty.CompareAndSwap(true, false) {
			s.saveFailures()
		}
		if s.archivesDirty.CompareAndSwap(true, false) {
			s.saveArchives()
		}
		if s.submissionsDirty.CompareAndSwap(true, false) {
			s.saveSubmissions()
		}
		if s.denylistDirty.CompareAndSwap(true, false) {
			s.saveDenylist()
		}
		if s.settingsDirty.CompareAndSwap(true, false) {
			s.saveSettings()
		}
	}
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
	if _, hadFailure := s.failures[entry.CID]; hadFailure {
		delete(s.failures, entry.CID)
		s.failuresDirty.Store(true)
	}
	s.addToKeywordIndex(entry.CID, entry)
	if s.pending > 0 {
		s.pending--
	}
	s.mu.Unlock()
	s.indexDirty.Store(true)
}

func (s *Store) addToKeywordIndex(key string, entry *IndexEntry) {
	labels := make([]string, 0, len(entry.Keywords)+3)
	labels = append(labels, entry.Keywords...)
	for _, l := range []string{entry.BroadField, entry.SubTopic} {
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

	// Persist every attempt (not just terminal ones) so failure counts survive
	// restarts and chronically-failing CIDs eventually reach the permanent cap
	// instead of being retried indefinitely across runs.
	s.failuresDirty.Store(true)

	if f.Count >= maxRetries {
		if s.pending > 0 {
			s.pending--
		}
		slog.Warn("permanently failed", "cid", cid, "reason", reason)
	}
}

// RecordRateLimited notes a transient rate-limit failure for a CID without
// incrementing its failure count. Because the count is left untouched, the CID
// never reaches the permanent-failure cap and stays in the pending set, so it
// is retried on the next archive run instead of being dropped just because the
// upstream API was temporarily busy.
func (s *Store) RecordRateLimited(cid, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	f, ok := s.failures[cid]
	if !ok {
		f = &failureRecord{}
		s.failures[cid] = f
	}
	f.LastTry = time.Now()
	f.Reason = reason
	s.failuresDirty.Store(true)
}

// RequeueRateLimited clears failure records that were caused by rate limiting,
// making those CIDs pending again. This recovers documents that hit the
// permanent-failure cap purely because the upstream API was busy (a transient
// condition) rather than because the document is unprocessable. Returns the
// number of records cleared. Intended to run once at startup.
func (s *Store) RequeueRateLimited() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	cleared := 0
	for cid, f := range s.failures {
		if strings.Contains(f.Reason, "rate limited") {
			delete(s.failures, cid)
			cleared++
		}
	}
	if cleared > 0 {
		s.failuresDirty.Store(true)
	}
	return cleared
}

// Failures returns the permanently-failed documents (those that hit the retry
// cap), most recent attempt first.
func (s *Store) Failures() []FailedDoc {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]FailedDoc, 0)
	for cid, f := range s.failures {
		if f.Count >= maxRetries {
			out = append(out, FailedDoc{CID: cid, Count: f.Count, Reason: f.Reason, LastTry: f.LastTry})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].LastTry.After(out[j].LastTry)
	})
	return out
}

// RetryFailure clears a CID's failure record so it becomes pending again.
// Returns true if a record existed.
func (s *Store) RetryFailure(cid string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.failures[cid]; !ok {
		return false
	}
	delete(s.failures, cid)
	s.failuresDirty.Store(true)
	return true
}

// ArchivesContaining returns the CIDs of archives that list the given document
// CID among their members.
func (s *Store) ArchivesContaining(docCID string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []string
	for cid, a := range s.archives {
		if containsString(a.DocCIDs, docCID) {
			out = append(out, cid)
		}
	}
	return out
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
		Failed:         permFailed,
		Skipped:        s.skipped,
		UniqueKeywords: len(s.keywordCIDs),
		Archives:       len(s.archives),
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
			haystack := strings.ToLower(entry.Title + " " + entry.BroadField + " " + entry.SubTopic)
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

// AddArchive registers an archive in the "crawling" state, or updates the owner
// of an existing one. Returns true if the archive was newly created.
func (s *Store) AddArchive(cid, owner string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	a, ok := s.archives[cid]
	if !ok {
		s.archives[cid] = &Archive{
			CID:         cid,
			Owner:       strings.TrimSpace(owner),
			Status:      archiveQueued,
			SubmittedAt: time.Now(),
		}
		s.archivesDirty.Store(true)
		return true
	}
	if o := strings.TrimSpace(owner); o != "" {
		a.Owner = o
		s.archivesDirty.Store(true)
	}
	return false
}

// MarkArchiveCrawling moves an archive into the "crawling" state when the
// dispatcher begins processing it.
func (s *Store) MarkArchiveCrawling(cid string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if a := s.archives[cid]; a != nil {
		a.Status = archiveCrawling
		s.archivesDirty.Store(true)
	}
}

// SetArchiveDocs records the document CIDs discovered by the crawler and moves
// the archive into the "indexing" state.
func (s *Store) SetArchiveDocs(cid string, docCIDs []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	a := s.archives[cid]
	if a == nil {
		return
	}
	a.DocCIDs = docCIDs
	a.DocCount = len(docCIDs)
	a.Status = archiveIndexing
	s.archivesDirty.Store(true)
}

// MarkArchiveFailed records a terminal crawl/index failure for an archive.
func (s *Store) MarkArchiveFailed(cid, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if a := s.archives[cid]; a != nil {
		a.Status = archiveFailed
		a.Error = reason
		s.archivesDirty.Store(true)
	}
}

// FinalizeArchive aggregates the labels of an archive's member documents into
// archive-level keywords and field counts, then marks it "done".
func (s *Store) FinalizeArchive(cid string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	a := s.archives[cid]
	if a == nil {
		return
	}

	kwCount := make(map[string]int)
	fieldCount := make(map[string]int)
	indexed, failed := 0, 0

	for _, dc := range a.DocCIDs {
		entry := s.entries[dc]
		if entry == nil {
			if f, ok := s.failures[dc]; ok && f.Count >= maxRetries {
				failed++
			}
			continue
		}
		indexed++
		if !containsString(entry.Archives, cid) {
			entry.Archives = append(entry.Archives, cid)
		}
		for _, kw := range entry.Keywords {
			if kw = strings.ToLower(strings.TrimSpace(kw)); kw != "" {
				kwCount[kw]++
			}
		}
		if bf := strings.TrimSpace(entry.BroadField); bf != "" {
			fieldCount[bf]++
		}
	}

	a.Indexed = indexed
	a.Failed = failed
	a.TopKeywords = topCounted(kwCount, archiveTopKeywords)
	a.BroadFields = topFields(fieldCount)
	a.Status = archiveDone
	a.IndexedAt = time.Now()
	if a.Name == "" {
		if len(a.BroadFields) > 0 {
			a.Name = a.BroadFields[0].Field
		} else {
			a.Name = a.CID
		}
	}

	s.archivesDirty.Store(true)
	s.indexDirty.Store(true) // member entries gained archive membership links
}

// Archives returns all archives, newest submission first.
func (s *Store) Archives() []Archive {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]Archive, 0, len(s.archives))
	for _, a := range s.archives {
		out = append(out, *a)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].SubmittedAt.After(out[j].SubmittedAt)
	})
	return out
}

// ArchiveRefs resolves a set of archive CIDs to lightweight references
// (CID + name + status) for display. CIDs with no matching archive record are
// returned with only the CID populated.
func (s *Store) ArchiveRefs(cids []string) []ArchiveRef {
	if len(cids) == 0 {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	refs := make([]ArchiveRef, 0, len(cids))
	for _, cid := range cids {
		if a := s.archives[cid]; a != nil {
			refs = append(refs, ArchiveRef{CID: a.CID, Name: a.Name, Status: a.Status})
		} else {
			refs = append(refs, ArchiveRef{CID: cid})
		}
	}
	return refs
}

// GetArchive returns an archive and the indexed entries of its member
// documents (in archive order).
func (s *Store) GetArchive(cid string) (Archive, []IndexEntry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	a, ok := s.archives[cid]
	if !ok {
		return Archive{}, nil, false
	}
	docs := make([]IndexEntry, 0, len(a.DocCIDs))
	for _, dc := range a.DocCIDs {
		if e := s.entries[dc]; e != nil {
			docs = append(docs, *e)
		}
	}
	return *a, docs, true
}

// ArchiveFailures returns the permanently-failed member documents of an
// archive, most recent attempt first.
func (s *Store) ArchiveFailures(cid string) []FailedDoc {
	s.mu.RLock()
	defer s.mu.RUnlock()

	a := s.archives[cid]
	if a == nil {
		return nil
	}
	out := make([]FailedDoc, 0)
	for _, dc := range a.DocCIDs {
		if f, ok := s.failures[dc]; ok && f.Count >= maxRetries {
			out = append(out, FailedDoc{CID: dc, Count: f.Count, Reason: f.Reason, LastTry: f.LastTry})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].LastTry.After(out[j].LastTry)
	})
	return out
}

// SearchArchives returns archives whose aggregated labels match every term in
// the query (AND logic).
func (s *Store) SearchArchives(query string) []Archive {
	query = strings.ToLower(strings.TrimSpace(query))
	if query == "" {
		return s.Archives()
	}
	terms := strings.Fields(query)

	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []Archive
	for _, a := range s.archives {
		hay := strings.ToLower(a.Name + " " + a.Owner + " " + strings.Join(a.TopKeywords, " "))
		for _, f := range a.BroadFields {
			hay += " " + strings.ToLower(f.Field)
		}
		matchAll := true
		for _, t := range terms {
			if !strings.Contains(hay, t) {
				matchAll = false
				break
			}
		}
		if matchAll {
			out = append(out, *a)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].SubmittedAt.After(out[j].SubmittedAt)
	})
	return out
}

func containsString(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}

// removeString returns a new slice with all occurrences of want removed.
func removeString(list []string, want string) []string {
	out := make([]string, 0, len(list))
	for _, v := range list {
		if v != want {
			out = append(out, v)
		}
	}
	return out
}

// topCounted returns the n highest-frequency keys, ties broken alphabetically.
func topCounted(counts map[string]int, n int) []string {
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if counts[keys[i]] != counts[keys[j]] {
			return counts[keys[i]] > counts[keys[j]]
		}
		return keys[i] < keys[j]
	})
	if len(keys) > n {
		keys = keys[:n]
	}
	return keys
}

func topFields(counts map[string]int) []FieldCount {
	out := make([]FieldCount, 0, len(counts))
	for f, c := range counts {
		out = append(out, FieldCount{Field: f, Count: c})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Field < out[j].Field
	})
	return out
}

// SaveAll forces a synchronous flush of all persisted state to disk, clearing
// any pending dirty state.
func (s *Store) SaveAll() {
	s.indexDirty.Store(false)
	s.failuresDirty.Store(false)
	s.archivesDirty.Store(false)
	s.submissionsDirty.Store(false)
	s.denylistDirty.Store(false)
	s.settingsDirty.Store(false)
	s.saveIndex()
	s.saveFailures()
	s.saveArchives()
	s.saveSubmissions()
	s.saveDenylist()
	s.saveSettings()
}

func (s *Store) indexPath() string {
	return filepath.Join(s.dir, indexFileName)
}

func (s *Store) failuresPath() string {
	return filepath.Join(s.dir, failuresFileName)
}

func (s *Store) archivesPath() string {
	return filepath.Join(s.dir, archivesFileName)
}

func (s *Store) submissionsPath() string {
	return filepath.Join(s.dir, submissionsFileName)
}

func (s *Store) denylistPath() string {
	return filepath.Join(s.dir, denylistFileName)
}

func (s *Store) settingsPath() string {
	return filepath.Join(s.dir, settingsFileName)
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

func (s *Store) loadArchives() {
	data, err := os.ReadFile(s.archivesPath())
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Warn("failed to read archives", "path", s.archivesPath(), "error", err)
		}
		return
	}

	var records map[string]*Archive
	if err := json.Unmarshal(data, &records); err != nil {
		slog.Warn("failed to parse archives", "path", s.archivesPath(), "error", err)
		return
	}
	resumable := 0
	for cidStr, a := range records {
		if a.CID == "" {
			a.CID = cidStr
		}
		// Archives interrupted mid-run are left in their non-terminal state so
		// they can be re-enqueued and resumed on startup (see ResumableArchives).
		if a.Status == archiveQueued || a.Status == archiveCrawling || a.Status == archiveIndexing {
			resumable++
		}
		s.archives[cidStr] = a
	}
	if len(records) > 0 {
		slog.Info("loaded archives", "count", len(records), "resumable", resumable)
	}
}

// ResumableArchives returns the CIDs of archives that were interrupted before
// completion (still queued, crawling, or indexing) and should be re-run.
func (s *Store) ResumableArchives() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []string
	for cid, a := range s.archives {
		switch a.Status {
		case archiveQueued, archiveCrawling, archiveIndexing:
			out = append(out, cid)
		}
	}
	return out
}

// ArchiveDocCIDs returns a copy of the document CIDs already discovered for an
// archive. A non-empty result means the crawl phase completed in a prior run
// and can be skipped on resume.
func (s *Store) ArchiveDocCIDs(cid string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	a := s.archives[cid]
	if a == nil || len(a.DocCIDs) == 0 {
		return nil
	}
	out := make([]string, len(a.DocCIDs))
	copy(out, a.DocCIDs)
	return out
}

// ReviewEnabled reports whether admin-review mode is on (submissions are held
// for moderation instead of being indexed immediately).
func (s *Store) ReviewEnabled() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.reviewEnabled
}

// SetReviewEnabled toggles admin-review mode and persists the setting.
func (s *Store) SetReviewEnabled(enabled bool) {
	s.mu.Lock()
	s.reviewEnabled = enabled
	s.mu.Unlock()
	s.settingsDirty.Store(true)
}

// IsDenied reports whether a CID has been denylisted by an admin.
func (s *Store) IsDenied(cid string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.denylist[cid]
	return ok
}

// Deny denylists a CID and removes it from the pending review queue.
func (s *Store) Deny(cid string) {
	s.mu.Lock()
	s.denylist[cid] = time.Now()
	delete(s.submissions, cid)
	s.mu.Unlock()
	s.denylistDirty.Store(true)
	s.submissionsDirty.Store(true)
}

// AddSubmission parks a CID for admin review.
func (s *Store) AddSubmission(cid, kind, owner string) {
	s.mu.Lock()
	s.submissions[cid] = &Submission{
		CID:         cid,
		Kind:        kind,
		Owner:       strings.TrimSpace(owner),
		SubmittedAt: time.Now(),
	}
	s.mu.Unlock()
	s.submissionsDirty.Store(true)
}

// Submissions returns the pending review queue, newest first.
func (s *Store) Submissions() []Submission {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]Submission, 0, len(s.submissions))
	for _, sub := range s.submissions {
		out = append(out, *sub)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].SubmittedAt.After(out[j].SubmittedAt)
	})
	return out
}

// TakeSubmission removes a pending submission and returns it (used on approval).
func (s *Store) TakeSubmission(cid string) (Submission, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sub, ok := s.submissions[cid]
	if !ok {
		return Submission{}, false
	}
	delete(s.submissions, cid)
	s.submissionsDirty.Store(true)
	return *sub, true
}

// DeleteDocument purges an indexed/failed document and any references to it.
// Returns true if the document existed.
func (s *Store) DeleteDocument(cid string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, hasEntry := s.entries[cid]
	_, hasFailure := s.failures[cid]
	if !hasEntry && !hasFailure {
		return false
	}
	s.deleteDocumentLocked(cid)
	return true
}

// DeleteArchive removes an archive record. Member documents that belong to no
// other archive are deleted from the index too; documents shared with another
// archive are kept (only their membership link to this archive is removed).
// Returns true if the archive existed.
func (s *Store) DeleteArchive(cid string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	a := s.archives[cid]
	if a == nil {
		return false
	}
	for _, dc := range a.DocCIDs {
		if s.docInOtherArchiveLocked(dc, cid) {
			if e := s.entries[dc]; e != nil {
				e.Archives = removeString(e.Archives, cid)
				s.indexDirty.Store(true)
			}
		} else {
			s.deleteDocumentLocked(dc)
		}
	}
	delete(s.archives, cid)
	s.archivesDirty.Store(true)
	return true
}

// deleteDocumentLocked removes a document's index entry, keyword links, failure
// record, and references from every archive. Caller must hold s.mu.
func (s *Store) deleteDocumentLocked(cid string) {
	_, wasIndexed := s.entries[cid]
	f, wasFailureRec := s.failures[cid]
	wasPermFailed := wasFailureRec && f.Count >= maxRetries

	if wasIndexed {
		for kw, set := range s.keywordCIDs {
			if _, in := set[cid]; in {
				delete(set, cid)
				if len(set) == 0 {
					delete(s.keywordCIDs, kw)
				}
			}
		}
		delete(s.entries, cid)
		s.indexDirty.Store(true)
	}
	if wasFailureRec {
		delete(s.failures, cid)
		s.failuresDirty.Store(true)
	}
	for _, a := range s.archives {
		if !containsString(a.DocCIDs, cid) {
			continue
		}
		a.DocCIDs = removeString(a.DocCIDs, cid)
		a.DocCount = len(a.DocCIDs)
		if wasIndexed && a.Indexed > 0 {
			a.Indexed--
		}
		if wasPermFailed && a.Failed > 0 {
			a.Failed--
		}
		s.archivesDirty.Store(true)
	}
}

// docInOtherArchiveLocked reports whether docCID is a member of any archive
// other than exclude. Caller must hold s.mu.
func (s *Store) docInOtherArchiveLocked(docCID, exclude string) bool {
	for cid, a := range s.archives {
		if cid == exclude {
			continue
		}
		if containsString(a.DocCIDs, docCID) {
			return true
		}
	}
	return false
}

func (s *Store) saveArchives() {
	s.mu.RLock()
	snapshot := make(map[string]*Archive, len(s.archives))
	for k, v := range s.archives {
		snapshot[k] = v
	}
	s.mu.RUnlock()

	if len(snapshot) == 0 {
		return
	}
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		slog.Error("failed to marshal archives", "error", err)
		return
	}
	atomicWrite(s.archivesPath(), data)
}

func (s *Store) saveIndex() {
	s.mu.RLock()
	snapshot := make(map[string]*IndexEntry, len(s.entries))
	for k, v := range s.entries {
		snapshot[k] = v
	}
	s.mu.RUnlock()

	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		slog.Error("failed to marshal index", "error", err)
		return
	}
	atomicWrite(s.indexPath(), data)
}

func (s *Store) saveFailures() {
	s.mu.RLock()
	snapshot := make(map[string]*failureRecord, len(s.failures))
	for cidStr, f := range s.failures {
		snapshot[cidStr] = f
	}
	s.mu.RUnlock()

	if len(snapshot) == 0 {
		return
	}
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		slog.Error("failed to marshal failures", "error", err)
		return
	}
	atomicWrite(s.failuresPath(), data)
}

func (s *Store) loadSubmissions() {
	data, err := os.ReadFile(s.submissionsPath())
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Warn("failed to read submissions", "path", s.submissionsPath(), "error", err)
		}
		return
	}
	var records map[string]*Submission
	if err := json.Unmarshal(data, &records); err != nil {
		slog.Warn("failed to parse submissions", "path", s.submissionsPath(), "error", err)
		return
	}
	for cidStr, sub := range records {
		if sub.CID == "" {
			sub.CID = cidStr
		}
		s.submissions[cidStr] = sub
	}
	if len(records) > 0 {
		slog.Info("loaded pending submissions", "count", len(records))
	}
}

func (s *Store) saveSubmissions() {
	s.mu.RLock()
	snapshot := make(map[string]*Submission, len(s.submissions))
	for k, v := range s.submissions {
		snapshot[k] = v
	}
	s.mu.RUnlock()

	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		slog.Error("failed to marshal submissions", "error", err)
		return
	}
	atomicWrite(s.submissionsPath(), data)
}

func (s *Store) loadDenylist() {
	data, err := os.ReadFile(s.denylistPath())
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Warn("failed to read denylist", "path", s.denylistPath(), "error", err)
		}
		return
	}
	var records map[string]time.Time
	if err := json.Unmarshal(data, &records); err != nil {
		slog.Warn("failed to parse denylist", "path", s.denylistPath(), "error", err)
		return
	}
	for cidStr, t := range records {
		s.denylist[cidStr] = t
	}
	if len(records) > 0 {
		slog.Info("loaded denylist", "count", len(records))
	}
}

func (s *Store) saveDenylist() {
	s.mu.RLock()
	snapshot := make(map[string]time.Time, len(s.denylist))
	for k, v := range s.denylist {
		snapshot[k] = v
	}
	s.mu.RUnlock()

	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		slog.Error("failed to marshal denylist", "error", err)
		return
	}
	atomicWrite(s.denylistPath(), data)
}

func (s *Store) loadSettings() {
	data, err := os.ReadFile(s.settingsPath())
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Warn("failed to read settings", "path", s.settingsPath(), "error", err)
		}
		return
	}
	var st persistedSettings
	if err := json.Unmarshal(data, &st); err != nil {
		slog.Warn("failed to parse settings", "path", s.settingsPath(), "error", err)
		return
	}
	s.reviewEnabled = st.ReviewEnabled
	slog.Info("loaded settings", "review_enabled", st.ReviewEnabled)
}

func (s *Store) saveSettings() {
	s.mu.RLock()
	st := persistedSettings{ReviewEnabled: s.reviewEnabled}
	s.mu.RUnlock()

	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		slog.Error("failed to marshal settings", "error", err)
		return
	}
	atomicWrite(s.settingsPath(), data)
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
