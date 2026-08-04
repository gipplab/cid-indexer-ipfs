package main

import (
	"database/sql"
	"log/slog"
	"sort"
	"strings"
	"time"
)

// Pending returns CIDs that are neither indexed nor permanently failed.
func (s *Store) Pending(cids []string) []string {
	indexed := s.cidSet("SELECT cid FROM documents")
	failed := s.cidSet("SELECT cid FROM failures WHERE count >= ?", maxRetries)

	var pending []string
	for _, c := range cids {
		if _, ok := indexed[c]; ok {
			continue
		}
		if _, ok := failed[c]; ok {
			continue
		}
		pending = append(pending, c)
	}
	s.mu.Lock()
	s.pending = len(pending)
	s.mu.Unlock()
	return pending
}

// Add stores an indexed entry and clears any prior failure record.
func (s *Store) Add(entry *IndexEntry) {
	err := s.write(func(tx *sql.Tx) error {
		if _, err := tx.Exec(`
INSERT INTO documents(cid, title, broad_field, sub_topic, keywords, indexed_at)
VALUES(?,?,?,?,?,?)
ON CONFLICT(cid) DO UPDATE SET
    title=excluded.title, broad_field=excluded.broad_field,
    sub_topic=excluded.sub_topic, keywords=excluded.keywords,
    indexed_at=excluded.indexed_at`,
			entry.CID, entry.Title, entry.BroadField, entry.SubTopic,
			marshalJSON(entry.Keywords), nano(entry.IndexedAt)); err != nil {
			return err
		}

		var rowid int64
		if err := tx.QueryRow("SELECT rowid FROM documents WHERE cid=?", entry.CID).Scan(&rowid); err != nil {
			return err
		}
		if _, err := tx.Exec("DELETE FROM documents_fts WHERE rowid=?", rowid); err != nil {
			return err
		}
		if _, err := tx.Exec(`
INSERT INTO documents_fts(rowid, cid, title, broad_field, sub_topic, keywords)
VALUES(?,?,?,?,?,?)`,
			rowid, entry.CID, entry.Title, entry.BroadField, entry.SubTopic,
			strings.Join(entry.Keywords, " ")); err != nil {
			return err
		}

		if _, err := tx.Exec("DELETE FROM labels WHERE doc_cid=?", entry.CID); err != nil {
			return err
		}
		for _, label := range entry.labelSet() {
			if _, err := tx.Exec("INSERT OR IGNORE INTO labels(label, doc_cid) VALUES(?,?)", label, entry.CID); err != nil {
				return err
			}
		}
		_, err := tx.Exec("DELETE FROM failures WHERE cid=?", entry.CID)
		return err
	})
	if err != nil {
		slog.Error("failed to add entry", "cid", entry.CID, "error", err)
		return
	}
	s.mu.Lock()
	if s.pending > 0 {
		s.pending--
	}
	s.mu.Unlock()
}

func (e *IndexEntry) labelSet() []string {
	seen := make(map[string]struct{})
	var out []string
	add := func(s string) {
		s = strings.ToLower(strings.TrimSpace(s))
		if s == "" {
			return
		}
		if _, ok := seen[s]; ok {
			return
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	for _, kw := range e.Keywords {
		add(kw)
	}
	add(e.BroadField)
	add(e.SubTopic)
	return out
}

// RecordFailure increments the failure counter for a CID.
func (s *Store) RecordFailure(cid, reason string) {
	now := nano(time.Now())
	var count int
	err := s.write(func(tx *sql.Tx) error {
		if _, err := tx.Exec(`
INSERT INTO failures(cid, count, last_try, reason) VALUES(?,1,?,?)
ON CONFLICT(cid) DO UPDATE SET count=count+1, last_try=excluded.last_try, reason=excluded.reason`,
			cid, now, reason); err != nil {
			return err
		}
		return tx.QueryRow("SELECT count FROM failures WHERE cid=?", cid).Scan(&count)
	})
	if err != nil {
		slog.Error("failed to record failure", "cid", cid, "error", err)
		return
	}
	if count >= maxRetries {
		s.mu.Lock()
		if s.pending > 0 {
			s.pending--
		}
		s.mu.Unlock()
		slog.Warn("permanently failed", "cid", cid, "reason", reason)
	}
}

// RecordRateLimited notes a rate-limit failure without incrementing the failure count.
func (s *Store) RecordRateLimited(cid, reason string) {
	now := nano(time.Now())
	err := s.write(func(tx *sql.Tx) error {
		_, err := tx.Exec(`
INSERT INTO failures(cid, count, last_try, reason) VALUES(?,0,?,?)
ON CONFLICT(cid) DO UPDATE SET last_try=excluded.last_try, reason=excluded.reason`,
			cid, now, reason)
		return err
	})
	if err != nil {
		slog.Error("failed to record rate-limit", "cid", cid, "error", err)
	}
}

// RequeueRateLimited clears rate-limit failure records. Returns the count cleared.
func (s *Store) RequeueRateLimited() int {
	var n int64
	err := s.write(func(tx *sql.Tx) error {
		res, err := tx.Exec("DELETE FROM failures WHERE reason LIKE '%rate limited%'")
		if err != nil {
			return err
		}
		n, _ = res.RowsAffected()
		return nil
	})
	if err != nil {
		slog.Error("failed to requeue rate-limited failures", "error", err)
		return 0
	}
	return int(n)
}

// Failures returns permanently failed documents, most recent first.
func (s *Store) Failures() []FailedDoc {
	rows, err := s.db.Query(
		"SELECT cid, count, last_try, reason FROM failures WHERE count >= ? ORDER BY last_try DESC",
		maxRetries)
	if err != nil {
		slog.Error("failures query failed", "error", err)
		return nil
	}
	defer rows.Close()
	return scanFailures(rows)
}

// RetryFailure clears a CID's failure record. Returns true if a record existed.
func (s *Store) RetryFailure(cid string) bool {
	var n int64
	err := s.write(func(tx *sql.Tx) error {
		res, err := tx.Exec("DELETE FROM failures WHERE cid=?", cid)
		if err != nil {
			return err
		}
		n, _ = res.RowsAffected()
		return nil
	})
	if err != nil {
		slog.Error("retry failure delete failed", "cid", cid, "error", err)
		return false
	}
	return n > 0
}

// RecordSkip marks a non-indexable CID (e.g. non-PDF) as permanently done.
func (s *Store) RecordSkip(cid string) {
	now := nano(time.Now())
	err := s.write(func(tx *sql.Tx) error {
		_, err := tx.Exec(`
INSERT INTO failures(cid, count, last_try, reason) VALUES(?,?,?,?)
ON CONFLICT(cid) DO UPDATE SET count=excluded.count, last_try=excluded.last_try, reason=excluded.reason`,
			cid, maxRetries, now, "skipped: not a PDF")
		return err
	})
	if err != nil {
		slog.Error("failed to record skip", "cid", cid, "error", err)
	}
	s.mu.Lock()
	s.skipped++
	if s.pending > 0 {
		s.pending--
	}
	s.mu.Unlock()
}

// Stats returns summary statistics.
func (s *Store) Stats() StoreStats {
	var indexed, failed, kws, archives int
	s.db.QueryRow("SELECT COUNT(*) FROM documents").Scan(&indexed)
	s.db.QueryRow("SELECT COUNT(*) FROM failures WHERE count >= ?", maxRetries).Scan(&failed)
	s.db.QueryRow("SELECT COUNT(DISTINCT label) FROM labels").Scan(&kws)
	s.db.QueryRow("SELECT COUNT(*) FROM archives").Scan(&archives)

	s.mu.Lock()
	pending, skipped := s.pending, s.skipped
	s.mu.Unlock()

	return StoreStats{
		Indexed:        indexed,
		Pending:        pending,
		Failed:         failed,
		Skipped:        skipped,
		UniqueKeywords: kws,
		Archives:       archives,
		Enabled:        indexed > 0,
	}
}

// Search returns all entries matching the query (FTS prefix + AND semantics).
func (s *Store) Search(query string) []IndexEntry {
	results, _ := s.SearchPage(query, 0, -1)
	return results
}

// SearchPage returns one page of matching entries plus the total match count.
func (s *Store) SearchPage(query string, offset, limit int) ([]IndexEntry, int) {
	match := ftsMatch(query)
	if match == "" {
		return nil, 0
	}

	var total int
	if err := s.db.QueryRow(
		"SELECT COUNT(*) FROM documents_fts WHERE documents_fts MATCH ?", match).Scan(&total); err != nil {
		slog.Error("search count failed", "error", err)
		return nil, 0
	}
	if total == 0 {
		return nil, 0
	}

	rows, err := s.db.Query(`
SELECT d.cid, d.title, d.broad_field, d.sub_topic, d.keywords, d.indexed_at
FROM documents_fts f
JOIN documents d ON d.cid = f.cid
WHERE documents_fts MATCH ?
ORDER BY rank, d.indexed_at DESC
LIMIT ? OFFSET ?`, match, limit, offset)
	if err != nil {
		slog.Error("search query failed", "error", err)
		return nil, total
	}
	defer rows.Close()

	results := s.scanEntries(rows)
	return results, total
}

// Suggest returns keyword suggestions matching the prefix, ranked by document count.
func (s *Store) Suggest(prefix string) []KeywordSuggestion {
	prefix = strings.ToLower(strings.TrimSpace(prefix))
	like := "%" + prefix + "%"
	rows, err := s.db.Query(`
SELECT label, COUNT(DISTINCT doc_cid) AS c
FROM labels
WHERE ? = '' OR label LIKE ?
GROUP BY label
ORDER BY c DESC, label ASC
LIMIT 20`, prefix, like)
	if err != nil {
		slog.Error("suggest query failed", "error", err)
		return nil
	}
	defer rows.Close()

	var out []KeywordSuggestion
	for rows.Next() {
		var ks KeywordSuggestion
		if err := rows.Scan(&ks.Keyword, &ks.CIDCount); err == nil {
			out = append(out, ks)
		}
	}
	return out
}

// RecordSearch adds a search to the in-memory recent searches list.
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
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]RecentSearch, len(s.recent))
	for i, sr := range s.recent {
		result[len(s.recent)-1-i] = sr
	}
	return result
}

func (s *Store) cidSet(query string, args ...interface{}) map[string]struct{} {
	out := make(map[string]struct{})
	rows, err := s.db.Query(query, args...)
	if err != nil {
		slog.Error("cidSet query failed", "error", err)
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var cid string
		if rows.Scan(&cid) == nil {
			out[cid] = struct{}{}
		}
	}
	return out
}

// scanEntries reads document rows and fills archive membership in one batch.
func (s *Store) scanEntries(rows *sql.Rows) []IndexEntry {
	var entries []IndexEntry
	var cids []string
	for rows.Next() {
		var e IndexEntry
		var keywords string
		var indexedAt int64
		if err := rows.Scan(&e.CID, &e.Title, &e.BroadField, &e.SubTopic, &keywords, &indexedAt); err != nil {
			continue
		}
		e.Keywords = parseStrings(keywords)
		e.IndexedAt = fromNano(indexedAt)
		entries = append(entries, e)
		cids = append(cids, e.CID)
	}
	if len(entries) == 0 {
		return entries
	}
	membership := s.archivesForDocs(cids)
	for i := range entries {
		entries[i].Archives = membership[entries[i].CID]
	}
	return entries
}

// archivesForDocs maps each given document CID to the archive CIDs containing it.
func (s *Store) archivesForDocs(cids []string) map[string][]string {
	out := make(map[string][]string)
	if len(cids) == 0 {
		return out
	}
	args := make([]interface{}, len(cids))
	for i, c := range cids {
		args[i] = c
	}
	rows, err := s.db.Query(
		"SELECT doc_cid, archive_cid FROM archive_docs WHERE doc_cid IN ("+placeholders(len(cids))+")", args...)
	if err != nil {
		slog.Error("archivesForDocs query failed", "error", err)
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var doc, arch string
		if rows.Scan(&doc, &arch) == nil {
			out[doc] = append(out[doc], arch)
		}
	}
	for _, v := range out {
		sort.Strings(v)
	}
	return out
}

func scanFailures(rows *sql.Rows) []FailedDoc {
	var out []FailedDoc
	for rows.Next() {
		var fd FailedDoc
		var lastTry int64
		if err := rows.Scan(&fd.CID, &fd.Count, &lastTry, &fd.Reason); err == nil {
			fd.LastTry = fromNano(lastTry)
			out = append(out, fd)
		}
	}
	return out
}
