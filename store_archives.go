package main

import (
	"database/sql"
	"log/slog"
	"strings"
	"time"
)

// scanner is satisfied by both *sql.Row and *sql.Rows.
type scanner interface {
	Scan(dest ...interface{}) error
}

const archiveColumns = "cid, name, owner, status, doc_count, indexed, failed, top_keywords, broad_fields, error, submitted_at, indexed_at"

func scanArchive(sc scanner) (Archive, error) {
	var a Archive
	var topKW, broadFields string
	var submittedAt, indexedAt int64
	err := sc.Scan(&a.CID, &a.Name, &a.Owner, &a.Status, &a.DocCount, &a.Indexed, &a.Failed,
		&topKW, &broadFields, &a.Error, &submittedAt, &indexedAt)
	if err != nil {
		return Archive{}, err
	}
	a.TopKeywords = parseStrings(topKW)
	a.BroadFields = parseFields(broadFields)
	a.SubmittedAt = fromNano(submittedAt)
	a.IndexedAt = fromNano(indexedAt)
	return a, nil
}

// AddArchive registers an archive in the queued state, or updates the owner of
// an existing one. Returns true if the archive was newly created.
func (s *Store) AddArchive(cid, owner string) bool {
	owner = strings.TrimSpace(owner)
	created := false
	err := s.write(func(tx *sql.Tx) error {
		var exists int
		tx.QueryRow("SELECT 1 FROM archives WHERE cid=?", cid).Scan(&exists)
		if exists == 0 {
			_, err := tx.Exec(
				"INSERT INTO archives(cid, owner, status, submitted_at) VALUES(?,?,?,?)",
				cid, owner, archiveQueued, nano(time.Now()))
			created = err == nil
			return err
		}
		if owner != "" {
			_, err := tx.Exec("UPDATE archives SET owner=? WHERE cid=?", owner, cid)
			return err
		}
		return nil
	})
	if err != nil {
		slog.Error("add archive failed", "cid", cid, "error", err)
	}
	return created
}

// MarkArchiveCrawling moves an archive into the crawling state.
func (s *Store) MarkArchiveCrawling(cid string) {
	s.updateArchive("UPDATE archives SET status=? WHERE cid=?", archiveCrawling, cid)
}

// MarkArchiveFailed records a terminal crawl/index failure for an archive.
func (s *Store) MarkArchiveFailed(cid, reason string) {
	s.updateArchive("UPDATE archives SET status=?, error=? WHERE cid=?", archiveFailed, reason, cid)
}

func (s *Store) updateArchive(query string, args ...interface{}) {
	if err := s.write(func(tx *sql.Tx) error {
		_, err := tx.Exec(query, args...)
		return err
	}); err != nil {
		slog.Error("update archive failed", "error", err)
	}
}

// SetArchiveDocs records the document CIDs discovered by the crawler (in order)
// and moves the archive into the indexing state.
func (s *Store) SetArchiveDocs(cid string, docCIDs []string) {
	err := s.write(func(tx *sql.Tx) error {
		if _, err := tx.Exec(
			"UPDATE archives SET doc_count=?, status=? WHERE cid=?",
			len(docCIDs), archiveIndexing, cid); err != nil {
			return err
		}
		if _, err := tx.Exec("DELETE FROM archive_docs WHERE archive_cid=?", cid); err != nil {
			return err
		}
		for i, dc := range docCIDs {
			if _, err := tx.Exec(
				"INSERT OR IGNORE INTO archive_docs(archive_cid, doc_cid, position) VALUES(?,?,?)",
				cid, dc, i); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		slog.Error("set archive docs failed", "cid", cid, "error", err)
	}
}

// FinalizeArchive aggregates the labels of an archive's member documents into
// archive-level keywords and field counts, then marks it done.
func (s *Store) FinalizeArchive(cid string) {
	var exists int
	s.db.QueryRow("SELECT 1 FROM archives WHERE cid=?", cid).Scan(&exists)
	if exists == 0 {
		return
	}

	kwCount := make(map[string]int)
	fieldCount := make(map[string]int)
	indexed := 0

	rows, err := s.db.Query(`
SELECT d.keywords, d.broad_field
FROM archive_docs ad
JOIN documents d ON d.cid = ad.doc_cid
WHERE ad.archive_cid=?`, cid)
	if err != nil {
		slog.Error("finalize archive query failed", "cid", cid, "error", err)
		return
	}
	for rows.Next() {
		var keywords, broadField string
		if err := rows.Scan(&keywords, &broadField); err != nil {
			continue
		}
		indexed++
		for _, kw := range parseStrings(keywords) {
			if kw = strings.ToLower(strings.TrimSpace(kw)); kw != "" {
				kwCount[kw]++
			}
		}
		if bf := strings.TrimSpace(broadField); bf != "" {
			fieldCount[bf]++
		}
	}
	rows.Close()

	var failed int
	s.db.QueryRow(`
SELECT COUNT(*)
FROM archive_docs ad
JOIN failures f ON f.cid = ad.doc_cid
WHERE ad.archive_cid=? AND f.count >= ?`, cid, maxRetries).Scan(&failed)

	topKeywords := topCounted(kwCount, 12)
	broadFields := topFields(fieldCount)

	err = s.write(func(tx *sql.Tx) error {
		var name string
		tx.QueryRow("SELECT name FROM archives WHERE cid=?", cid).Scan(&name)
		if name == "" {
			if len(broadFields) > 0 {
				name = broadFields[0].Field
			} else {
				name = cid
			}
		}
		_, err := tx.Exec(`
UPDATE archives SET indexed=?, failed=?, top_keywords=?, broad_fields=?,
    status=?, indexed_at=?, name=?
WHERE cid=?`,
			indexed, failed, marshalJSON(topKeywords), marshalJSON(broadFields),
			archiveDone, nano(time.Now()), name, cid)
		return err
	})
	if err != nil {
		slog.Error("finalize archive update failed", "cid", cid, "error", err)
	}
}

// Archives returns all archives, newest submission first.
func (s *Store) Archives() []Archive {
	rows, err := s.db.Query("SELECT " + archiveColumns + " FROM archives ORDER BY submitted_at DESC")
	if err != nil {
		slog.Error("archives query failed", "error", err)
		return nil
	}
	defer rows.Close()
	var out []Archive
	for rows.Next() {
		if a, err := scanArchive(rows); err == nil {
			out = append(out, a)
		}
	}
	return out
}

// SearchArchives returns archives whose aggregated labels match every term in
// the query (AND logic). An empty query returns all archives.
func (s *Store) SearchArchives(query string) []Archive {
	query = strings.ToLower(strings.TrimSpace(query))
	all := s.Archives()
	if query == "" {
		return all
	}
	terms := strings.Fields(query)
	var out []Archive
	for _, a := range all {
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
			out = append(out, a)
		}
	}
	return out
}

// ArchiveRefs resolves a set of archive CIDs to lightweight references
// (CID + name + status) for display, preserving input order.
func (s *Store) ArchiveRefs(cids []string) []ArchiveRef {
	if len(cids) == 0 {
		return nil
	}
	args := make([]interface{}, len(cids))
	for i, c := range cids {
		args[i] = c
	}
	rows, err := s.db.Query(
		"SELECT cid, name, status FROM archives WHERE cid IN ("+placeholders(len(cids))+")", args...)
	if err != nil {
		slog.Error("archive refs query failed", "error", err)
		return nil
	}
	defer rows.Close()

	found := make(map[string]ArchiveRef)
	for rows.Next() {
		var ref ArchiveRef
		if rows.Scan(&ref.CID, &ref.Name, &ref.Status) == nil {
			found[ref.CID] = ref
		}
	}
	refs := make([]ArchiveRef, 0, len(cids))
	for _, cid := range cids {
		if ref, ok := found[cid]; ok {
			refs = append(refs, ref)
		} else {
			refs = append(refs, ArchiveRef{CID: cid})
		}
	}
	return refs
}

// GetArchive returns an archive and the indexed entries of its member documents
// (in archive order).
func (s *Store) GetArchive(cid string) (Archive, []IndexEntry, bool) {
	a, err := scanArchive(s.db.QueryRow("SELECT "+archiveColumns+" FROM archives WHERE cid=?", cid))
	if err == sql.ErrNoRows {
		return Archive{}, nil, false
	}
	if err != nil {
		slog.Error("get archive failed", "cid", cid, "error", err)
		return Archive{}, nil, false
	}
	a.DocCIDs = s.ArchiveDocCIDs(cid)

	rows, err := s.db.Query(`
SELECT d.cid, d.title, d.broad_field, d.sub_topic, d.keywords, d.indexed_at
FROM archive_docs ad
JOIN documents d ON d.cid = ad.doc_cid
WHERE ad.archive_cid=?
ORDER BY ad.position`, cid)
	if err != nil {
		slog.Error("get archive docs failed", "cid", cid, "error", err)
		return a, nil, true
	}
	defer rows.Close()
	return a, s.scanEntries(rows), true
}

// ArchiveFailures returns the permanently-failed member documents of an
// archive, most recent attempt first.
func (s *Store) ArchiveFailures(cid string) []FailedDoc {
	rows, err := s.db.Query(`
SELECT f.cid, f.count, f.last_try, f.reason
FROM archive_docs ad
JOIN failures f ON f.cid = ad.doc_cid
WHERE ad.archive_cid=? AND f.count >= ?
ORDER BY f.last_try DESC`, cid, maxRetries)
	if err != nil {
		slog.Error("archive failures query failed", "cid", cid, "error", err)
		return nil
	}
	defer rows.Close()
	return scanFailures(rows)
}

// ArchivesContaining returns the CIDs of archives that list the given document
// CID among their members.
func (s *Store) ArchivesContaining(docCID string) []string {
	rows, err := s.db.Query("SELECT archive_cid FROM archive_docs WHERE doc_cid=?", docCID)
	if err != nil {
		slog.Error("archives containing query failed", "cid", docCID, "error", err)
		return nil
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var cid string
		if rows.Scan(&cid) == nil {
			out = append(out, cid)
		}
	}
	return out
}

// ArchiveDocCIDs returns a copy of the document CIDs discovered for an archive,
// in crawl order. A non-empty result means the crawl phase completed already.
func (s *Store) ArchiveDocCIDs(cid string) []string {
	rows, err := s.db.Query("SELECT doc_cid FROM archive_docs WHERE archive_cid=? ORDER BY position", cid)
	if err != nil {
		slog.Error("archive doc cids query failed", "cid", cid, "error", err)
		return nil
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var dc string
		if rows.Scan(&dc) == nil {
			out = append(out, dc)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// ResumableArchives returns the CIDs of archives interrupted before completion
// (still queued, crawling, or indexing) that should be re-run on startup.
func (s *Store) ResumableArchives() []string {
	rows, err := s.db.Query(
		"SELECT cid FROM archives WHERE status IN (?,?,?)",
		archiveQueued, archiveCrawling, archiveIndexing)
	if err != nil {
		slog.Error("resumable archives query failed", "error", err)
		return nil
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var cid string
		if rows.Scan(&cid) == nil {
			out = append(out, cid)
		}
	}
	return out
}
