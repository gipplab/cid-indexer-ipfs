package main

import (
	"database/sql"
	"log/slog"
	"strings"
	"time"
)

// ReviewEnabled reports whether admin review mode is on.
func (s *Store) ReviewEnabled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reviewEnabled
}

func (s *Store) loadReviewEnabled() bool {
	var v string
	s.db.QueryRow("SELECT value FROM settings WHERE key=?", settingReviewEnabled).Scan(&v)
	return v == "1"
}

// SetReviewEnabled toggles review mode and persists the setting.
func (s *Store) SetReviewEnabled(enabled bool) {
	v := "0"
	if enabled {
		v = "1"
	}
	err := s.write(func(tx *sql.Tx) error {
		_, err := tx.Exec(`
INSERT INTO settings(key, value) VALUES(?,?)
ON CONFLICT(key) DO UPDATE SET value=excluded.value`, settingReviewEnabled, v)
		return err
	})
	if err != nil {
		slog.Error("set review mode failed", "error", err)
		return
	}
	s.mu.Lock()
	s.reviewEnabled = enabled
	s.mu.Unlock()
}

// IsDenied reports whether a CID is on the admin denylist.
func (s *Store) IsDenied(cid string) bool {
	var v int
	s.db.QueryRow("SELECT 1 FROM denylist WHERE cid=?", cid).Scan(&v)
	return v == 1
}

// Deny denylist a CID and removes it from the review queue.
func (s *Store) Deny(cid string) {
	err := s.write(func(tx *sql.Tx) error {
		if _, err := tx.Exec(
			"INSERT OR REPLACE INTO denylist(cid, added_at) VALUES(?,?)", cid, nano(time.Now())); err != nil {
			return err
		}
		_, err := tx.Exec("DELETE FROM submissions WHERE cid=?", cid)
		return err
	})
	if err != nil {
		slog.Error("deny failed", "cid", cid, "error", err)
	}
}

// AddSubmission parks a CID for admin review.
func (s *Store) AddSubmission(cid, kind, owner string) {
	err := s.write(func(tx *sql.Tx) error {
		_, err := tx.Exec(`
INSERT INTO submissions(cid, kind, owner, submitted_at) VALUES(?,?,?,?)
ON CONFLICT(cid) DO UPDATE SET kind=excluded.kind, owner=excluded.owner, submitted_at=excluded.submitted_at`,
			cid, kind, strings.TrimSpace(owner), nano(time.Now()))
		return err
	})
	if err != nil {
		slog.Error("add submission failed", "cid", cid, "error", err)
	}
}

// Submissions returns the pending review queue, newest first.
func (s *Store) Submissions() []Submission {
	rows, err := s.db.Query(
		"SELECT cid, kind, owner, submitted_at FROM submissions ORDER BY submitted_at DESC")
	if err != nil {
		slog.Error("submissions query failed", "error", err)
		return nil
	}
	defer rows.Close()
	var out []Submission
	for rows.Next() {
		var sub Submission
		var submittedAt int64
		if rows.Scan(&sub.CID, &sub.Kind, &sub.Owner, &submittedAt) == nil {
			sub.SubmittedAt = fromNano(submittedAt)
			out = append(out, sub)
		}
	}
	return out
}

// TakeSubmission removes a pending submission and returns it.
func (s *Store) TakeSubmission(cid string) (Submission, bool) {
	var sub Submission
	var submittedAt int64
	found := false
	err := s.write(func(tx *sql.Tx) error {
		row := tx.QueryRow("SELECT cid, kind, owner, submitted_at FROM submissions WHERE cid=?", cid)
		if err := row.Scan(&sub.CID, &sub.Kind, &sub.Owner, &submittedAt); err != nil {
			if err == sql.ErrNoRows {
				return nil
			}
			return err
		}
		found = true
		_, err := tx.Exec("DELETE FROM submissions WHERE cid=?", cid)
		return err
	})
	if err != nil {
		slog.Error("take submission failed", "cid", cid, "error", err)
		return Submission{}, false
	}
	if !found {
		return Submission{}, false
	}
	sub.SubmittedAt = fromNano(submittedAt)
	return sub, true
}

// DeleteDocument removes an indexed or failed document and its references.
func (s *Store) DeleteDocument(cid string) bool {
	found := false
	err := s.write(func(tx *sql.Tx) error {
		var hasEntry, hasFailure int
		tx.QueryRow("SELECT 1 FROM documents WHERE cid=?", cid).Scan(&hasEntry)
		tx.QueryRow("SELECT 1 FROM failures WHERE cid=?", cid).Scan(&hasFailure)
		if hasEntry == 0 && hasFailure == 0 {
			return nil
		}
		found = true
		return deleteDocumentTx(tx, cid)
	})
	if err != nil {
		slog.Error("delete document failed", "cid", cid, "error", err)
		return false
	}
	return found
}

// DeleteArchive removes an archive. Orphaned member documents are deleted;
// documents shared with another archive are kept.
func (s *Store) DeleteArchive(cid string) bool {
	found := false
	err := s.write(func(tx *sql.Tx) error {
		var exists int
		tx.QueryRow("SELECT 1 FROM archives WHERE cid=?", cid).Scan(&exists)
		if exists == 0 {
			return nil
		}
		found = true

		docs, err := txStrings(tx, "SELECT doc_cid FROM archive_docs WHERE archive_cid=?", cid)
		if err != nil {
			return err
		}
		for _, dc := range docs {
			var others int
			tx.QueryRow(
				"SELECT COUNT(*) FROM archive_docs WHERE doc_cid=? AND archive_cid<>?", dc, cid).Scan(&others)
			if others > 0 {
				continue // shared: keep the document, drop only this membership below
			}
			if err := deleteDocumentTx(tx, dc); err != nil {
				return err
			}
		}
		if _, err := tx.Exec("DELETE FROM archive_docs WHERE archive_cid=?", cid); err != nil {
			return err
		}
		_, err = tx.Exec("DELETE FROM archives WHERE cid=?", cid)
		return err
	})
	if err != nil {
		slog.Error("delete archive failed", "cid", cid, "error", err)
		return false
	}
	return found
}

// deleteDocumentTx removes a document and updates archive counts. Caller holds the transaction.
func deleteDocumentTx(tx *sql.Tx, cid string) error {
	var rowid int64
	wasIndexed := false
	if err := tx.QueryRow("SELECT rowid FROM documents WHERE cid=?", cid).Scan(&rowid); err == nil {
		wasIndexed = true
	}
	var failCount int
	tx.QueryRow("SELECT count FROM failures WHERE cid=?", cid).Scan(&failCount)
	wasPermFailed := failCount >= maxRetries

	idx, fail := 0, 0
	if wasIndexed {
		idx = 1
	}
	if wasPermFailed {
		fail = 1
	}
	if _, err := tx.Exec(`
UPDATE archives SET
    doc_count = CASE WHEN doc_count > 0 THEN doc_count - 1 ELSE 0 END,
    indexed   = CASE WHEN ? = 1 AND indexed > 0 THEN indexed - 1 ELSE indexed END,
    failed    = CASE WHEN ? = 1 AND failed  > 0 THEN failed  - 1 ELSE failed  END
WHERE cid IN (SELECT archive_cid FROM archive_docs WHERE doc_cid=?)`, idx, fail, cid); err != nil {
		return err
	}

	if wasIndexed {
		if _, err := tx.Exec("DELETE FROM documents_fts WHERE rowid=?", rowid); err != nil {
			return err
		}
	}
	for _, q := range []string{
		"DELETE FROM documents WHERE cid=?",
		"DELETE FROM labels WHERE doc_cid=?",
		"DELETE FROM failures WHERE cid=?",
		"DELETE FROM archive_docs WHERE doc_cid=?",
	} {
		if _, err := tx.Exec(q, cid); err != nil {
			return err
		}
	}
	return nil
}

// txStrings runs a single-column query inside a transaction.
func txStrings(tx *sql.Tx, query string, args ...interface{}) ([]string, error) {
	rows, err := tx.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
