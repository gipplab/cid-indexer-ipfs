package main

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const defaultPort = 8384

const (
	sessionCookie = "admin_session"
	sessionTTL    = 12 * time.Hour
)

var indexingActive atomic.Bool

// sessionStore holds active admin sessions in memory (token -> expiry). Sessions
// are intentionally not persisted; restarting the server requires re-login.
type sessionStore struct {
	mu       sync.Mutex
	sessions map[string]time.Time
}

func newSessionStore() *sessionStore {
	return &sessionStore{sessions: make(map[string]time.Time)}
}

func (ss *sessionStore) create() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	tok := hex.EncodeToString(b)
	ss.mu.Lock()
	ss.sessions[tok] = time.Now().Add(sessionTTL)
	ss.mu.Unlock()
	return tok, nil
}

func (ss *sessionStore) valid(tok string) bool {
	if tok == "" {
		return false
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	exp, ok := ss.sessions[tok]
	if !ok {
		return false
	}
	if time.Now().After(exp) {
		delete(ss.sessions, tok)
		return false
	}
	return true
}

func (ss *sessionStore) destroy(tok string) {
	ss.mu.Lock()
	delete(ss.sessions, tok)
	ss.mu.Unlock()
}

// authed reports whether the request carries a valid admin session cookie.
func (ss *sessionStore) authed(r *http.Request) bool {
	c, err := r.Cookie(sessionCookie)
	if err != nil {
		return false
	}
	return ss.valid(c.Value)
}

// searchResult is an indexed document plus the resolved archives that contain
// it, returned by /api/search. The embedded IndexEntry's fields are flattened
// into the JSON object alongside archive_refs.
type searchResult struct {
	IndexEntry
	ArchiveRefs []ArchiveRef `json:"archive_refs,omitempty"`
}

func startServer(store *Store, port int, cfg PipelineConfig, ix *Indexer) error {
	mux := http.NewServeMux()
	sessions := newSessionStore()

	// requireAdmin wraps a handler so it only runs for authenticated admins.
	requireAdmin := func(h http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if !sessions.authed(r) {
				writeJSONError(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			h(w, r)
		}
	}

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(dashboardHTML(cfg.Gateway)))
	})

	mux.HandleFunc("/admin", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(adminHTML(cfg.Gateway)))
	})

	mux.HandleFunc("/api/search", func(w http.ResponseWriter, r *http.Request) {
		query := strings.TrimSpace(r.URL.Query().Get("q"))
		if query == "" {
			writeJSON(w, map[string]interface{}{"results": []struct{}{}, "count": 0})
			return
		}
		results := store.Search(query)
		store.RecordSearch(query, len(results))
		enriched := make([]searchResult, 0, len(results))
		for _, e := range results {
			enriched = append(enriched, searchResult{
				IndexEntry:  e,
				ArchiveRefs: store.ArchiveRefs(e.Archives),
			})
		}
		writeJSON(w, map[string]interface{}{"query": query, "results": enriched, "count": len(enriched)})
	})

	mux.HandleFunc("/api/suggest", func(w http.ResponseWriter, r *http.Request) {
		prefix := strings.TrimSpace(r.URL.Query().Get("q"))
		suggestions := store.Suggest(prefix)
		if suggestions == nil {
			suggestions = []KeywordSuggestion{}
		}
		writeJSON(w, map[string]interface{}{"suggestions": suggestions})
	})

	mux.HandleFunc("/api/recent", func(w http.ResponseWriter, r *http.Request) {
		recent := store.GetRecentSearches()
		if recent == nil {
			recent = []RecentSearch{}
		}
		writeJSON(w, map[string]interface{}{"searches": recent})
	})

	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		stats := store.Stats()
		stats.Indexing = indexingActive.Load()
		stats.Queued = ix.Queued()
		stats.Model = cfg.Model
		stats.APIBase = cfg.APIBase
		writeJSON(w, stats)
	})

	// /api/submit accepts a single pasted CID (plus optional owner) and
	// auto-classifies it: a directory becomes an archive (crawled + indexed),
	// anything else is indexed as a standalone document.
	mux.HandleFunc("/api/submit", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		cid := strings.TrimSpace(r.FormValue("cid"))
		owner := strings.TrimSpace(r.FormValue("owner"))
		if cid == "" {
			writeJSONError(w, "missing cid", http.StatusBadRequest)
			return
		}

		if store.IsDenied(cid) {
			writeJSONError(w, "this CID has been denylisted by an administrator", http.StatusForbidden)
			return
		}

		apiKey := loadAPIKey(cfg.DataDir)
		if apiKey == "" {
			writeJSON(w, map[string]interface{}{
				"message": "no API key configured for indexing",
			})
			return
		}

		// Classify so we can route and label the submission. This is a single
		// lightweight gateway request; indexing itself is queued and happens in
		// the background, so new submissions are never rejected.
		cr := newCrawler(cfg.Gateway, cfg.MaxDepth, cfg.MaxDocs)
		kind, err := cr.Classify(cid)
		if err != nil {
			writeJSONError(w, "could not fetch CID from gateway: "+err.Error(), http.StatusBadGateway)
			return
		}

		subKind := submissionDocument
		if kind == kindDir {
			subKind = submissionArchive
		}

		// In review mode, park the submission for admin moderation instead of
		// indexing it. It is held out of the archives list until approved.
		if store.ReviewEnabled() {
			store.AddSubmission(cid, subKind, owner)
			writeJSON(w, map[string]interface{}{
				"type":    subKind,
				"review":  true,
				"message": "submitted for admin review",
				"cid":     cid,
			})
			return
		}

		if kind == kindDir {
			ix.EnqueueArchive(cid, owner)
			writeJSON(w, map[string]interface{}{
				"type":    "archive",
				"message": "archive queued for crawling and indexing",
				"cid":     cid,
				"queued":  ix.Queued(),
			})
			return
		}

		ix.EnqueueDocs([]string{cid})
		writeJSON(w, map[string]interface{}{
			"type":    "document",
			"message": "document queued for indexing",
			"cid":     cid,
			"queued":  ix.Queued(),
		})
	})

	mux.HandleFunc("/api/failures", func(w http.ResponseWriter, r *http.Request) {
		failures := store.Failures()
		if failures == nil {
			failures = []FailedDoc{}
		}
		writeJSON(w, map[string]interface{}{"failures": failures, "count": len(failures)})
	})

	// /api/retry clears a permanently-failed document's failure record so it
	// becomes pending again, then re-enqueues it. If the document belongs to one
	// or more archives, those archives are re-run (skipping the crawl) so the
	// document is reindexed and its archive membership is restored on finalize.
	mux.HandleFunc("/api/retry", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		cid := strings.TrimSpace(r.FormValue("cid"))
		if cid == "" {
			writeJSONError(w, "missing cid", http.StatusBadRequest)
			return
		}
		if !store.RetryFailure(cid) {
			writeJSONError(w, "no failed document with that cid", http.StatusNotFound)
			return
		}
		if archives := store.ArchivesContaining(cid); len(archives) > 0 {
			for _, a := range archives {
				ix.EnqueueArchive(a, "")
			}
		} else {
			ix.EnqueueDocs([]string{cid})
		}
		writeJSON(w, map[string]interface{}{
			"message": "document re-queued for indexing",
			"cid":     cid,
			"queued":  ix.Queued(),
		})
	})

	mux.HandleFunc("/api/archives", func(w http.ResponseWriter, r *http.Request) {
		archives := store.Archives()
		if archives == nil {
			archives = []Archive{}
		}
		writeJSON(w, map[string]interface{}{"archives": archives, "count": len(archives)})
	})

	mux.HandleFunc("/api/archives/search", func(w http.ResponseWriter, r *http.Request) {
		query := strings.TrimSpace(r.URL.Query().Get("q"))
		archives := store.SearchArchives(query)
		if archives == nil {
			archives = []Archive{}
		}
		writeJSON(w, map[string]interface{}{"query": query, "archives": archives, "count": len(archives)})
	})

	mux.HandleFunc("/api/archive", func(w http.ResponseWriter, r *http.Request) {
		cid := strings.TrimSpace(r.URL.Query().Get("cid"))
		if cid == "" {
			writeJSONError(w, "missing cid", http.StatusBadRequest)
			return
		}
		archive, docs, ok := store.GetArchive(cid)
		if !ok {
			writeJSONError(w, "archive not found", http.StatusNotFound)
			return
		}
		if docs == nil {
			docs = []IndexEntry{}
		}
		failures := store.ArchiveFailures(cid)
		if failures == nil {
			failures = []FailedDoc{}
		}
		writeJSON(w, map[string]interface{}{"archive": archive, "documents": docs, "failures": failures})
	})

	// --- Admin auth + moderation ---

	mux.HandleFunc("/api/admin/login", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		adminKey := loadAPIKey(cfg.DataDir)
		if adminKey == "" {
			writeJSONError(w, "no API key configured on the server", http.StatusServiceUnavailable)
			return
		}
		key := strings.TrimSpace(r.FormValue("key"))
		if subtle.ConstantTimeCompare([]byte(key), []byte(adminKey)) != 1 {
			writeJSONError(w, "invalid key", http.StatusUnauthorized)
			return
		}
		tok, err := sessions.create()
		if err != nil {
			writeJSONError(w, "could not create session", http.StatusInternalServerError)
			return
		}
		http.SetCookie(w, &http.Cookie{
			Name:     sessionCookie,
			Value:    tok,
			Path:     "/",
			HttpOnly: true,
			SameSite: http.SameSiteLaxMode,
			MaxAge:   int(sessionTTL.Seconds()),
		})
		writeJSON(w, map[string]interface{}{"authed": true})
	})

	mux.HandleFunc("/api/admin/logout", func(w http.ResponseWriter, r *http.Request) {
		if c, err := r.Cookie(sessionCookie); err == nil {
			sessions.destroy(c.Value)
		}
		http.SetCookie(w, &http.Cookie{
			Name:     sessionCookie,
			Value:    "",
			Path:     "/",
			HttpOnly: true,
			MaxAge:   -1,
		})
		writeJSON(w, map[string]interface{}{"authed": false})
	})

	mux.HandleFunc("/api/admin/session", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]interface{}{
			"authed":         sessions.authed(r),
			"review_enabled": store.ReviewEnabled(),
		})
	})

	mux.HandleFunc("/api/admin/submissions", requireAdmin(func(w http.ResponseWriter, r *http.Request) {
		subs := store.Submissions()
		if subs == nil {
			subs = []Submission{}
		}
		writeJSON(w, map[string]interface{}{
			"submissions":    subs,
			"count":          len(subs),
			"review_enabled": store.ReviewEnabled(),
		})
	}))

	mux.HandleFunc("/api/admin/review-mode", requireAdmin(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		v := strings.TrimSpace(strings.ToLower(r.FormValue("enabled")))
		enabled := v == "true" || v == "1" || v == "on" || v == "yes"
		store.SetReviewEnabled(enabled)
		writeJSON(w, map[string]interface{}{"review_enabled": enabled})
	}))

	mux.HandleFunc("/api/admin/approve", requireAdmin(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		cid := strings.TrimSpace(r.FormValue("cid"))
		if cid == "" {
			writeJSONError(w, "missing cid", http.StatusBadRequest)
			return
		}
		sub, ok := store.TakeSubmission(cid)
		if !ok {
			writeJSONError(w, "no pending submission with that cid", http.StatusNotFound)
			return
		}
		if sub.Kind == submissionArchive {
			ix.EnqueueArchive(cid, sub.Owner)
		} else {
			ix.EnqueueDocs([]string{cid})
		}
		writeJSON(w, map[string]interface{}{
			"message": "submission approved and queued for indexing",
			"cid":     cid,
			"type":    sub.Kind,
			"queued":  ix.Queued(),
		})
	}))

	mux.HandleFunc("/api/admin/deny", requireAdmin(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		cid := strings.TrimSpace(r.FormValue("cid"))
		if cid == "" {
			writeJSONError(w, "missing cid", http.StatusBadRequest)
			return
		}
		store.Deny(cid)
		writeJSON(w, map[string]interface{}{"message": "submission denied and denylisted", "cid": cid})
	}))

	mux.HandleFunc("/api/admin/delete-archive", requireAdmin(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		cid := strings.TrimSpace(r.FormValue("cid"))
		if cid == "" {
			writeJSONError(w, "missing cid", http.StatusBadRequest)
			return
		}
		if !store.DeleteArchive(cid) {
			writeJSONError(w, "archive not found", http.StatusNotFound)
			return
		}
		store.SaveAll()
		writeJSON(w, map[string]interface{}{"message": "archive removed", "cid": cid})
	}))

	mux.HandleFunc("/api/admin/delete-document", requireAdmin(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		cid := strings.TrimSpace(r.FormValue("cid"))
		if cid == "" {
			writeJSONError(w, "missing cid", http.StatusBadRequest)
			return
		}
		if !store.DeleteDocument(cid) {
			writeJSONError(w, "document not found", http.StatusNotFound)
			return
		}
		store.SaveAll()
		writeJSON(w, map[string]interface{}{"message": "document removed", "cid": cid})
	}))

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 60 * time.Second, // /api/submit classifies via the gateway synchronously
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		<-sigCh
		slog.Info("shutting down, flushing state to disk")
		store.SaveAll()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	}()

	slog.Info("web UI started", "url", fmt.Sprintf("http://localhost:%d", port))
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(v)
}

func writeJSONError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func dashboardHTML(gateway string) string {
	return `<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>CID Indexer - Keyword Search</title>
    <style>
        * { box-sizing: border-box; }
        body { font-family: "Courier New", Courier, monospace; margin: 0; background: #f8f9fa; color: #333; }
        .container { max-width: 960px; margin: 0 auto; padding: 20px; }
        h1 { color: #222; text-transform: uppercase; border-bottom: 2px solid #333; padding-bottom: 10px; font-size: 1.4em; }
        .disclosure { cursor: pointer; user-select: none; font-size: 0.8em; text-transform: uppercase; letter-spacing: 0.08em; color: #555; display: inline-flex; align-items: center; white-space: nowrap; background: none; border: none; padding: 0; }
        .disclosure:hover { color: #111; }
        .disclosure.open { color: #1565c0; }
        .stats-row { display: none; gap: 20px; margin: 10px 0 0; flex-wrap: wrap; }
        .stats-row.open { display: flex; }
        .stat-card { background: white; padding: 15px 20px; border: 1px solid #333; text-align: center; flex: 1 1 140px; }
        .stat-value { font-size: 2em; font-weight: 700; color: #000; }
        .stat-label { color: #555; font-size: 0.75em; text-transform: uppercase; margin-top: 4px; }
        .section { background: white; padding: 20px; border: 1px solid #333; margin: 20px 0; }
        .search-row { display: flex; align-items: center; gap: 10px; margin-top: 10px; }
        .input-wrap { position: relative; flex: 1; }
        .input-wrap input { width: 100%; padding: 12px 14px; border: 1px solid #333; font-family: inherit; font-size: 1em; }
        .input-wrap input:focus { outline: none; border-color: #06A77D; }
        .ac-panel { position: absolute; top: 100%; left: 0; right: 0; background: white; border: 1px solid #333; border-top: none; max-height: 220px; overflow-y: auto; z-index: 100; display: none; }
        .ac-item { padding: 8px 14px; cursor: pointer; display: flex; justify-content: space-between; font-size: 0.9em; }
        .ac-item:hover, .ac-item.active { background: #f0f0f0; }
        .ac-count { color: #999; font-size: 0.85em; }
        .btn { background: none; border: 1px solid #999; cursor: pointer; font-family: inherit; font-size: 0.85em; padding: 4px 10px; text-transform: uppercase; color: #333; }
        .btn:hover { background: #eee; border-color: #333; }
        .btn-search { padding: 12px 24px !important; font-size: 1em !important; }
        .recent { margin-top: 12px; display: flex; flex-wrap: wrap; gap: 6px; align-items: center; }
        .recent-label { font-size: 0.75em; color: #999; text-transform: uppercase; margin-right: 4px; }
        .tag-btn { background: #eee; border: 1px solid #ccc; padding: 3px 10px; font-size: 0.8em; cursor: pointer; text-transform: lowercase; font-family: inherit; }
        .tag-btn:hover { background: #ddd; border-color: #999; }
        .results { margin-top: 15px; }
        .results-hdr { display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; padding-bottom: 8px; border-bottom: 1px solid #ddd; }
        .result-item { display: flex; flex-direction: column; gap: 4px; padding: 12px 0; border-bottom: 1px solid #eee; font-size: 0.9em; }
        .result-title { font-weight: 600; margin-bottom: 2px; text-decoration: none; color: inherit; display: block; cursor: pointer; }
        .result-title:hover { color: #06A77D; }
        .result-cid { font-family: monospace; word-break: break-all; color: #555; font-size: 0.85em; }
        .result-kws { display: flex; flex-wrap: nowrap; gap: 4px; margin-top: 4px; overflow: hidden; }
        .kw-tag { background: #e8f5e9; border: 1px solid #a5d6a7; padding: 1px 6px; font-size: 0.8em; border-radius: 2px; cursor: pointer; white-space: nowrap; flex: 0 0 auto; }
        .kw-tag:hover { background: #c8e6c9; }
        .class-tags { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 2px; }
        .doc-arch { margin-top: 6px; font-size: 0.8em; }
        .doc-arch-toggle { cursor: pointer; color: #1565c0; user-select: none; display: inline-flex; align-items: center; gap: 4px; }
        .doc-arch-toggle:hover { text-decoration: underline; }
        .doc-arch-toggle .tri { display: inline-block; transition: transform 0.1s; }
        .doc-arch.open .doc-arch-toggle .tri { transform: rotate(90deg); }
        .doc-arch-list { display: none; flex-wrap: wrap; gap: 6px; margin-top: 6px; }
        .doc-arch.open .doc-arch-list { display: flex; }
        .arch-chip { display: inline-flex; align-items: center; gap: 4px; background: #f5f5f5; border: 1px solid #ccc; padding: 1px 6px; font-size: 0.95em; cursor: pointer; }
        .arch-chip:hover { background: #e8eef7; border-color: #1565c0; }
        .class-tag { padding: 1px 8px; font-size: 0.8em; border-radius: 2px; cursor: pointer; border: 1px solid; }
        .class-tag:hover { filter: brightness(0.92); }
        .field-tag { background: #e3f2fd; border-color: #90caf9; color: #1565c0; }
        .subtopic-tag { background: #f3e5f5; border-color: #ce93d8; color: #7b1fa2; }
        .pagination { display: flex; flex-wrap: wrap; justify-content: center; align-items: center; gap: 6px; margin-top: 12px; padding-top: 10px; border-top: 1px solid #eee; }
        .page-ellipsis { font-size: 0.85em; color: #999; padding: 0 2px; }
        .page-btn { background: #eee; border: 1px solid #ccc; padding: 4px 12px; font-size: 0.85em; cursor: pointer; font-family: inherit; }
        .page-btn:hover { background: #ddd; border-color: #999; }
        .page-btn.active { background: #333; color: white; border-color: #333; }
        .page-btn:disabled { opacity: 0.4; cursor: default; }
        .page-info { font-size: 0.8em; color: #666; }
        .upload-status { margin-top: 10px; font-size: 0.85em; }
        .upload-status.err { color: #D00000; }
        .indexing-badge { display: inline-block; background: #06A77D; color: white; padding: 2px 8px; font-size: 0.75em; text-transform: uppercase; margin-left: 8px; animation: pulse 1.5s infinite; }
        @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.6; } }
        .arch-grid { display: flex; flex-direction: column; gap: 10px; margin-top: 14px; }
        .arch-card { border: 1px solid #333; background: #fff; width: 100%; }
        .arch-head { display: flex; align-items: center; gap: 10px; padding: 10px 12px; cursor: pointer; }
        .arch-head:hover { background: #f7f7f7; }
        .arch-chevron { color: #888; font-size: 0.8em; transition: transform 0.15s; }
        .arch-card.open .arch-chevron { transform: rotate(90deg); }
        .arch-name { font-weight: 700; flex: 1; word-break: break-word; }
        .arch-head-meta { display: flex; align-items: center; flex-wrap: wrap; gap: 10px; font-size: 0.78em; color: #666; }
        .arch-body { display: none; flex-direction: column; gap: 8px; padding: 0 12px 12px 12px; }
        .arch-card.open .arch-body { display: flex; }
        .arch-meta { font-size: 0.78em; color: #666; display: flex; flex-wrap: wrap; gap: 10px; }
        .arch-cid { font-family: monospace; font-size: 0.75em; color: #888; word-break: break-all; }
        .arch-kws { display: flex; flex-wrap: wrap; gap: 4px; }
        .arch-actions { display: flex; gap: 6px; margin-top: 4px; }
        .status-badge { display: inline-block; padding: 1px 8px; font-size: 0.7em; text-transform: uppercase; border: 1px solid; border-radius: 2px; }
        .status-done { background: #e8f5e9; border-color: #a5d6a7; color: #2e7d32; }
        .status-indexing, .status-crawling { background: #fff8e1; border-color: #ffe082; color: #f57f17; }
        .status-queued { background: #eceff1; border-color: #b0bec5; color: #455a64; }
        .status-failed { background: #ffebee; border-color: #ef9a9a; color: #c62828; }
        .modal-overlay { position: fixed; inset: 0; background: rgba(0,0,0,0.5); display: none; align-items: flex-start; justify-content: center; z-index: 200; overflow-y: auto; padding: 30px 12px; }
        .modal-overlay.open { display: flex; }
        .modal-box { background: #fff; border: 1px solid #333; max-width: 900px; width: 100%; padding: 20px; }
        .modal-hdr { display: flex; justify-content: space-between; align-items: flex-start; border-bottom: 1px solid #ddd; padding-bottom: 10px; margin-bottom: 10px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>CID Indexer <span id="indexingBadge" class="indexing-badge" style="display:none;">Indexing...</span></h1>
        <div style="display:flex; align-items:center; gap:12px; flex-wrap:wrap; margin-bottom:10px;">
            <div id="statsToggle" class="disclosure">[ Stats ]</div>
            <div id="modelInfo" style="font-size:0.78em; color:#888;"></div>
        </div>
        <div id="statsRow" class="stats-row">
            <div class="stat-card"><div class="stat-value" id="stat-indexed" style="color:#06A77D;">--</div><div class="stat-label">Indexed</div></div>
            <div class="stat-card"><div class="stat-value" id="stat-pending" style="color:#D00000;">--</div><div class="stat-label">Pending</div></div>
            <div class="stat-card"><div class="stat-value" id="stat-keywords">--</div><div class="stat-label">Unique Keywords</div></div>
            <div class="stat-card"><div class="stat-value" id="stat-archives" style="color:#1565c0;">--</div><div class="stat-label">Archives</div></div>
        </div>
        <div class="section">
            <div style="display:flex; justify-content:space-between; align-items:center;">
                <h3 style="margin:0; text-transform:uppercase; font-size:1em;">Keyword Search</h3>
            </div>
            <div class="search-row">
                <div class="input-wrap">
                    <input type="text" id="searchInput" placeholder="Search documents by keyword..." autocomplete="off">
                    <div id="acPanel" class="ac-panel"></div>
                </div>
                <button class="btn btn-search" id="searchBtn">SEARCH</button>
            </div>
            <div id="recentDiv" class="recent"></div>
            <div id="resultsDiv" class="results" style="display:none;"></div>
        </div>
        <div class="section">
            <div style="display:flex; justify-content:space-between; align-items:center;">
                <h3 style="margin:0; text-transform:uppercase; font-size:1em;">Browse Archives</h3>
            </div>
            <div class="search-row">
                <div class="input-wrap">
                    <input type="text" id="archSearch" placeholder="Filter archives by topic, keyword, or owner..." autocomplete="off">
                </div>
                <button class="btn btn-search" id="archSearchBtn">FILTER</button>
            </div>
            <div id="archGrid" class="arch-grid"></div>
        </div>
        <div class="section">
            <h3 style="margin:0 0 6px 0; text-transform:uppercase; font-size:1em;">Add Document or Archive</h3>
            <div style="font-size:0.8em; color:#888; margin-bottom:10px;">Paste a single PDF CID to index one document, or a collection/directory CID to crawl and label the whole archive.</div>
            <div class="search-row" style="margin-top:0;">
                <div class="input-wrap" style="flex:2;">
                    <input type="text" id="cidInput" placeholder="Paste a CID (document or archive)..." autocomplete="off">
                </div>
                <div class="input-wrap" style="flex:1;">
                    <input type="text" id="ownerInput" placeholder="Owner (optional)" autocomplete="off">
                </div>
                <button class="btn btn-search" id="cidAddBtn">SUBMIT</button>
            </div>
            <div id="cidAddStatus" class="upload-status"></div>
        </div>
    </div>
    <div id="archModal" class="modal-overlay"><div class="modal-box" id="archModalBox"></div></div>
    <script>
        var GATEWAY = ` + "'" + gateway + "'" + `;
        var allResults = [];
        var currentQuery = '';
        var page = 0;
        var perPage = 20;
        var acIndex = -1;
        var acTimeout;

        var searchInput = document.getElementById('searchInput');
        var acPanel = document.getElementById('acPanel');

        function esc(t) { var d = document.createElement('div'); d.textContent = t; return d.innerHTML; }
        function escJs(t) { return t ? t.replace(/\\/g,'\\\\').replace(/'/g,"\\'").replace(/"/g,'\\"') : ''; }
        function toggleDocArch(el) { el.parentNode.classList.toggle('open'); }

        function doSearch(query) {
            if (!query || !query.trim()) return;
            query = query.trim();
            searchInput.value = query;
            hideAC();
            currentQuery = query;
            page = 0;
            var div = document.getElementById('resultsDiv');
            div.style.display = 'block';
            div.innerHTML = '<p style="color:#999;">Searching...</p>';
            fetch('/api/search?q=' + encodeURIComponent(query) + '&t=' + Date.now())
                .then(function(r) { return r.json(); })
                .then(function(data) {
                    allResults = data.results || [];
                    if (allResults.length === 0) {
                        div.innerHTML = '<p style="color:#666;">No documents found for "' + esc(query) + '".</p>';
                        return;
                    }
                    renderPage();
                    div.scrollIntoView({ behavior: 'smooth', block: 'start' });
                    loadRecent();
                })
                .catch(function() { div.innerHTML = '<p style="color:red;">Search failed.</p>'; });
        }

        function renderPage() {
            var total = allResults.length;
            var totalPages = Math.ceil(total / perPage);
            var start = page * perPage;
            var end = Math.min(start + perPage, total);
            var items = allResults.slice(start, end);

            var html = '<div class="results-hdr"><span>' + total + ' document' + (total !== 1 ? 's' : '') + ' matching "' + esc(currentQuery) + '"</span><button class="btn" onclick="document.getElementById(\'resultsDiv\').style.display=\'none\'">CLOSE</button></div>';
            html += items.map(function(r) {
                var cidDisplay = r.cid || '';
                var viewUrl = GATEWAY + '/ipfs/' + encodeURIComponent(cidDisplay);
                var title = r.title ? '<a class="result-title" href="' + viewUrl + '" target="_blank" rel="noopener">' + esc(r.title) + '</a>' : '';
                var bf = r.broad_field || '', st = r.sub_topic || '';
                var classHtml = '';
                if (bf || st) {
                    classHtml = '<div class="class-tags">';
                    if (bf) classHtml += '<span class="class-tag field-tag" onclick="doSearch(\'' + escJs(bf) + '\')">' + esc(bf) + '</span>';
                    if (st) classHtml += '<span class="class-tag subtopic-tag" onclick="doSearch(\'' + escJs(st) + '\')">' + esc(st) + '</span>';
                    classHtml += '</div>';
                }
                var kws = (r.keywords || []).map(function(k) { return '<span class="kw-tag" onclick="doSearch(\'' + escJs(k) + '\')">' + esc(k) + '</span>'; }).join('');
                var refs = r.archive_refs || [];
                var archHtml = '';
                if (refs.length) {
                    var chips = refs.map(function(a) {
                        var label = a.name ? a.name : a.cid;
                        var badge = a.status ? ' <span class="status-badge status-' + esc(a.status) + '">' + esc(a.status) + '</span>' : '';
                        return '<span class="arch-chip" title="' + esc(a.cid) + '" onclick="openArchive(\'' + escJs(a.cid) + '\')">' + esc(label) + badge + '</span>';
                    }).join('');
                    archHtml = '<div class="doc-arch">' +
                        '<span class="doc-arch-toggle" onclick="toggleDocArch(this)"><span class="tri">&#9656;</span> in ' + refs.length + ' archive' + (refs.length !== 1 ? 's' : '') + '</span>' +
                        '<div class="doc-arch-list">' + chips + '</div></div>';
                }
                return '<div class="result-item">' + title + classHtml +
                    '<div class="result-cid">' + esc(cidDisplay) + '</div>' +
                    '<div class="result-kws">' + kws + '</div>' + archHtml + '</div>';
            }).join('');

            if (totalPages > 1) {
                html += '<div class="pagination">';
                html += '<button class="page-btn" onclick="goPage(' + (page - 1) + ')"' + (page === 0 ? ' disabled' : '') + '>&laquo;</button>';
                pageWindow(page, totalPages).forEach(function(p) {
                    if (p === '...') {
                        html += '<span class="page-ellipsis">\u2026</span>';
                    } else {
                        html += '<button class="page-btn' + (p === page ? ' active' : '') + '" onclick="goPage(' + p + ')">' + (p + 1) + '</button>';
                    }
                });
                html += '<button class="page-btn" onclick="goPage(' + (page + 1) + ')"' + (page >= totalPages - 1 ? ' disabled' : '') + '>&raquo;</button>';
                html += '<span class="page-info">' + (start + 1) + '\u2013' + end + ' of ' + total + '</span>';
                html += '</div>';
            }

            document.getElementById('resultsDiv').innerHTML = html;
        }

        // Build a compact list of page indices to display, with '...' markers for
        // gaps. Always shows the first and last page plus a window around the
        // current page so the control never overflows horizontally.
        function pageWindow(current, totalPages) {
            var span = 2; // pages to show on each side of the current page
            var pages = [];
            var last = -1;
            for (var p = 0; p < totalPages; p++) {
                if (p === 0 || p === totalPages - 1 || (p >= current - span && p <= current + span)) {
                    if (last !== -1 && p - last > 1) pages.push('...');
                    pages.push(p);
                    last = p;
                }
            }
            return pages;
        }

        function goPage(p) {
            var totalPages = Math.ceil(allResults.length / perPage);
            if (p < 0 || p >= totalPages) return;
            page = p;
            renderPage();
            document.getElementById('resultsDiv').scrollIntoView({ behavior: 'smooth', block: 'start' });
        }

        function showAC(suggestions) {
            if (!suggestions || suggestions.length === 0) { hideAC(); return; }
            acIndex = -1;
            acPanel.innerHTML = suggestions.map(function(s, i) {
                return '<div class="ac-item" data-idx="' + i + '" onmousedown="doSearch(\'' + escJs(s.keyword) + '\')">' + esc(s.keyword) + '<span class="ac-count">' + s.cid_count + ' doc' + (s.cid_count !== 1 ? 's' : '') + '</span></div>';
            }).join('');
            acPanel.style.display = 'block';
        }

        function hideAC() { acPanel.style.display = 'none'; acIndex = -1; }

        function fetchSuggestions() {
            var q = searchInput.value.trim();
            if (q.length < 1) { hideAC(); return; }
            fetch('/api/suggest?q=' + encodeURIComponent(q) + '&t=' + Date.now())
                .then(function(r) { return r.json(); })
                .then(function(data) { showAC(data.suggestions || []); })
                .catch(function() { hideAC(); });
        }

        searchInput.addEventListener('input', function() {
            clearTimeout(acTimeout);
            acTimeout = setTimeout(fetchSuggestions, 200);
        });

        searchInput.addEventListener('keydown', function(e) {
            var items = acPanel.querySelectorAll('.ac-item');
            if (e.key === 'ArrowDown') {
                e.preventDefault();
                acIndex = Math.min(acIndex + 1, items.length - 1);
                items.forEach(function(el, i) { el.classList.toggle('active', i === acIndex); });
                if (acIndex >= 0) searchInput.value = items[acIndex].textContent.replace(/\d+ docs?$/, '').trim();
            } else if (e.key === 'ArrowUp') {
                e.preventDefault();
                acIndex = Math.max(acIndex - 1, -1);
                items.forEach(function(el, i) { el.classList.toggle('active', i === acIndex); });
                if (acIndex >= 0) searchInput.value = items[acIndex].textContent.replace(/\d+ docs?$/, '').trim();
            } else if (e.key === 'Enter') {
                hideAC();
                doSearch(searchInput.value);
            } else if (e.key === 'Escape') {
                hideAC();
            }
        });

        searchInput.addEventListener('blur', function() { setTimeout(hideAC, 150); });
        document.getElementById('searchBtn').addEventListener('click', function() { doSearch(searchInput.value); });

        function loadRecent() {
            fetch('/api/recent?t=' + Date.now())
                .then(function(r) { return r.json(); })
                .then(function(data) {
                    var el = document.getElementById('recentDiv');
                    var searches = data.searches || [];
                    if (searches.length === 0) { el.innerHTML = ''; return; }
                    el.innerHTML = '<span class="recent-label">Recent:</span>' + searches.slice(0, 8).map(function(s) {
                        return '<button class="tag-btn" onclick="doSearch(\'' + escJs(s.keyword) + '\')">' + esc(s.keyword) + ' (' + s.result_count + ')</button>';
                    }).join('');
                })
                .catch(function() {});
        }

        function loadStats() {
            fetch('/api/stats?t=' + Date.now())
                .then(function(r) { return r.json(); })
                .then(function(data) {
                    document.getElementById('stat-indexed').textContent = data.indexed || 0;
                    document.getElementById('stat-pending').textContent = data.pending || 0;
                    document.getElementById('stat-keywords').textContent = data.unique_keywords || 0;
                    document.getElementById('stat-archives').textContent = data.archives || 0;
                    var badge = document.getElementById('indexingBadge');
                    var queued = data.queued || 0;
                    badge.style.display = (data.indexing || queued > 0) ? '' : 'none';
                    badge.textContent = queued > 0 ? ('Indexing... (' + queued + ' queued)') : 'Indexing...';
                    var mi = document.getElementById('modelInfo');
                    if (data.model) {
                        mi.textContent = 'Model: ' + data.model + '  \u00b7  API: ' + data.api_base;
                    }
                })
                .catch(function() {});
        }

        // --- Stats disclosure (hidden by default) ---
        var statsToggle = document.getElementById('statsToggle');
        var statsRow = document.getElementById('statsRow');
        statsToggle.addEventListener('click', function() {
            var open = statsRow.classList.toggle('open');
            statsToggle.classList.toggle('open', open);
        });

        // --- Archives browse ---
        var archGrid = document.getElementById('archGrid');
        var archSearch = document.getElementById('archSearch');
        var expandedArchives = {};

        function statusBadge(s) {
            var cls = 'status-badge status-' + (s || 'crawling');
            return '<span class="' + cls + '">' + esc(s || '') + '</span>';
        }

        function archTitle(a) {
            return (a.name && a.name !== a.cid) ? a.name : 'Untitled archive';
        }

        function renderArchives(archives) {
            if (!archives || archives.length === 0) {
                archGrid.innerHTML = '<p style="color:#999; font-size:0.9em;">No archives yet. Paste a collection CID below to index one.</p>';
                return;
            }
            archGrid.innerHTML = archives.map(function(a) {
                var name = archTitle(a);
                var fields = (a.broad_fields || []).slice(0, 3).map(function(f) {
                    return '<span class="class-tag field-tag" onclick="filterArch(\'' + escJs(f.field) + '\')">' + esc(f.field) + ' (' + f.count + ')</span>';
                }).join('');
                var kws = (a.top_keywords || []).slice(0, 6).map(function(k) {
                    return '<span class="kw-tag" onclick="filterArch(\'' + escJs(k) + '\')">' + esc(k) + '</span>';
                }).join('');
                var meta = '<span>' + (a.indexed || 0) + '/' + (a.doc_count || 0) + ' docs</span>';
                if (a.owner) meta += '<span>by ' + esc(a.owner) + '</span>';
                meta += statusBadge(a.status);
                var openCls = expandedArchives[a.cid] ? ' open' : '';
                return '<div class="arch-card' + openCls + '">' +
                    '<div class="arch-head" onclick="toggleArch(this, \'' + escJs(a.cid) + '\')">' +
                        '<span class="arch-chevron">\u25B6</span>' +
                        '<span class="arch-name">' + esc(name) + '</span>' +
                        '<span class="arch-head-meta">' + meta + '</span>' +
                    '</div>' +
                    '<div class="arch-body">' +
                        (fields ? '<div class="class-tags">' + fields + '</div>' : '') +
                        (kws ? '<div class="arch-kws">' + kws + '</div>' : '') +
                        '<div class="arch-cid">' + esc(a.cid) + '</div>' +
                        '<div class="arch-actions">' +
                            '<button class="btn" onclick="openArchive(\'' + escJs(a.cid) + '\')">DETAILS</button>' +
                            '<button class="btn" onclick="replicate(\'' + escJs(a.cid) + '\')">REPLICATE</button>' +
                        '</div>' +
                    '</div>' +
                '</div>';
            }).join('');
        }

        function toggleArch(head, cid) {
            var card = head.parentNode;
            if (card.classList.toggle('open')) { expandedArchives[cid] = true; }
            else { delete expandedArchives[cid]; }
        }

        function loadArchives() {
            var q = archSearch.value.trim();
            var url = q ? '/api/archives/search?q=' + encodeURIComponent(q) : '/api/archives';
            fetch(url + (url.indexOf('?') > -1 ? '&' : '?') + 't=' + Date.now())
                .then(function(r) { return r.json(); })
                .then(function(data) { renderArchives(data.archives || []); })
                .catch(function() {});
        }

        function filterArch(term) {
            archSearch.value = term;
            loadArchives();
            archSearch.scrollIntoView({ behavior: 'smooth', block: 'center' });
        }

        function replicate(cid) {
            var cmd = 'ipfs pin add ' + cid;
            if (navigator.clipboard && navigator.clipboard.writeText) {
                navigator.clipboard.writeText(cid).catch(function() {});
            }
            alert('Archive CID copied.\n\nTo replicate, pin it with your own IPFS node:\n\n' + cmd);
        }

        document.getElementById('archSearchBtn').addEventListener('click', loadArchives);
        archSearch.addEventListener('keydown', function(e) { if (e.key === 'Enter') loadArchives(); });

        // --- Archive detail modal ---
        var archModal = document.getElementById('archModal');
        var archModalBox = document.getElementById('archModalBox');

        function closeArchive() { archModal.classList.remove('open'); }
        archModal.addEventListener('click', function(e) { if (e.target === archModal) closeArchive(); });

        function openArchive(cid) {
            archModalBox.innerHTML = '<p style="color:#999;">Loading...</p>';
            archModal.classList.add('open');
            fetch('/api/archive?cid=' + encodeURIComponent(cid) + '&t=' + Date.now())
                .then(function(r) { return r.json(); })
                .then(function(data) {
                    if (data.error) { archModalBox.innerHTML = '<p style="color:red;">' + esc(data.error) + '</p>'; return; }
                    var a = data.archive || {};
                    var docs = data.documents || [];
                    var viewUrl = GATEWAY + '/ipfs/' + encodeURIComponent(a.cid);
                    var html = '<div class="modal-hdr"><div><div style="font-weight:700; font-size:1.1em;">' + esc(archTitle(a)) + '</div>' +
                        '<div class="arch-meta" style="margin-top:6px;">' + (a.indexed || 0) + '/' + (a.doc_count || 0) + ' docs' +
                        (a.owner ? '<span>by ' + esc(a.owner) + '</span>' : '') + statusBadge(a.status) + '</div></div>' +
                        '<button class="btn" onclick="closeArchive()">CLOSE</button></div>';
                    html += '<div class="arch-cid" style="margin-bottom:8px;"><a href="' + viewUrl + '" target="_blank" rel="noopener">' + esc(a.cid) + '</a></div>';
                    if (a.error) html += '<div style="color:#c62828; font-size:0.85em; margin-bottom:8px;">' + esc(a.error) + '</div>';
                    var kws = (a.top_keywords || []).map(function(k) { return '<span class="kw-tag" onclick="filterArch(\'' + escJs(k) + '\')">' + esc(k) + '</span>'; }).join('');
                    if (kws) html += '<div class="arch-kws" style="margin-bottom:12px;">' + kws + '</div>';
                    html += '<div style="margin-bottom:6px; text-transform:uppercase; font-size:0.85em; color:#555;">Documents</div>';
                    html += docs.map(function(r) {
                        var durl = GATEWAY + '/ipfs/' + encodeURIComponent(r.cid || '');
                        var title = r.title ? '<a class="result-title" href="' + durl + '" target="_blank" rel="noopener">' + esc(r.title) + '</a>' : esc(r.cid);
                        var dkws = (r.keywords || []).map(function(k) { return '<span class="kw-tag">' + esc(k) + '</span>'; }).join('');
                        return '<div class="result-item">' + title + '<div class="result-cid">' + esc(r.cid || '') + '</div><div class="result-kws">' + dkws + '</div></div>';
                    }).join('');
                    if (docs.length === 0) html += '<p style="color:#999; font-size:0.9em;">No indexed documents yet.</p>';
                    archModalBox.innerHTML = html;
                })
                .catch(function() { archModalBox.innerHTML = '<p style="color:red;">Failed to load archive.</p>'; });
        }

        // --- Submit single CID (auto-classified) ---
        var cidInput = document.getElementById('cidInput');
        var ownerInput = document.getElementById('ownerInput');
        var cidAddBtn = document.getElementById('cidAddBtn');
        var cidAddStatus = document.getElementById('cidAddStatus');

        function submitCID() {
            var cid = cidInput.value.trim();
            if (!cid) return;
            cidAddStatus.className = 'upload-status';
            cidAddStatus.textContent = 'Submitting...';
            var fd = new FormData();
            fd.append('cid', cid);
            fd.append('owner', ownerInput.value.trim());
            fetch('/api/submit', { method: 'POST', body: fd })
            .then(function(r) { return r.json().then(function(d) { return { ok: r.ok, data: d }; }); })
            .then(function(res) {
                if (!res.ok) {
                    cidAddStatus.className = 'upload-status err';
                    cidAddStatus.textContent = res.data.error || 'Failed';
                    return;
                }
                cidAddStatus.className = 'upload-status';
                cidAddStatus.textContent = res.data.message || 'Submitted';
                cidInput.value = '';
                loadStats();
                loadArchives();
            })
            .catch(function(e) {
                cidAddStatus.className = 'upload-status err';
                cidAddStatus.textContent = 'Failed: ' + e.message;
            });
        }

        cidAddBtn.addEventListener('click', submitCID);
        cidInput.addEventListener('keydown', function(e) {
            if (e.key === 'Enter') submitCID();
        });

        loadRecent();
        loadStats();
        loadArchives();
        setInterval(function() { loadStats(); loadArchives(); }, 5000);
    </script>
</body>
</html>`
}

func adminHTML(gateway string) string {
	return `<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>CID Indexer - Admin</title>
    <style>
        * { box-sizing: border-box; }
        body { font-family: "Courier New", Courier, monospace; margin: 0; background: #f8f9fa; color: #333; }
        .container { max-width: 860px; margin: 0 auto; padding: 20px; }
        h1 { color: #222; text-transform: uppercase; border-bottom: 2px solid #333; padding-bottom: 10px; font-size: 1.4em; }
        a { color: #1565c0; }
        .section { background: white; padding: 20px; border: 1px solid #333; margin: 20px 0; }
        .section h3 { margin: 0 0 12px; text-transform: uppercase; font-size: 1em; }
        .btn { background: none; border: 1px solid #999; cursor: pointer; font-family: inherit; font-size: 0.85em; padding: 5px 12px; text-transform: uppercase; color: #333; }
        .btn:hover { background: #eee; border-color: #333; }
        .btn:disabled { opacity: 0.4; cursor: default; }
        .btn-allow { border-color: #2e7d32; color: #2e7d32; }
        .btn-allow:hover { background: #e8f5e9; }
        .btn-deny { border-color: #c62828; color: #c62828; }
        .btn-deny:hover { background: #ffebee; }
        input[type=text], input[type=password] { padding: 10px 12px; border: 1px solid #333; font-family: inherit; font-size: 0.95em; width: 100%; }
        .row { display: flex; gap: 10px; align-items: center; }
        .sub-row { display: flex; align-items: center; gap: 10px; padding: 10px 0; border-bottom: 1px solid #eee; font-size: 0.88em; }
        .sub-main { flex: 1; min-width: 0; }
        .sub-cid { font-family: monospace; word-break: break-all; }
        .kind-tag { display: inline-block; padding: 1px 8px; font-size: 0.72em; text-transform: uppercase; border: 1px solid; border-radius: 2px; margin-top: 3px; }
        .kind-archive { background: #e3f2fd; border-color: #90caf9; color: #1565c0; }
        .kind-document { background: #f3e5f5; border-color: #ce93d8; color: #7b1fa2; }
        .status { font-size: 0.85em; margin-top: 8px; min-height: 1.2em; }
        .status.err { color: #c62828; }
        .status.ok { color: #2e7d32; }
        .toggle-state { font-weight: 700; }
        .muted { color: #888; font-size: 0.8em; }
        .toolbar { display: flex; justify-content: space-between; align-items: center; }
        .fail-row { display: flex; align-items: flex-start; gap: 10px; padding: 8px 0; border-bottom: 1px solid #eee; font-size: 0.85em; }
        .fail-main { flex: 1; min-width: 0; }
        .fail-cid { font-family: monospace; word-break: break-all; color: #555; }
        .fail-reason { color: #c62828; font-size: 0.9em; margin-top: 2px; word-break: break-word; }
        .btn-retry { flex: 0 0 auto; }
    </style>
</head>
<body>
    <div class="container">
        <h1>CID Indexer - Admin</h1>
        <div class="muted" style="margin-bottom:10px;"><a href="/">&larr; Back to search</a></div>

        <div class="section" id="loginSection" style="display:none;">
            <h3>Admin Login</h3>
            <div class="muted" style="margin-bottom:10px;">Log in with the server's API key. Sent over the local connection; use only on a trusted network.</div>
            <div class="row">
                <input type="password" id="keyInput" placeholder="API key" autocomplete="off">
                <button class="btn" id="loginBtn">LOGIN</button>
            </div>
            <div id="loginStatus" class="status"></div>
        </div>

        <div id="adminView" style="display:none;">
            <div class="section">
                <div class="toolbar">
                    <h3 style="margin:0;">Review Mode</h3>
                    <button class="btn" id="logoutBtn">LOGOUT</button>
                </div>
                <div style="margin-top:10px;" class="row">
                    <span>Status: <span class="toggle-state" id="reviewState">--</span></span>
                    <button class="btn" id="reviewToggle">TOGGLE</button>
                </div>
                <div class="muted" style="margin-top:8px;">When ON, user submissions are held below for review and are not indexed until approved.</div>
            </div>

            <div class="section">
                <div class="toolbar">
                    <h3 style="margin:0;">Pending Review <span id="subCount" style="color:#c62828;"></span></h3>
                    <button class="btn" id="refreshBtn">REFRESH</button>
                </div>
                <div id="subList" style="margin-top:10px;"></div>
                <div id="subStatus" class="status"></div>
            </div>

            <div class="section">
                <h3>Remove Content</h3>
                <div class="muted" style="margin-bottom:10px;">Remove an archive (orphaned member documents are deleted; shared documents are kept) or an individual document from the index.</div>
                <div class="row" style="margin-bottom:8px;">
                    <input type="text" id="delArchiveCid" placeholder="Archive CID to remove" autocomplete="off">
                    <button class="btn btn-deny" id="delArchiveBtn">REMOVE</button>
                </div>
                <div class="row">
                    <input type="text" id="delDocCid" placeholder="Document CID to remove" autocomplete="off">
                    <button class="btn btn-deny" id="delDocBtn">REMOVE</button>
                </div>
                <div id="delStatus" class="status"></div>
            </div>

            <div class="section">
                <div class="toolbar">
                    <h3 style="margin:0;">Failed Documents <span id="failCount" style="color:#c62828;"></span></h3>
                    <button class="btn" id="retryAllBtn">RETRY ALL</button>
                </div>
                <div class="muted" style="margin:8px 0;">These CIDs failed indexing after repeated attempts. Retry to re-queue them (e.g. after raising -convert-timeout).</div>
                <div id="failList"><div class="muted">No failed documents.</div></div>
            </div>
        </div>
    </div>
    <script>
        var GATEWAY = ` + "'" + gateway + "'" + `;
        var reviewEnabled = false;

        function esc(t) { var d = document.createElement('div'); d.textContent = t; return d.innerHTML; }
        function escJs(t) { return t ? t.replace(/\\/g,'\\\\').replace(/'/g,"\\'").replace(/"/g,'\\"') : ''; }

        var loginSection = document.getElementById('loginSection');
        var adminView = document.getElementById('adminView');

        function checkSession() {
            fetch('/api/admin/session?t=' + Date.now())
                .then(function(r) { return r.json(); })
                .then(function(d) {
                    reviewEnabled = !!d.review_enabled;
                    if (d.authed) {
                        loginSection.style.display = 'none';
                        adminView.style.display = '';
                        renderReviewState();
                        loadSubmissions();
                        loadFailures();
                    } else {
                        loginSection.style.display = '';
                        adminView.style.display = 'none';
                    }
                })
                .catch(function() {});
        }

        function renderReviewState() {
            var el = document.getElementById('reviewState');
            el.textContent = reviewEnabled ? 'ON (review required)' : 'OFF (public, auto-index)';
            el.style.color = reviewEnabled ? '#c62828' : '#2e7d32';
        }

        function login() {
            var key = document.getElementById('keyInput').value.trim();
            var st = document.getElementById('loginStatus');
            st.className = 'status'; st.textContent = 'Logging in...';
            var fd = new FormData(); fd.append('key', key);
            fetch('/api/admin/login', { method: 'POST', body: fd })
                .then(function(r) { return r.json().then(function(d) { return { ok: r.ok, data: d }; }); })
                .then(function(res) {
                    if (!res.ok) { st.className = 'status err'; st.textContent = res.data.error || 'Login failed'; return; }
                    document.getElementById('keyInput').value = '';
                    st.textContent = '';
                    checkSession();
                })
                .catch(function() { st.className = 'status err'; st.textContent = 'Login failed'; });
        }

        function logout() {
            fetch('/api/admin/logout', { method: 'POST' }).then(function() { checkSession(); });
        }

        function toggleReview() {
            var fd = new FormData(); fd.append('enabled', reviewEnabled ? 'false' : 'true');
            fetch('/api/admin/review-mode', { method: 'POST', body: fd })
                .then(function(r) { return r.json(); })
                .then(function(d) { reviewEnabled = !!d.review_enabled; renderReviewState(); })
                .catch(function() {});
        }

        function loadSubmissions() {
            fetch('/api/admin/submissions?t=' + Date.now())
                .then(function(r) { if (r.status === 401) { checkSession(); return null; } return r.json(); })
                .then(function(d) {
                    if (!d) return;
                    var subs = d.submissions || [];
                    document.getElementById('subCount').textContent = subs.length ? '(' + subs.length + ')' : '';
                    var list = document.getElementById('subList');
                    if (subs.length === 0) {
                        list.innerHTML = '<div class="muted">No submissions awaiting review.</div>';
                        return;
                    }
                    list.innerHTML = subs.map(function(s) {
                        var url = GATEWAY + '/ipfs/' + encodeURIComponent(s.cid);
                        var kindCls = s.kind === 'archive' ? 'kind-archive' : 'kind-document';
                        return '<div class="sub-row"><div class="sub-main">' +
                            '<div class="sub-cid"><a href="' + url + '" target="_blank" rel="noopener">' + esc(s.cid) + '</a></div>' +
                            '<span class="kind-tag ' + kindCls + '">' + esc(s.kind) + '</span>' +
                            (s.owner ? ' <span class="muted">by ' + esc(s.owner) + '</span>' : '') +
                            '</div>' +
                            '<button class="btn btn-deny" onclick="decide(\'deny\', \'' + escJs(s.cid) + '\', this)">DENY</button>' +
                            '<button class="btn btn-allow" onclick="decide(\'approve\', \'' + escJs(s.cid) + '\', this)">ALLOW</button>' +
                            '</div>';
                    }).join('');
                })
                .catch(function() {});
        }

        function decide(action, cid, btn) {
            var st = document.getElementById('subStatus');
            var row = btn.parentNode;
            row.querySelectorAll('button').forEach(function(b) { b.disabled = true; });
            var fd = new FormData(); fd.append('cid', cid);
            fetch('/api/admin/' + action, { method: 'POST', body: fd })
                .then(function(r) { return r.json().then(function(d) { return { ok: r.ok, data: d }; }); })
                .then(function(res) {
                    st.className = res.ok ? 'status ok' : 'status err';
                    st.textContent = (res.data && (res.data.message || res.data.error)) || '';
                    loadSubmissions();
                })
                .catch(function() { st.className = 'status err'; st.textContent = 'Action failed'; loadSubmissions(); });
        }

        function removeContent(kind, inputId) {
            var input = document.getElementById(inputId);
            var cid = input.value.trim();
            var st = document.getElementById('delStatus');
            if (!cid) return;
            var fd = new FormData(); fd.append('cid', cid);
            fetch('/api/admin/delete-' + kind, { method: 'POST', body: fd })
                .then(function(r) { return r.json().then(function(d) { return { ok: r.ok, data: d }; }); })
                .then(function(res) {
                    st.className = res.ok ? 'status ok' : 'status err';
                    st.textContent = (res.data && (res.data.message || res.data.error)) || '';
                    if (res.ok) input.value = '';
                })
                .catch(function() { st.className = 'status err'; st.textContent = 'Remove failed'; });
        }

        // --- Failed documents ---
        var currentFailures = [];

        function loadFailures() {
            fetch('/api/failures?t=' + Date.now())
                .then(function(r) { return r.json(); })
                .then(function(data) {
                    currentFailures = data.failures || [];
                    document.getElementById('failCount').textContent = currentFailures.length ? '(' + currentFailures.length + ')' : '';
                    var list = document.getElementById('failList');
                    if (currentFailures.length === 0) {
                        list.innerHTML = '<div class="muted">No failed documents.</div>';
                        return;
                    }
                    list.innerHTML = currentFailures.map(function(f) {
                        var url = GATEWAY + '/ipfs/' + encodeURIComponent(f.cid);
                        return '<div class="fail-row"><div class="fail-main">' +
                            '<div class="fail-cid"><a href="' + url + '" target="_blank" rel="noopener">' + esc(f.cid) + '</a></div>' +
                            (f.reason ? '<div class="fail-reason">' + esc(f.reason) + '</div>' : '') +
                            '</div><button class="btn btn-retry" onclick="retryDoc(\'' + escJs(f.cid) + '\', this)">RETRY</button></div>';
                    }).join('');
                })
                .catch(function() {});
        }

        function retryDoc(cid, btn) {
            if (btn) { btn.disabled = true; btn.textContent = '...'; }
            var fd = new FormData();
            fd.append('cid', cid);
            fetch('/api/retry', { method: 'POST', body: fd })
                .then(function(r) { return r.json(); })
                .then(function() { loadFailures(); })
                .catch(function() { if (btn) { btn.disabled = false; btn.textContent = 'RETRY'; } });
        }

        document.getElementById('retryAllBtn').addEventListener('click', function() {
            var btn = this;
            btn.disabled = true;
            var cids = currentFailures.map(function(f) { return f.cid; });
            Promise.all(cids.map(function(c) {
                var fd = new FormData(); fd.append('cid', c);
                return fetch('/api/retry', { method: 'POST', body: fd }).catch(function() {});
            })).then(function() { btn.disabled = false; loadFailures(); });
        });

        document.getElementById('loginBtn').addEventListener('click', login);
        document.getElementById('keyInput').addEventListener('keydown', function(e) { if (e.key === 'Enter') login(); });
        document.getElementById('logoutBtn').addEventListener('click', logout);
        document.getElementById('reviewToggle').addEventListener('click', toggleReview);
        document.getElementById('refreshBtn').addEventListener('click', loadSubmissions);
        document.getElementById('delArchiveBtn').addEventListener('click', function() { removeContent('archive', 'delArchiveCid'); });
        document.getElementById('delDocBtn').addEventListener('click', function() { removeContent('document', 'delDocCid'); });

        checkSession();
        setInterval(function() { if (adminView.style.display !== 'none') { loadSubmissions(); loadFailures(); } }, 5000);
    </script>
</body>
</html>`
}
