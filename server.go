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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const defaultPort = 8384

// Pagination bounds for /api/search.
const (
	defaultSearchLimit = 20
	maxSearchLimit     = 100
)

func parseIntDefault(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}

func clampInt(n, lo, hi int) int {
	if n < lo {
		return lo
	}
	if n > hi {
		return hi
	}
	return n
}

const (
	sessionCookie = "admin_session"
	sessionTTL    = 12 * time.Hour
)

var indexingActive atomic.Bool

// sessionStore holds active admin sessions in memory.
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

type searchResult struct {
	IndexEntry
	ArchiveRefs []ArchiveRef `json:"archive_refs,omitempty"`
}

func startServer(store *Store, port int, cfg PipelineConfig, ix *Indexer) error {
	mux := http.NewServeMux()
	sessions := newSessionStore()

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
		w.Write([]byte(renderPage("dashboard", cfg.Gateway)))
	})

	mux.HandleFunc("/admin", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(renderPage("admin", cfg.Gateway)))
	})

	mux.HandleFunc("/api/search", func(w http.ResponseWriter, r *http.Request) {
		query := strings.TrimSpace(r.URL.Query().Get("q"))
		if query == "" {
			writeJSON(w, map[string]interface{}{"results": []struct{}{}, "count": 0, "offset": 0, "limit": 0})
			return
		}
		offset := clampInt(parseIntDefault(r.URL.Query().Get("offset"), 0), 0, 1<<30)
		limit := clampInt(parseIntDefault(r.URL.Query().Get("limit"), defaultSearchLimit), 1, maxSearchLimit)

		results, total := store.SearchPage(query, offset, limit)
		if offset == 0 {
			store.RecordSearch(query, total)
		}
		enriched := make([]searchResult, 0, len(results))
		for _, e := range results {
			enriched = append(enriched, searchResult{
				IndexEntry:  e,
				ArchiveRefs: store.ArchiveRefs(e.Archives),
			})
		}
		writeJSON(w, map[string]interface{}{
			"query":   query,
			"results": enriched,
			"count":   total,
			"offset":  offset,
			"limit":   limit,
		})
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
		WriteTimeout: 60 * time.Second,
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
