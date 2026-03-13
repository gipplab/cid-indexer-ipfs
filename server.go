package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

const defaultPort = 8384

var indexingActive atomic.Bool

func startServer(store *Store, port int, cfg PipelineConfig) error {
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(dashboardHTML(cfg.Gateway)))
	})

	mux.HandleFunc("/api/search", func(w http.ResponseWriter, r *http.Request) {
		query := strings.TrimSpace(r.URL.Query().Get("q"))
		if query == "" {
			writeJSON(w, map[string]interface{}{"results": []struct{}{}, "count": 0})
			return
		}
		results := store.Search(query)
		if results == nil {
			results = []IndexEntry{}
		}
		store.RecordSearch(query, len(results))
		writeJSON(w, map[string]interface{}{"query": query, "results": results, "count": len(results)})
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
		stats.Model = cfg.Model
		stats.APIBase = cfg.APIBase
		writeJSON(w, stats)
	})

	mux.HandleFunc("/api/cids.txt", func(w http.ResponseWriter, r *http.Request) {
		cids := store.AllCIDs()
		sort.Strings(cids)
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Content-Disposition", "attachment; filename=\"cids.txt\"")
		for _, c := range cids {
			fmt.Fprintln(w, c)
		}
	})

	mux.HandleFunc("/api/upload", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var cidText string

		contentType := r.Header.Get("Content-Type")
		if strings.HasPrefix(contentType, "multipart/form-data") {
			r.ParseMultipartForm(10 << 20) // 10 MB
			file, _, err := r.FormFile("file")
			if err != nil {
				writeJSONError(w, "missing file field: "+err.Error(), http.StatusBadRequest)
				return
			}
			defer file.Close()
			data, err := io.ReadAll(io.LimitReader(file, 10<<20))
			if err != nil {
				writeJSONError(w, "read error: "+err.Error(), http.StatusBadRequest)
				return
			}
			cidText = string(data)
		} else {
			data, err := io.ReadAll(io.LimitReader(r.Body, 10<<20))
			if err != nil {
				writeJSONError(w, "read error: "+err.Error(), http.StatusBadRequest)
				return
			}
			cidText = string(data)
		}

		cids := parseCIDList(cidText)
		if len(cids) == 0 {
			writeJSONError(w, "no CIDs found in upload", http.StatusBadRequest)
			return
		}

		newCount := store.AppendCIDs(cids)
		total := len(store.AllCIDs())
		slog.Info("CID upload received", "uploaded", len(cids), "new", newCount, "total", total)

		if indexingActive.Load() {
			writeJSON(w, map[string]interface{}{
				"message": fmt.Sprintf("added %d new CIDs, indexing in progress", newCount),
				"new":     newCount,
				"total":   total,
			})
			return
		}

		apiKey := loadAPIKey(cfg.DataDir)
		if apiKey == "" {
			writeJSON(w, map[string]interface{}{
				"message": fmt.Sprintf("added %d new CIDs, no API key for indexing", newCount),
				"new":     newCount,
				"total":   total,
			})
			return
		}

		allCIDs := store.AllCIDs()
		pending := store.Pending(allCIDs)
		if len(pending) == 0 {
			writeJSON(w, map[string]interface{}{
				"message": "all CIDs already indexed",
				"total":   total,
				"pending": 0,
			})
			return
		}

		indexingActive.Store(true)
		go func() {
			defer indexingActive.Store(false)
			indexPending(store, pending, apiKey, cfg)
		}()

		writeJSON(w, map[string]interface{}{
			"message": "indexing started",
			"new":     newCount,
			"total":   total,
			"pending": len(pending),
		})
	})

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 20 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	slog.Info("web UI started", "url", fmt.Sprintf("http://localhost:%d", port))
	return srv.ListenAndServe()
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
    <title>CID Indexer — Keyword Search</title>
    <style>
        * { box-sizing: border-box; }
        body { font-family: "Courier New", Courier, monospace; margin: 0; background: #f8f9fa; color: #333; }
        .container { max-width: 960px; margin: 0 auto; padding: 20px; }
        h1 { color: #222; text-transform: uppercase; border-bottom: 2px solid #333; padding-bottom: 10px; font-size: 1.4em; }
        .stats-row { display: flex; gap: 20px; margin: 15px 0; flex-wrap: wrap; }
        .stat-card { background: white; padding: 15px 20px; border: 1px solid #333; text-align: center; min-width: 140px; }
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
        .result-kws { display: flex; flex-wrap: wrap; gap: 4px; margin-top: 4px; }
        .kw-tag { background: #e8f5e9; border: 1px solid #a5d6a7; padding: 1px 6px; font-size: 0.8em; border-radius: 2px; cursor: pointer; }
        .kw-tag:hover { background: #c8e6c9; }
        .class-tags { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 2px; }
        .class-tag { padding: 1px 8px; font-size: 0.8em; border-radius: 2px; cursor: pointer; border: 1px solid; }
        .class-tag:hover { filter: brightness(0.92); }
        .field-tag { background: #e3f2fd; border-color: #90caf9; color: #1565c0; }
        .subtopic-tag { background: #f3e5f5; border-color: #ce93d8; color: #7b1fa2; }
        .niche-tag { background: #fff3e0; border-color: #ffcc80; color: #e65100; }
        .pagination { display: flex; justify-content: center; align-items: center; gap: 6px; margin-top: 12px; padding-top: 10px; border-top: 1px solid #eee; }
        .page-btn { background: #eee; border: 1px solid #ccc; padding: 4px 12px; font-size: 0.85em; cursor: pointer; font-family: inherit; }
        .page-btn:hover { background: #ddd; border-color: #999; }
        .page-btn.active { background: #333; color: white; border-color: #333; }
        .page-btn:disabled { opacity: 0.4; cursor: default; }
        .page-info { font-size: 0.8em; color: #666; }
        .upload-zone { border: 2px dashed #bbb; padding: 20px; text-align: center; cursor: pointer; color: #888; font-size: 0.9em; transition: border-color 0.2s, color 0.2s; }
        .upload-zone:hover, .upload-zone.drag-over { border-color: #06A77D; color: #06A77D; }
        .upload-zone input[type="file"] { display: none; }
        .upload-status { margin-top: 10px; font-size: 0.85em; }
        .upload-status.err { color: #D00000; }
        .indexing-badge { display: inline-block; background: #06A77D; color: white; padding: 2px 8px; font-size: 0.75em; text-transform: uppercase; margin-left: 8px; animation: pulse 1.5s infinite; }
        @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.6; } }
    </style>
</head>
<body>
    <div class="container">
        <h1>CID Indexer <span id="indexingBadge" class="indexing-badge" style="display:none;">Indexing...</span></h1>
        <div id="modelInfo" style="font-size:0.78em; color:#888; margin-bottom:10px;"></div>
        <div class="stats-row">
            <div class="stat-card"><div class="stat-value" id="stat-total">--</div><div class="stat-label">Total CIDs</div><a id="cidDownload" href="/api/cids.txt" class="btn" style="margin-top:6px;display:none;font-size:0.7em;">Download list</a></div>
            <div class="stat-card"><div class="stat-value" id="stat-indexed" style="color:#06A77D;">--</div><div class="stat-label">Indexed</div></div>
            <div class="stat-card"><div class="stat-value" id="stat-pending" style="color:#D00000;">--</div><div class="stat-label">Pending</div></div>
            <div class="stat-card"><div class="stat-value" id="stat-keywords">--</div><div class="stat-label">Unique Keywords</div></div>
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
            <h3 style="margin:0 0 12px 0; text-transform:uppercase; font-size:1em;">Add CIDs</h3>
            <div class="search-row" style="margin-top:0;">
                <div class="input-wrap">
                    <input type="text" id="cidInput" placeholder="Paste a CID..." autocomplete="off">
                </div>
                <button class="btn btn-search" id="cidAddBtn">ADD</button>
            </div>
            <div id="cidAddStatus" class="upload-status"></div>
            <div style="margin-top:14px;">
                <div id="uploadZone" class="upload-zone">
                    <div>Drop a CID list file here, or click to browse</div>
                    <div style="font-size:0.8em; margin-top:4px; color:#aaa;">One CID per line, plain text</div>
                    <input type="file" id="uploadFile" accept=".txt,.csv,text/plain">
                </div>
                <div id="uploadStatus" class="upload-status"></div>
            </div>
        </div>
    </div>
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
                var bf = r.broad_field || '', st = r.sub_topic || '', rn = r.research_niche || '';
                var classHtml = '';
                if (bf || st || rn) {
                    classHtml = '<div class="class-tags">';
                    if (bf) classHtml += '<span class="class-tag field-tag" onclick="doSearch(\'' + escJs(bf) + '\')">' + esc(bf) + '</span>';
                    if (st) classHtml += '<span class="class-tag subtopic-tag" onclick="doSearch(\'' + escJs(st) + '\')">' + esc(st) + '</span>';
                    if (rn) classHtml += '<span class="class-tag niche-tag" onclick="doSearch(\'' + escJs(rn) + '\')">' + esc(rn) + '</span>';
                    classHtml += '</div>';
                }
                var kws = (r.keywords || []).map(function(k) { return '<span class="kw-tag" onclick="doSearch(\'' + escJs(k) + '\')">' + esc(k) + '</span>'; }).join('');
                return '<div class="result-item">' + title + classHtml +
                    '<div class="result-cid">' + esc(cidDisplay) + '</div>' +
                    '<div class="result-kws">' + kws + '</div></div>';
            }).join('');

            if (totalPages > 1) {
                html += '<div class="pagination">';
                html += '<button class="page-btn" onclick="goPage(' + (page - 1) + ')"' + (page === 0 ? ' disabled' : '') + '>&laquo;</button>';
                for (var p = 0; p < totalPages; p++) {
                    html += '<button class="page-btn' + (p === page ? ' active' : '') + '" onclick="goPage(' + p + ')">' + (p + 1) + '</button>';
                }
                html += '<button class="page-btn" onclick="goPage(' + (page + 1) + ')"' + (page >= totalPages - 1 ? ' disabled' : '') + '>&raquo;</button>';
                html += '<span class="page-info">' + (start + 1) + '\u2013' + end + ' of ' + total + '</span>';
                html += '</div>';
            }

            document.getElementById('resultsDiv').innerHTML = html;
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
                    var total = data.total_cids || 0;
                    document.getElementById('stat-total').textContent = total;
                    document.getElementById('stat-indexed').textContent = data.indexed || 0;
                    document.getElementById('stat-pending').textContent = data.pending || 0;
                    document.getElementById('stat-keywords').textContent = data.unique_keywords || 0;
                    document.getElementById('cidDownload').style.display = total > 0 ? '' : 'none';
                    document.getElementById('indexingBadge').style.display = data.indexing ? '' : 'none';
                    var mi = document.getElementById('modelInfo');
                    if (data.model) {
                        mi.textContent = 'Model: ' + data.model + '  \u00b7  API: ' + data.api_base;
                    }
                })
                .catch(function() {});
        }

        // --- Single CID ---
        var cidInput = document.getElementById('cidInput');
        var cidAddBtn = document.getElementById('cidAddBtn');
        var cidAddStatus = document.getElementById('cidAddStatus');

        function submitCID() {
            var cid = cidInput.value.trim();
            if (!cid) return;
            cidAddStatus.className = 'upload-status';
            cidAddStatus.textContent = '';
            fetch('/api/upload', {
                method: 'POST',
                headers: { 'Content-Type': 'text/plain' },
                body: cid
            })
            .then(function(r) { return r.json().then(function(d) { return { ok: r.ok, data: d }; }); })
            .then(function(res) {
                if (!res.ok) {
                    cidAddStatus.className = 'upload-status err';
                    cidAddStatus.textContent = res.data.error || 'Failed';
                    return;
                }
                cidInput.value = '';
                loadStats();
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

        // --- File Upload ---
        var uploadZone = document.getElementById('uploadZone');
        var uploadFile = document.getElementById('uploadFile');
        var uploadStatus = document.getElementById('uploadStatus');

        uploadZone.addEventListener('click', function() { uploadFile.click(); });

        uploadZone.addEventListener('dragover', function(e) {
            e.preventDefault();
            uploadZone.classList.add('drag-over');
        });
        uploadZone.addEventListener('dragleave', function() {
            uploadZone.classList.remove('drag-over');
        });
        uploadZone.addEventListener('drop', function(e) {
            e.preventDefault();
            uploadZone.classList.remove('drag-over');
            if (e.dataTransfer.files.length > 0) submitFile(e.dataTransfer.files[0]);
        });
        uploadFile.addEventListener('change', function() {
            if (uploadFile.files.length > 0) submitFile(uploadFile.files[0]);
        });

        function submitFile(file) {
            uploadStatus.className = 'upload-status';
            uploadStatus.textContent = 'Uploading ' + file.name + '...';
            var fd = new FormData();
            fd.append('file', file);
            fetch('/api/upload', { method: 'POST', body: fd })
                .then(function(r) { return r.json().then(function(d) { return { ok: r.ok, data: d }; }); })
                .then(function(res) {
                    uploadStatus.textContent = '';
                    if (!res.ok) {
                        uploadStatus.className = 'upload-status err';
                        uploadStatus.textContent = res.data.error || 'Upload failed';
                        return;
                    }
                    loadStats();
                })
                .catch(function(e) {
                    uploadStatus.className = 'upload-status err';
                    uploadStatus.textContent = 'Upload failed: ' + e.message;
                });
            uploadFile.value = '';
        }

        loadRecent();
        loadStats();
        setInterval(loadStats, 5000);
    </script>
</body>
</html>`
}
