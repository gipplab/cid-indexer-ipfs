package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	enumTimeout     = 60 * time.Second
	classifyTimeout = 30 * time.Second
	classifyPeek    = 1024 // bytes inspected to classify a CID

	defaultMaxDepth = 8
	defaultMaxDocs  = 5000
)

// nodeKind classifies what an IPFS CID points to.
type nodeKind int

const (
	kindUnknown nodeKind = iota
	kindDir
	kindPDF
	kindOther
)

func (k nodeKind) String() string {
	switch k {
	case kindDir:
		return "directory"
	case kindPDF:
		return "pdf"
	case kindOther:
		return "other"
	default:
		return "unknown"
	}
}

// childEntry is a single entry inside an IPFS UnixFS directory.
type childEntry struct {
	Name string
	CID  string
}

// crawler enumerates document CIDs in an archive via an IPFS gateway.
type crawler struct {
	gateway  string
	maxDepth int
	maxDocs  int
	client   *http.Client
}

func newCrawler(gateway string, maxDepth, maxDocs int) *crawler {
	if maxDepth <= 0 {
		maxDepth = defaultMaxDepth
	}
	if maxDocs <= 0 {
		maxDocs = defaultMaxDocs
	}
	return &crawler{
		gateway:  strings.TrimRight(gateway, "/"),
		maxDepth: maxDepth,
		maxDocs:  maxDocs,
		client:   &http.Client{Timeout: enumTimeout},
	}
}

// Classify determines whether a CID is a directory, PDF, or other file.
func (c *crawler) Classify(cid string) (nodeKind, error) {
	reqURL := c.gateway + "/ipfs/" + url.PathEscape(cid)
	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		return kindUnknown, err
	}
	req.Header.Set("Range", "bytes=0-"+strconv.Itoa(classifyPeek-1))

	client := &http.Client{Timeout: classifyTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return kindUnknown, fmt.Errorf("gateway request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return kindUnknown, fmt.Errorf("gateway returned %s", resp.Status)
	}

	ct := strings.ToLower(resp.Header.Get("Content-Type"))
	peek, _ := io.ReadAll(io.LimitReader(resp.Body, classifyPeek))

	switch {
	case strings.HasPrefix(string(peek), "%PDF"):
		return kindPDF, nil
	case strings.Contains(ct, "application/pdf"):
		return kindPDF, nil
	case strings.Contains(ct, "text/html"):
		// Gateways render UnixFS directories as an HTML index. A redirect from
		// /ipfs/{cid} to /ipfs/{cid}/ is followed automatically by the client.
		return kindDir, nil
	default:
		return kindOther, nil
	}
}

// nonPDFExt lists common file extensions that are neither directories nor
// PDFs, so such children can be skipped without a gateway round-trip.
var nonPDFExt = map[string]struct{}{
	".txt": {}, ".md": {}, ".json": {}, ".xml": {}, ".csv": {}, ".tsv": {},
	".html": {}, ".htm": {}, ".png": {}, ".jpg": {}, ".jpeg": {}, ".gif": {},
	".svg": {}, ".webp": {}, ".zip": {}, ".gz": {}, ".tar": {}, ".bz2": {},
	".doc": {}, ".docx": {}, ".ppt": {}, ".pptx": {}, ".xls": {}, ".xlsx": {},
	".mp4": {}, ".mp3": {}, ".wav": {}, ".avi": {}, ".mov": {}, ".bin": {},
	".epub": {}, ".djvu": {}, ".ps": {}, ".tex": {}, ".bib": {}, ".yaml": {}, ".yml": {},
}

// classifyByName guesses a child's kind from its filename, avoiding a gateway fetch.
func classifyByName(name string) nodeKind {
	n := strings.ToLower(strings.TrimSpace(name))
	if n == "" {
		return kindUnknown
	}
	if strings.HasSuffix(n, ".pdf") {
		return kindPDF
	}
	ext := path.Ext(n)
	if ext == "" {
		return kindUnknown // no extension: most likely a subdirectory
	}
	if _, ok := nonPDFExt[ext]; ok {
		return kindOther
	}
	return kindUnknown // unknown extension: probe to avoid skipping a directory
}

// Crawl walks an archive CID and returns PDF document CIDs in discovery order.
func (c *crawler) Crawl(rootCID string) ([]string, error) {
	kind, err := c.Classify(rootCID)
	if err != nil {
		return nil, fmt.Errorf("classify root: %w", err)
	}
	if kind != kindDir {
		return nil, fmt.Errorf("CID is not a directory (kind=%s)", kind)
	}

	visited := make(map[string]struct{})
	docs := make(map[string]struct{})
	var order []string

	var walk func(cid string, depth int)
	walk = func(cid string, depth int) {
		if len(order) >= c.maxDocs {
			return
		}
		if _, seen := visited[cid]; seen {
			return
		}
		visited[cid] = struct{}{}

		children, err := c.enumerateDir(cid)
		if err != nil {
			slog.Warn("enumerate failed", "cid", cid, "error", err)
			return
		}
		for _, ch := range children {
			if len(order) >= c.maxDocs {
				slog.Warn("max docs reached, stopping crawl", "max", c.maxDocs)
				return
			}
			if _, seen := visited[ch.CID]; seen {
				continue
			}
			kind := classifyByName(ch.Name)
			if kind == kindUnknown {
				var err error
				kind, err = c.Classify(ch.CID)
				if err != nil {
					slog.Warn("classify child failed", "cid", ch.CID, "name", ch.Name, "error", err)
					continue
				}
			}
			switch kind {
			case kindDir:
				if depth+1 < c.maxDepth {
					walk(ch.CID, depth+1)
				} else {
					slog.Warn("max depth reached, skipping subdir", "cid", ch.CID, "depth", depth+1)
				}
			case kindPDF:
				if _, dup := docs[ch.CID]; !dup {
					docs[ch.CID] = struct{}{}
					order = append(order, ch.CID)
				}
			default:
				slog.Debug("skipping non-PDF entry", "cid", ch.CID, "name", ch.Name)
			}
		}
	}

	walk(rootCID, 0)
	return order, nil
}

// enumerateDir lists immediate children, trying dag-json first then HTML fallback.
func (c *crawler) enumerateDir(cid string) ([]childEntry, error) {
	if children, err := c.enumerateDagJSON(cid); err == nil && len(children) > 0 {
		return children, nil
	}
	return c.enumerateHTML(cid)
}

func (c *crawler) enumerateDagJSON(cid string) ([]childEntry, error) {
	reqURL := c.gateway + "/ipfs/" + url.PathEscape(cid) + "?format=dag-json"
	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.ipld.dag-json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %s", resp.Status)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "json") {
		return nil, fmt.Errorf("unexpected content-type %q", ct)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxFetchSize))
	if err != nil {
		return nil, err
	}

	var node struct {
		Links []struct {
			Hash struct {
				Slash string `json:"/"`
			} `json:"Hash"`
			Name string `json:"Name"`
		} `json:"Links"`
	}
	if err := json.Unmarshal(body, &node); err != nil {
		return nil, err
	}

	out := make([]childEntry, 0, len(node.Links))
	for _, l := range node.Links {
		if l.Hash.Slash == "" {
			continue
		}
		out = append(out, childEntry{Name: l.Name, CID: l.Hash.Slash})
	}
	return out, nil
}

// dirHrefRe matches the per-entry links emitted by the gateway HTML directory
// index, e.g. href="/ipfs/Qm...?filename=paper". The query-string form carries
// the child's own CID, unlike the path-form link.
var dirHrefRe = regexp.MustCompile(`href="/ipfs/([A-Za-z0-9]+)\?filename=([^"]*)"`)

func (c *crawler) enumerateHTML(cid string) ([]childEntry, error) {
	reqURL := c.gateway + "/ipfs/" + url.PathEscape(cid) + "/"
	resp, err := c.client.Get(reqURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %s", resp.Status)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxFetchSize))
	if err != nil {
		return nil, err
	}

	matches := dirHrefRe.FindAllStringSubmatch(string(body), -1)
	seen := make(map[string]struct{})
	out := make([]childEntry, 0, len(matches))
	for _, m := range matches {
		childCID, rawName := m[1], m[2]
		if childCID == cid {
			continue // self-link in the index header
		}
		if _, dup := seen[childCID]; dup {
			continue
		}
		seen[childCID] = struct{}{}
		name := rawName
		if dec, err := url.QueryUnescape(rawName); err == nil {
			name = dec
		}
		out = append(out, childEntry{Name: name, CID: childCID})
	}
	return out, nil
}
