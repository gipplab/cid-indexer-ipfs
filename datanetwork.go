package main

import (
	"encoding/json"
	"fmt"
	"html"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	defaultDatanetworkURL = "https://www.iosp.science/datanetwork"
	datanetworkRefresh    = 5 * time.Minute
	datanetworkRetry      = 20 * time.Second
	datanetworkLSTimeout  = 3 * time.Minute
	datanetworkPageLimit  = 4 << 20
)

// Kubo peer IDs of the consortium meeting points (iosp-datanetwork-test
// ops/meeting-points.json). Empty addresses are filled in via the public DHT.
var datanetworkPeerIDs = []string{
	"12D3KooWJsbtcPjYbsEd2pDG1JPWAx37qoW3S4rbgyNsVkArDxFX", // node-00, iosp-nodes
	"12D3KooWJZJS2rsP7KRBhiuQUCeqwwcJpWkJmJ2d7LafsxQooVaJ", // node-00-observer, iosp-laptops
}

var (
	fullCIDRe    = regexp.MustCompile(`\b(Qm[1-9A-HJ-NP-Za-km-z]{44}|bafy[a-z2-7]{20,90})\b`)
	datasetRowRe = regexp.MustCompile(`<span class="text-sm text-ink">([^<]+)</span>\s*<span class="font-mono text-xs text-ink-mute">([1-9A-HJ-NP-Za-km-z]+)`)
	h2Re         = regexp.MustCompile(`(?s)<h2[^>]*>(.*?)</h2>`)
	tagRe        = regexp.MustCompile(`<[^>]*>`)
)

// dnState is the in-memory sync status shown beside the stored catalog.
type dnState struct {
	mu      sync.Mutex
	url     string
	err     string
	syncing bool
}

func (d *dnState) setURL(u string) {
	d.mu.Lock()
	d.url = u
	d.mu.Unlock()
}

func (d *dnState) begin() {
	d.mu.Lock()
	d.syncing = true
	d.mu.Unlock()
}

func (d *dnState) finish(err error) {
	d.mu.Lock()
	d.syncing = false
	if err != nil {
		d.err = err.Error()
	} else {
		d.err = ""
	}
	d.mu.Unlock()
}

func (d *dnState) snapshot() (pageURL, errMsg string, syncing bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.url, d.err, d.syncing
}

// runDatanetworkSync keeps the file catalog aligned with the public readout.
func runDatanetworkSync(store *Store, cfg PipelineConfig, ix *Indexer, pageURL string, state *dnState) {
	state.setURL(pageURL)
	for {
		state.begin()
		err := syncDatanetworkOnce(store, cfg, ix, pageURL)
		state.finish(err)
		if err != nil {
			slog.Warn("datanetwork sync incomplete", "error", err)
			time.Sleep(datanetworkRetry)
			continue
		}
		time.Sleep(datanetworkRefresh)
	}
}

func syncDatanetworkOnce(store *Store, cfg PipelineConfig, ix *Indexer, pageURL string) error {
	if strings.TrimSpace(cfg.IPFSAPI) == "" {
		return fmt.Errorf("datanetwork sync needs -ipfs-api so files can be listed from Kubo")
	}
	connectDatanetworkPeers(cfg.IPFSAPI)

	body, err := fetchDatanetworkPage(pageURL)
	if err != nil {
		return err
	}
	found := parseDatanetworkPage(body)
	if len(found) == 0 {
		return fmt.Errorf("datanetwork page listed no datasets")
	}
	slog.Info("datanetwork readout", "datasets", len(found))

	pending := store.UpsertDatasets(found)
	var failed []string
	for _, ds := range pending {
		store.MarkDatasetCrawling(ds.CID)
		files, truncated, err := walkUnixFS(cfg.IPFSAPI, ds.CID, cfg.MaxDepth, cfg.MaxDocs)
		if err != nil {
			slog.Warn("dataset listing failed", "name", ds.Name, "cid", ds.CID, "error", err)
			store.MarkDatasetFailed(ds.CID, err.Error())
			failed = append(failed, ds.Name)
			continue
		}
		if len(files) == 1 && files[0].Path == "" {
			files[0].Name = ds.Name
		}
		note := ""
		if truncated {
			note = fmt.Sprintf("listing stopped at %d entries", cfg.MaxDocs)
		}
		store.SaveDatasetFiles(ds.CID, files, note)
		slog.Info("dataset listed", "name", ds.Name, "cid", ds.CID, "entries", len(files))
		go pinCID(cfg.IPFSAPI, ds.CID)
	}
	var listed []NetworkFile
	for _, ds := range store.Datanetwork() {
		listed = append(listed, ds.Files...)
	}
	enqueueFiles(store, ix, cfg, listed)
	if len(failed) > 0 {
		return fmt.Errorf("could not list %s", strings.Join(failed, ", "))
	}
	return nil
}

func enqueueFiles(store *Store, ix *Indexer, cfg PipelineConfig, files []NetworkFile) {
	if ix == nil {
		return
	}
	if _, err := resolveLLM(store, cfg); err != nil {
		return
	}
	var pdfs []string
	seen := map[string]struct{}{}
	for _, f := range files {
		if f.Dir {
			continue
		}
		if _, ok := seen[f.CID]; ok {
			continue
		}
		seen[f.CID] = struct{}{}
		pdfs = append(pdfs, f.CID)
	}
	if len(pdfs) > 0 {
		ix.EnqueueDocs(pdfs)
	}
}

func connectDatanetworkPeers(api string) {
	for _, id := range datanetworkPeerIDs {
		q := url.Values{}
		q.Set("arg", "/p2p/"+id)
		resp, err := kuboCall(api, "swarm/connect", q, 90*time.Second)
		if err != nil {
			slog.Warn("swarm connect failed", "peer", id, "error", err)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			err := readKuboErr(resp)
			resp.Body.Close()
			slog.Warn("swarm connect rejected", "peer", id, "error", err)
			continue
		}
		resp.Body.Close()
		slog.Info("connected to datanetwork peer", "peer", id)
	}
}

func pinCID(api, cid string) {
	q := url.Values{}
	q.Set("arg", cid)
	q.Set("recursive", "true")
	resp, err := kuboCall(api, "pin/add", q, 3*time.Minute)
	if err != nil {
		slog.Warn("pin failed", "cid", cid, "error", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		slog.Warn("pin rejected", "cid", cid, "error", readKuboErr(resp))
		return
	}
	slog.Info("pinned dataset", "cid", cid)
}

func fetchDatanetworkPage(pageURL string) (string, error) {
	req, err := http.NewRequest(http.MethodGet, pageURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "text/html")
	req.Header.Set("User-Agent", "cidindexer-ipfs")
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("datanetwork page: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("datanetwork page returned %s", resp.Status)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, datanetworkPageLimit))
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// parseDatanetworkPage reads dataset name, CID, and cluster from the public
// readout. Visible rows carry a short CID prefix; the full CID is matched
// from elsewhere on the same page.
func parseDatanetworkPage(body string) []DatasetRef {
	full := fullCIDRe.FindAllString(body, -1)
	var heads []pageHead
	for _, loc := range h2Re.FindAllStringSubmatchIndex(body, -1) {
		name := strings.TrimSpace(tagRe.ReplaceAllString(body[loc[2]:loc[3]], ""))
		name = strings.TrimSpace(html.UnescapeString(name))
		heads = append(heads, pageHead{at: loc[0], name: name})
	}

	order := []string{}
	byCID := map[string]*DatasetRef{}
	for _, loc := range datasetRowRe.FindAllStringSubmatchIndex(body, -1) {
		name := strings.TrimSpace(html.UnescapeString(body[loc[2]:loc[3]]))
		prefix := body[loc[4]:loc[5]]
		cid := matchCIDPrefix(full, prefix)
		if name == "" || cid == "" {
			continue
		}
		cluster := clusterAt(heads, loc[0])
		if prev, ok := byCID[cid]; ok {
			if cluster != "" {
				prev.Clusters = append(prev.Clusters, cluster)
			}
			if prev.Name == "" {
				prev.Name = name
			}
			continue
		}
		ref := &DatasetRef{CID: cid, Name: name}
		if cluster != "" {
			ref.Clusters = []string{cluster}
		}
		byCID[cid] = ref
		order = append(order, cid)
	}

	out := make([]DatasetRef, 0, len(order))
	for _, cid := range order {
		ref := byCID[cid]
		ref.Clusters = normalizeClusters(ref.Clusters)
		out = append(out, *ref)
	}
	return out
}

func matchCIDPrefix(full []string, prefix string) string {
	var hit string
	for _, cid := range full {
		if strings.HasPrefix(cid, prefix) {
			if hit != "" && hit != cid {
				return ""
			}
			hit = cid
		}
	}
	return hit
}

type pageHead struct {
	at   int
	name string
}

func clusterAt(heads []pageHead, pos int) string {
	name := ""
	for _, h := range heads {
		if h.at > pos {
			break
		}
		name = h.name
	}
	return name
}

// walkUnixFS lists every file under cid. truncated is set when maxFiles stops the walk.
func walkUnixFS(api, root string, maxDepth, maxFiles int) (files []NetworkFile, truncated bool, err error) {
	if maxDepth <= 0 {
		maxDepth = defaultMaxDepth
	}
	if maxFiles <= 0 {
		maxFiles = defaultMaxDocs
	}
	kind, size, err := kuboUnixfsStat(api, root)
	if err != nil {
		return nil, false, err
	}
	if kind != "directory" {
		return []NetworkFile{{
			Path: "",
			Name: root,
			CID:  root,
			Size: size,
		}}, false, nil
	}

	seen := map[string]struct{}{}

	var walk func(cid, rel string, depth int) error
	walk = func(cid, rel string, depth int) error {
		if len(files) >= maxFiles {
			truncated = true
			return nil
		}
		if _, ok := seen[cid+"\n"+rel]; ok {
			return nil
		}
		seen[cid+"\n"+rel] = struct{}{}

		links, lsErr := kuboLS(api, cid, datanetworkLSTimeout)
		if lsErr != nil {
			if !unixfsNotDir(lsErr) {
				return lsErr
			}
			name := path.Base(rel)
			if rel == "" || name == "." || name == "/" {
				name = cid
			}
			files = append(files, NetworkFile{
				Path: rel,
				Name: name,
				CID:  cid,
				Size: kuboUnixfsSize(api, cid),
			})
			return nil
		}
		if len(links) == 0 {
			if rel != "" {
				files = append(files, NetworkFile{
					Path: rel,
					Name: path.Base(rel),
					CID:  cid,
					Dir:  true,
				})
			}
			return nil
		}
		for _, l := range links {
			if l.Name == "" || l.Name == "." || l.Name == ".." || strings.ContainsAny(l.Name, "/\\") {
				continue
			}
			if len(files) >= maxFiles {
				truncated = true
				return nil
			}
			child := l.Name
			if rel != "" {
				child = rel + "/" + l.Name
			}
			if l.Dir {
				if depth+1 >= maxDepth {
					files = append(files, NetworkFile{
						Path: child,
						Name: l.Name,
						CID:  l.CID,
						Size: l.Size,
						Dir:  true,
					})
					continue
				}
				if err := walk(l.CID, child, depth+1); err != nil {
					return err
				}
				continue
			}
			files = append(files, NetworkFile{
				Path: child,
				Name: l.Name,
				CID:  l.CID,
				Size: l.Size,
			})
		}
		return nil
	}
	if err := walk(root, "", 0); err != nil {
		return nil, truncated, err
	}
	return files, truncated, nil
}

func unixfsNotDir(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "not a directory") || strings.Contains(s, "not a dir")
}

func kuboUnixfsSize(api, cid string) int64 {
	_, size, err := kuboUnixfsStat(api, cid)
	if err != nil {
		return 0
	}
	return size
}

func kuboUnixfsStat(api, cid string) (string, int64, error) {
	q := url.Values{}
	q.Set("arg", "/ipfs/"+cid)
	resp, err := kuboCall(api, "files/stat", q, 30*time.Second)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", 0, readKuboErr(resp)
	}
	var parsed struct {
		Size int64  `json:"Size"`
		Type string `json:"Type"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return "", 0, err
	}
	return parsed.Type, parsed.Size, nil
}
