package main

import (
	"bufio"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// PipelineConfig holds parameters for constructing an indexing pipeline.
type PipelineConfig struct {
	APIBase string
	Model   string
	Gateway string
	Workers int
	Spacing time.Duration
	DataDir string
}

func main() {
	var (
		outputDir = flag.String("o", ".", "output directory for index and failure files")
		gateway   = flag.String("gateway", "https://ipfs.io", "IPFS gateway base URL")
		workers   = flag.Int("workers", 4, "number of concurrent processing workers")
		model     = flag.String("model", defaultModel, "LLM model for keyword extraction")
		apiBase   = flag.String("api-base", defaultAPIBase, "OpenAI-compatible API base URL")
		spacing   = flag.Duration("spacing", 100*time.Millisecond, "minimum delay between dispatching CIDs to workers")
		cli       = flag.Bool("cli", false, "run in CLI mode (index CIDs and exit, no web UI)")
		port      = flag.Int("port", defaultPort, "web UI port")
	)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [flags] [cid-file]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Indexes PDF documents from IPFS by extracting keywords and metadata\n")
		fmt.Fprintf(os.Stderr, "via an OpenAI-compatible LLM API.\n\n")
		fmt.Fprintf(os.Stderr, "By default, starts a web UI for searching and uploading CIDs.\n")
		fmt.Fprintf(os.Stderr, "If a cid-file is given, indexing runs in the background alongside the UI.\n")
		fmt.Fprintf(os.Stderr, "Use -cli to index without starting the web server.\n\n")
		fmt.Fprintf(os.Stderr, "API key (required for indexing):\n")
		fmt.Fprintf(os.Stderr, "  Read from .api_key file (in -o dir, then cwd), or SAIA_API_KEY env var.\n\n")
		fmt.Fprintf(os.Stderr, "Flags:\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	store := NewStore(*outputDir)
	cfg := PipelineConfig{
		APIBase: *apiBase,
		Model:   *model,
		Gateway: *gateway,
		Workers: *workers,
		Spacing: *spacing,
		DataDir: *outputDir,
	}

	cidFile := flag.Arg(0)

	if *cli {
		if cidFile == "" {
			slog.Error("CLI mode requires a CID file argument")
			os.Exit(1)
		}
		runIndexer(store, cidFile, cfg)
		return
	}

	if cidFile != "" {
		go func() {
			indexingActive.Store(true)
			defer indexingActive.Store(false)
			runIndexer(store, cidFile, cfg)
		}()
	} else {
		go resumeIfPending(store, cfg)
	}

	if err := startServer(store, *port, cfg); err != nil {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
}

// runIndexer reads CIDs from a file, persists them, and indexes any pending ones.
func runIndexer(store *Store, cidFile string, cfg PipelineConfig) {
	apiKey := loadAPIKey(cfg.DataDir)
	if apiKey == "" {
		slog.Error("no API key found (checked .api_key file and SAIA_API_KEY env)")
		return
	}

	cids, err := readCIDs(cidFile)
	if err != nil {
		slog.Error("failed to read CID list", "error", err)
		return
	}
	if len(cids) == 0 {
		slog.Warn("no CIDs in file")
		return
	}

	store.AppendCIDs(cids)

	pending := store.Pending(store.AllCIDs())
	if len(pending) == 0 {
		slog.Info("all CIDs already indexed")
		return
	}
	indexPending(store, pending, apiKey, cfg)
}

// resumeIfPending picks up unfinished work from the persistent CID list on startup.
func resumeIfPending(store *Store, cfg PipelineConfig) {
	pending := store.Pending(store.AllCIDs())
	if len(pending) == 0 {
		return
	}
	apiKey := loadAPIKey(cfg.DataDir)
	if apiKey == "" {
		slog.Warn("pending CIDs but no API key, skipping", "pending", len(pending))
		return
	}
	slog.Info("resuming from persistent CID list", "pending", len(pending))
	indexingActive.Store(true)
	defer indexingActive.Store(false)
	indexPending(store, pending, apiKey, cfg)
}

// indexPending processes a pre-filtered list of pending CIDs through the pipeline.
func indexPending(store *Store, pending []string, apiKey string, cfg PipelineConfig) {
	pipeline := &Pipeline{
		APIKey:  apiKey,
		APIBase: cfg.APIBase,
		Model:   cfg.Model,
		Gateway: cfg.Gateway,
	}

	slog.Info("indexing", "pending", len(pending), "workers", cfg.Workers, "model", cfg.Model)

	work := make(chan string, cfg.Workers)
	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for cid := range work {
				entry, err := pipeline.Process(cid)
				if err != nil {
					slog.Error("processing failed", "cid", cid, "error", err)
					store.RecordFailure(cid, err.Error())
					continue
				}
				if entry == nil {
					store.RecordSkip()
					continue
				}
				store.Add(entry)
				slog.Info("indexed", "cid", cid, "title", entry.Title, "keywords", len(entry.Keywords))
			}
		}()
	}

	ticker := time.NewTicker(cfg.Spacing)
	defer ticker.Stop()
	for _, cid := range pending {
		<-ticker.C
		work <- cid
	}
	close(work)
	wg.Wait()

	store.SaveAll()
	stats := store.Stats()
	slog.Info("indexing complete",
		"indexed", stats.Indexed,
		"failed", stats.Failed,
		"keywords", stats.UniqueKeywords,
	)
}

// loadAPIKey reads the API key from .api_key (checking dataDir then cwd),
// falling back to the SAIA_API_KEY environment variable.
func loadAPIKey(dataDir string) string {
	for _, dir := range []string{dataDir, "."} {
		path := filepath.Join(dir, ".api_key")
		data, err := os.ReadFile(path)
		if err == nil {
			if key := strings.TrimSpace(string(data)); key != "" {
				slog.Info("loaded API key from file", "path", path)
				return key
			}
		}
	}
	if key := os.Getenv("SAIA_API_KEY"); key != "" {
		slog.Info("using API key from SAIA_API_KEY env")
		return key
	}
	return ""
}

func readCIDs(path string) ([]string, error) {
	var r *os.File
	if path == "" || path == "-" {
		r = os.Stdin
	} else {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		r = f
	}

	var cids []string
	seen := make(map[string]struct{})
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if _, dup := seen[line]; dup {
			continue
		}
		seen[line] = struct{}{}
		cids = append(cids, line)
	}
	return cids, sc.Err()
}

func parseCIDList(text string) []string {
	var cids []string
	seen := make(map[string]struct{})
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if _, dup := seen[line]; dup {
			continue
		}
		seen[line] = struct{}{}
		cids = append(cids, line)
	}
	return cids
}
