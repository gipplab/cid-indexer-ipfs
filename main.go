package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// PipelineConfig holds parameters for the indexing pipeline.
type PipelineConfig struct {
	APIBase       string
	Model         string
	Gateway       string
	IPFSAPI       string // Kubo RPC; when set, fetches and /ipfs links use it instead of Gateway
	PublicGateway string // absolute link base for the web UI; empty serves /ipfs on this server
	Workers       int
	ConvertRPS    int
	ChatRPS       int
	MaxTextLen    int
	ConvertTO     time.Duration
	Spacing       time.Duration
	Temperature   float64
	MaxDepth      int
	MaxDocs       int
	DataDir       string
}

// linkGateway is the base URL embedded in the web UI. Empty means same-origin
// /ipfs links, served by this process.
func (cfg PipelineConfig) linkGateway() string {
	return strings.TrimRight(cfg.PublicGateway, "/")
}

func main() {
	var (
		outputDir     = flag.String("o", "data", "data directory for the index, failures, archives, and moderation state")
		gateway       = flag.String("gateway", "https://ipfs.io", "IPFS gateway base URL used to fetch and crawl CIDs")
		ipfsAPI       = flag.String("ipfs-api", "", "Kubo RPC base URL (e.g. http://ipfs:5001); when set, fetches use the API instead of -gateway")
		publicGateway = flag.String("public-gateway", "", "absolute gateway base URL for links in the web UI; empty serves them on this server")
		workers       = flag.Int("workers", 8, "number of concurrent processing workers")
		convertRPS    = flag.Int("convert-rps", defaultConvertRPS, "max concurrent PDF-convert requests")
		chatRPS       = flag.Int("rps", defaultChatRPS, "max keyword-extraction (chat) requests per second")
		maxText       = flag.Int("max-text", defaultMaxTextLen, "max chars of document text sent to the LLM")
		convertTO     = flag.Duration("convert-timeout", defaultConvertTimeout, "HTTP timeout for a single PDF-convert request")
		model         = flag.String("model", defaultModel, "LLM model for keyword extraction")
		apiBase       = flag.String("api-base", defaultAPIBase, "OpenAI-compatible API base URL")
		spacing       = flag.Duration("spacing", 100*time.Millisecond, "minimum delay between dispatching CIDs to workers")
		temp          = flag.Float64("temp", defaultTemp, "sampling temperature for keyword extraction")
		maxDepth      = flag.Int("max-depth", defaultMaxDepth, "max directory recursion depth when crawling an archive")
		maxDocs       = flag.Int("max-docs", defaultMaxDocs, "max documents to discover per archive crawl")
		port          = flag.Int("port", defaultPort, "web UI port")
	)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [flags]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Indexes PDF documents from IPFS by extracting keywords and metadata\n")
		fmt.Fprintf(os.Stderr, "via an OpenAI-compatible LLM API.\n\n")
		fmt.Fprintf(os.Stderr, "Starts a web UI for searching and submitting document or archive CIDs.\n\n")
		fmt.Fprintf(os.Stderr, "API key (required for indexing):\n")
		fmt.Fprintf(os.Stderr, "  Read from .api_key file (in -o dir, then cwd), or SAIA_API_KEY env var.\n\n")
		fmt.Fprintf(os.Stderr, "Flags:\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	store := NewStore(*outputDir)
	cfg := PipelineConfig{
		APIBase:       *apiBase,
		Model:         *model,
		Gateway:       *gateway,
		IPFSAPI:       *ipfsAPI,
		PublicGateway: *publicGateway,
		Workers:       *workers,
		ConvertRPS:    *convertRPS,
		ChatRPS:       *chatRPS,
		MaxTextLen:    *maxText,
		ConvertTO:     *convertTO,
		Spacing:       *spacing,
		Temperature:   *temp,
		MaxDepth:      *maxDepth,
		MaxDocs:       *maxDocs,
		DataDir:       *outputDir,
	}

	indexer := NewIndexer(store, cfg)

	if requeued := store.RequeueRateLimited(); requeued > 0 {
		slog.Info("requeued rate-limited failures for retry", "count", requeued)
	}

	if resumable := store.ResumableArchives(); len(resumable) > 0 {
		slog.Info("resuming interrupted archives", "count", len(resumable))
		for _, cid := range resumable {
			indexer.EnqueueArchive(cid, "")
		}
	}

	if err := startServer(store, *port, cfg, indexer); err != nil {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
}

// saveAPIKey writes .api_key in the data dir. That file is checked before the
// SAIA_API_KEY env var, so it becomes the key for indexing and admin login.
func saveAPIKey(dataDir, key string) error {
	key = strings.TrimSpace(key)
	if key == "" || strings.ContainsAny(key, "\r\n") {
		return fmt.Errorf("invalid API key")
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(dataDir, ".api_key")
	tmp, err := os.CreateTemp(dataDir, ".api_key.*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			os.Remove(tmpName)
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.WriteString(key + "\n"); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	cleanup = false
	return nil
}

// loadAPIKey reads .api_key from the data dir, then cwd, then SAIA_API_KEY.
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
