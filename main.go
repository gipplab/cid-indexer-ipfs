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

// PipelineConfig holds parameters for constructing an indexing pipeline.
type PipelineConfig struct {
	APIBase     string
	Model       string
	Gateway     string
	Workers     int
	ConvertRPS  int
	ChatRPS     int
	MaxTextLen  int
	ConvertTO   time.Duration
	Spacing     time.Duration
	Temperature float64
	MaxDepth    int
	MaxDocs     int
	DataDir     string
}

func main() {
	var (
		outputDir  = flag.String("o", ".", "data directory for the index, failures, archives, and moderation state")
		gateway    = flag.String("gateway", "https://ipfs.io", "IPFS gateway base URL")
		workers    = flag.Int("workers", 8, "number of concurrent processing workers")
		convertRPS = flag.Int("convert-rps", defaultConvertRPS, "max PDF-convert requests per second")
		chatRPS    = flag.Int("rps", defaultChatRPS, "max keyword-extraction (chat) requests per second")
		maxText    = flag.Int("max-text", defaultMaxTextLen, "max chars of document text sent to the LLM")
		convertTO  = flag.Duration("convert-timeout", defaultConvertTimeout, "HTTP timeout for a single PDF-convert request")
		model      = flag.String("model", defaultModel, "LLM model for keyword extraction")
		apiBase    = flag.String("api-base", defaultAPIBase, "OpenAI-compatible API base URL")
		spacing    = flag.Duration("spacing", 100*time.Millisecond, "minimum delay between dispatching CIDs to workers")
		temp       = flag.Float64("temp", defaultTemp, "sampling temperature for keyword extraction")
		maxDepth   = flag.Int("max-depth", defaultMaxDepth, "max directory recursion depth when crawling an archive")
		maxDocs    = flag.Int("max-docs", defaultMaxDocs, "max documents to discover per archive crawl")
		port       = flag.Int("port", defaultPort, "web UI port")
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
		APIBase:     *apiBase,
		Model:       *model,
		Gateway:     *gateway,
		Workers:     *workers,
		ConvertRPS:  *convertRPS,
		ChatRPS:     *chatRPS,
		MaxTextLen:  *maxText,
		ConvertTO:   *convertTO,
		Spacing:     *spacing,
		Temperature: *temp,
		MaxDepth:    *maxDepth,
		MaxDocs:     *maxDocs,
		DataDir:     *outputDir,
	}

	indexer := NewIndexer(store, cfg)

	// Recover documents that were marked permanently failed only because the API
	// was rate limiting; those are transient and should be retried rather than
	// dropped. Done before resuming archives so the requeued CIDs are picked up.
	if requeued := store.RequeueRateLimited(); requeued > 0 {
		slog.Info("requeued rate-limited failures for retry", "count", requeued)
	}

	// Resume any archives that were interrupted before completion. Those with a
	// persisted document list skip straight to (idempotent) indexing; the rest
	// are re-crawled.
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
