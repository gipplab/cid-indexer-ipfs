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
		datanetwork   = flag.String("datanetwork", defaultDatanetworkURL, "public datanetwork readout to catalog; empty disables")
	)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [flags]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Indexes PDF documents from IPFS by extracting keywords and metadata\n")
		fmt.Fprintf(os.Stderr, "via an OpenAI-compatible LLM API.\n\n")
		fmt.Fprintf(os.Stderr, "Starts a web UI for searching and submitting document or archive CIDs.\n")
		fmt.Fprintf(os.Stderr, "With -datanetwork set, also lists every file in the IOSP datanetwork.\n\n")
		fmt.Fprintf(os.Stderr, "API key (required for indexing):\n")
		fmt.Fprintf(os.Stderr, "  Set in the admin language-model settings, or read from .api_key\n")
		fmt.Fprintf(os.Stderr, "  (in -o dir, then cwd) or SAIA_API_KEY. The admin password is separate.\n\n")
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

	dn := &dnState{}
	if page := strings.TrimSpace(*datanetwork); page != "" {
		slog.Info("cataloging datanetwork", "url", page)
		go runDatanetworkSync(store, cfg, indexer, page, dn)
	}

	if err := startServer(store, *port, cfg, indexer, dn); err != nil {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
}

// saveAPIKey writes .api_key in the data dir. That file is an indexing key,
// checked before SAIA_API_KEY. It is not the admin password.
func saveAPIKey(dataDir, key string) error {
	return writeSecret(dataDir, ".api_key", key)
}

// loadAdminPassword reads the admin password from the data dir. An empty
// result means the admin page is open until a password is set.
func loadAdminPassword(dataDir string) string {
	data, err := os.ReadFile(filepath.Join(dataDir, ".admin_password"))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// saveAdminPassword stores the admin password. Later logins must match it.
func saveAdminPassword(dataDir, password string) error {
	return writeSecret(dataDir, ".admin_password", password)
}

func writeSecret(dir, name, value string) error {
	value = strings.TrimSpace(value)
	if value == "" || strings.ContainsAny(value, "\r\n") {
		return fmt.Errorf("invalid secret")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(dir, name)
	tmp, err := os.CreateTemp(dir, name+".*")
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
	if _, err := tmp.WriteString(value + "\n"); err != nil {
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
