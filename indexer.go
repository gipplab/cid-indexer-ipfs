package main

import (
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Job kinds for the background dispatcher.
const (
	jobArchive = "archive"
	jobDocs    = "docs"
)

// indexJob is a unit of work for the background dispatcher.
type indexJob struct {
	kind    string
	cid     string   // archive CID (jobArchive)
	owner   string   // archive owner label (jobArchive)
	docCIDs []string // document CIDs (jobDocs)
}

// Indexer runs indexing work through a single background queue.
type Indexer struct {
	store  *Store
	cfg    PipelineConfig
	jobs   chan indexJob
	queued atomic.Int64
}

func NewIndexer(store *Store, cfg PipelineConfig) *Indexer {
	ix := &Indexer{
		store: store,
		cfg:   cfg,
		jobs:  make(chan indexJob, 4096),
	}
	go ix.run()
	return ix
}

// Queued returns jobs waiting to be processed, excluding the one in flight.
func (ix *Indexer) Queued() int {
	q := ix.queued.Load()
	if q < 0 {
		return 0
	}
	return int(q)
}

// EnqueueArchive registers an archive and schedules it for crawling + indexing.
func (ix *Indexer) EnqueueArchive(cid, owner string) {
	ix.store.AddArchive(cid, owner)
	ix.enqueue(indexJob{kind: jobArchive, cid: cid, owner: owner})
}

// EnqueueDocs schedules document CIDs for indexing.
func (ix *Indexer) EnqueueDocs(cids []string) {
	if len(cids) == 0 {
		return
	}
	ix.enqueue(indexJob{kind: jobDocs, docCIDs: cids})
}

func (ix *Indexer) enqueue(job indexJob) {
	ix.queued.Add(1)
	ix.jobs <- job
}

func (ix *Indexer) run() {
	for job := range ix.jobs {
		ix.queued.Add(-1)
		indexingActive.Store(true)
		ix.process(job)
		if ix.queued.Load() <= 0 && len(ix.jobs) == 0 {
			indexingActive.Store(false)
		}
	}
}

func (ix *Indexer) process(job indexJob) {
	apiKey := loadAPIKey(ix.cfg.DataDir)
	if apiKey == "" {
		slog.Warn("skipping queued job, no API key configured", "kind", job.kind)
		if job.kind == jobArchive {
			ix.store.MarkArchiveFailed(job.cid, "no API key configured for indexing")
		}
		return
	}

	switch job.kind {
	case jobArchive:
		indexArchive(ix.store, job.cid, job.owner, apiKey, ix.cfg)
	case jobDocs:
		pending := ix.store.Pending(job.docCIDs)
		if len(pending) > 0 {
			indexPending(ix.store, pending, apiKey, ix.cfg)
		} else {
			slog.Info("all queued document CIDs already indexed")
		}
	}
}

func indexPending(store *Store, pending []string, apiKey string, cfg PipelineConfig) {
	pipeline := &Pipeline{
		APIKey:      apiKey,
		APIBase:     cfg.APIBase,
		Model:       cfg.Model,
		Gateway:     cfg.Gateway,
		Temperature: cfg.Temperature,
		ConvertRPS:  cfg.ConvertRPS,
		ChatRPS:     cfg.ChatRPS,
		MaxTextLen:  cfg.MaxTextLen,
		ConvertTO:   cfg.ConvertTO,
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
					if errors.Is(err, ErrRateLimited) {
						slog.Warn("rate limited, leaving CID for a later run", "cid", cid, "error", err)
						store.RecordRateLimited(cid, err.Error())
						continue
					}
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

// indexArchive crawls an archive CID, indexes its PDFs, and aggregates labels.
// If the document list was persisted from a prior run, the crawl is skipped.
func indexArchive(store *Store, archiveCID, owner, apiKey string, cfg PipelineConfig) {
	store.AddArchive(archiveCID, owner)

	docCIDs := store.ArchiveDocCIDs(archiveCID)
	if len(docCIDs) > 0 {
		slog.Info("resuming archive, skipping crawl", "cid", archiveCID, "documents", len(docCIDs))
	} else {
		store.MarkArchiveCrawling(archiveCID)

		cr := newCrawler(cfg.Gateway, cfg.MaxDepth, cfg.MaxDocs)
		slog.Info("crawling archive", "cid", archiveCID)
		crawled, err := cr.Crawl(archiveCID)
		if err != nil {
			slog.Error("archive crawl failed", "cid", archiveCID, "error", err)
			store.MarkArchiveFailed(archiveCID, err.Error())
			return
		}

		store.SetArchiveDocs(archiveCID, crawled)
		slog.Info("archive crawled", "cid", archiveCID, "documents", len(crawled))
		docCIDs = crawled
	}

	if len(docCIDs) > 0 {
		pending := store.Pending(docCIDs)
		if len(pending) > 0 {
			indexPending(store, pending, apiKey, cfg)
		}
	}

	store.FinalizeArchive(archiveCID)
	store.SaveAll()
	if a, _, ok := store.GetArchive(archiveCID); ok {
		slog.Info("archive indexed",
			"cid", archiveCID,
			"indexed", a.Indexed,
			"failed", a.Failed,
			"keywords", len(a.TopKeywords),
		)
	}
}
