# cidindexer-ipfs

Extracts structured metadata and keywords from PDF documents stored on IPFS,
using an OpenAI-compatible LLM API. Includes a built-in web UI for searching
and managing the index.

Given an IPFS CID pointing to a PDF file, the tool:

1. Fetches the document from an IPFS gateway.
2. Converts the PDF to markdown via the API's document conversion endpoint.
3. Sends the markdown to an LLM to extract title, research field, topic,
   and the 10 most relevant keywords.
4. Persists the results to an embedded SQLite database (`index.db`), with an
   FTS5 full-text index over titles, fields, and keywords.

Processing is incremental: already indexed CIDs are skipped on subsequent runs.
Permanently failed CIDs (after 3 retries) are tracked in the same database.
The tool works exclusively with two kinds of input: a **document** CID (a single
PDF) and an **archive** CID (a directory of documents); both are submitted
through the web UI.

## Archives

In addition to individual documents, you can submit an **archive** CID, an
IPFS UnixFS directory (a collection of documents, possibly nested). The tool:

1. Crawls the directory recursively via the gateway, discovering every PDF it
   contains (directory entries are enumerated through the gateway's `dag-json`
   listing where available, falling back to the HTML directory index).
2. Indexes each contained document through the normal pipeline.
3. Aggregates the per-document labels into archive-level keywords and dominant
   research fields, stored in the index database.

Once an archive is fully processed it is labeled and becomes browsable in the
web UI, where other users can discover it and decide to **replicate** it (the
UI surfaces the archive CID and an `ipfs pin add <cid>` hint; performing the
replication is left to the operator's own IPFS node).

A pasted CID is auto-classified: a directory is treated as an archive, anything
else is indexed as a single document. Each archive may carry an optional
free-text **owner** label submitted alongside the CID (no authentication).

All indexing work runs through a single background queue, so archives and
documents can be submitted at any time, including while another run is already
in progress. New submissions are accepted immediately and processed in order
(the web UI shows the number of queued jobs).

Archives that are interrupted before completion (process restart mid-run) are
resumed automatically on the next startup. If the crawl phase had already
finished, the persisted document list is reused and the (expensive) re-crawl is
skipped, so only the indexing phase resumes and already-indexed documents are
skipped. Archives interrupted during the crawl itself are re-crawled from
scratch.

A document that keeps failing (e.g. a very large PDF that exceeds
`-convert-timeout`) is retried up to three times across runs; failure counts
are persisted, so a CID can't be retried forever and eventually settles as
**failed** instead of perpetually **pending**. Failed documents are listed in
the web UI with their last error, and can be re-queued individually (or all at
once) via the **Retry** buttons, which is useful after raising
`-convert-timeout`. If a retried document belongs to one or more archives, those
archives are re-run (crawl skipped) so its archive membership is restored once
it indexes.

Limitations: extremely large, HAMT-sharded directories are enumerated through
the gateway-rendered listing; the crawl is bounded by `-max-depth` and
`-max-docs`.

## Admin & moderation

A separate admin interface is served at `/admin`. Log in with the server's
configured API key (the same key used for indexing); on success the server
issues an in-memory session cookie. Sessions are not persisted, so a server
restart requires logging in again. The login submits the key over the local
connection, so only expose the admin interface on a trusted network.

The admin can:

- **Toggle review mode.** When review mode is OFF (the default), submitted CIDs
  are classified and indexed immediately, as before. When it is ON, any user may
  still submit a CID, but it is parked in a review queue (kept out of the
  archives list and not indexed) until an admin decides.
- **Approve / deny submissions.** The `/admin` page lists pending submissions,
  each labeled as a **document** or an **archive** and linked to the gateway for
  inspection. **Allow** queues it for indexing; **Deny** denylists the CID and
  removes it from the queue. Denylisted CIDs are rejected on future submission.
- **Remove content.** Remove an archive (its member documents that belong to no
  other archive are deleted from the index; documents shared with another
  archive are kept, only the membership link is dropped) or remove an individual
  document from the index by CID.

Moderation state is persisted across restarts in the index database: the
pending review queue, the denied-CID denylist, and the review-mode setting.

## Build

```sh
go build -o cidindexer-ipfs .
```

## Docker

A prebuilt image is published to GitHub Container Registry on every push to
`main` and on version tags (`v*`):

```sh
docker pull ghcr.io/gipplab/cid-indexer-ipfs:latest
```

The container persists all state under `/data` (the image runs with
`-o /data` by default) and listens on port `8384`. Provide the API key via the
`SAIA_API_KEY` environment variable, or mount a `.api_key` file into `/data`.

```sh
docker run -d --name cidindexer \
  -p 8384:8384 \
  -e SAIA_API_KEY="your-key" \
  -v cidindexer-data:/data \
  ghcr.io/gipplab/cid-indexer-ipfs:latest
```

Extra flags can be appended after the image name (they are passed straight to
the binary), e.g. `... :latest -gateway https://dweb.link -workers 12`.

### docker compose

```yaml
services:
  cidindexer:
    image: ghcr.io/gipplab/cid-indexer-ipfs:latest
    restart: unless-stopped
    ports:
      - "8384:8384"
    environment:
      SAIA_API_KEY: "${SAIA_API_KEY}"
    volumes:
      - cidindexer-data:/data

volumes:
  cidindexer-data:
```

Building the image locally instead of pulling it:

```sh
docker build -t cidindexer-ipfs .
```

## API key

An API key is required for indexing. The tool checks these locations in order:

1. `.api_key` file in the data directory (`-o`, defaults to `./data`).
2. `.api_key` file in the current working directory.
3. `SAIA_API_KEY` environment variable.

## Usage

The tool starts a web UI on port 8384. Document and archive CIDs are submitted
through the UI, which classifies each one and queues it for indexing.

```sh
# Start the web UI (search, submit document/archive CIDs, monitor progress):
./cidindexer-ipfs

# Custom output directory and port:
./cidindexer-ipfs -o ./index-data -port 9000
```

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-o` | `data` | Data directory for the index, failures, archives, and moderation state (created if missing) |
| `-gateway` | `https://ipfs.io` | IPFS gateway base URL |
| `-workers` | `8` | Number of concurrent processing workers |
| `-convert-rps` | `2` | Max PDF-convert requests per second (strict endpoint) |
| `-rps` | `4` | Max keyword-extraction (chat) requests per second |
| `-max-text` | `16000` | Max chars of document text sent to the LLM |
| `-convert-timeout` | `180s` | HTTP timeout for a single PDF-convert request |
| `-model` | `qwen3-30b-a3b-instruct-2507` | LLM model for keyword extraction |
| `-api-base` | `https://chat-ai.academiccloud.de/v1` | OpenAI-compatible API base URL |
| `-spacing` | `100ms` | Minimum delay between dispatching CIDs |
| `-temp` | `0.2` | Sampling temperature for keyword extraction |
| `-max-depth` | `8` | Max directory recursion depth when crawling an archive |
| `-max-docs` | `5000` | Max documents to discover per archive crawl |
| `-port` | `8384` | Web UI port |

## Web UI

- Keyword search with AND logic for multiple terms
- Autocomplete suggestions
- Clickable research field, sub-topic, and keyword tags
- Paginated results (20 per page)
- Recent searches
- Browse archives grid (aggregated labels, owner, status) with topic filtering
- Archive detail view listing the contained documents
- Replicate action (copies the archive CID + shows a pin command)
- Single CID paste field (auto-classified as document or archive)
- Live indexing progress

## Storage

All state is kept in a single embedded SQLite database, `index.db`, created in
the data directory (`-o`, defaults to `./data`). Alongside it SQLite maintains the usual
write-ahead-log sidecar files (`index.db-wal`, `index.db-shm`). The database
holds:

| Table | Contents |
|-------|----------|
| `documents` + `documents_fts` | Indexed metadata per CID and its FTS5 full-text index |
| `labels` | Keyword/field/sub-topic → document mapping (powers suggestions) |
| `archives` + `archive_docs` | Archives with aggregated labels and their member documents |
| `failures` | Permanently failed CIDs with error details |
| `submissions` | CIDs awaiting admin review (when review mode is on) |
| `denylist` | CIDs denied by an admin; rejected on submission |
| `settings` | Admin-configurable settings (review-mode toggle) |

The database is the only file that needs to be backed up or mounted to persist
state. Inspect it with the standard `sqlite3` CLI, e.g.:

```sh
sqlite3 data/index.db 'SELECT cid, title FROM documents LIMIT 5;'
```

## Integration with D-LOCKSS

The D-LOCKSS monitor can export payload CIDs via its dashboard
(`/api/payload-cids`). Submit those CIDs through the indexer's web UI to index
them. The indexed metadata now lives in `index.db` rather than a
`keyword_index.json` file, so wiring it into the monitor dashboard requires
exporting from the database (e.g. via `sqlite3`) into whatever format the
monitor expects.
