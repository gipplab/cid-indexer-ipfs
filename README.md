# cidindexer-ipfs

Extracts structured metadata and keywords from PDF documents stored on IPFS,
using an OpenAI-compatible LLM API. Includes a built-in web UI for searching
and managing the index.

Given a list of IPFS CIDs pointing to PDF files, the tool:

1. Fetches each document from an IPFS gateway.
2. Converts the PDF to markdown via the API's document conversion endpoint.
3. Sends the markdown to an LLM to extract title, research field, topic, niche,
   and the 10 most relevant keywords.
4. Persists the results to `keyword_index.json`.

Processing is incremental — already indexed CIDs are skipped on subsequent runs.
Permanently failed CIDs (after 3 retries) are tracked in `keyword_failures.json`.
All submitted CIDs are accumulated in a persistent `cids.txt`, so indexing
resumes automatically after a restart.

## Build

```sh
go build -o cidindexer-ipfs .
```

## API key

An API key is required for indexing. The tool checks these locations in order:

1. `.api_key` file in the output directory (`-o`).
2. `.api_key` file in the current working directory.
3. `SAIA_API_KEY` environment variable.

## Usage

By default the tool starts a web UI on port 8384. CIDs can be uploaded through
the UI or provided as a file argument.

```sh
# Start the web UI (search, upload CIDs, monitor progress):
./cidindexer-ipfs

# Start the web UI and begin indexing a CID file in the background:
./cidindexer-ipfs payload_cids.txt

# Index from a file without starting the web UI:
./cidindexer-ipfs -cli payload_cids.txt

# Pipe from stdin:
cat payload_cids.txt | ./cidindexer-ipfs -cli -

# Custom output directory and port:
./cidindexer-ipfs -o ./index-data -port 9000
```

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-o` | `.` | Output directory for index, failure, and CID list files |
| `-gateway` | `https://ipfs.io` | IPFS gateway base URL |
| `-workers` | `4` | Number of concurrent processing workers |
| `-model` | `llama-3.3-70b-instruct` | LLM model for keyword extraction |
| `-api-base` | `https://chat-ai.academiccloud.de/v1` | OpenAI-compatible API base URL |
| `-spacing` | `100ms` | Minimum delay between dispatching CIDs |
| `-cli` | `false` | Index CIDs and exit (no web UI) |
| `-port` | `8384` | Web UI port |

## Web UI

- Keyword search with AND logic for multiple terms
- Autocomplete suggestions
- Clickable research field, sub-topic, niche, and keyword tags
- Paginated results (20 per page)
- Recent searches
- Single CID paste field and bulk file upload
- CID list download
- Live indexing progress

## Output files

| File | Description |
|------|-------------|
| `keyword_index.json` | Indexed metadata keyed by CID |
| `keyword_failures.json` | Permanently failed CIDs with error details |
| `cids.txt` | Persistent list of all submitted CIDs |

Example entry in `keyword_index.json`:

```json
{
  "bafyrei...": {
    "cid": "bafyrei...",
    "title": "Attention Is All You Need",
    "broad_field": "Computer Science",
    "sub_topic": "Machine Learning",
    "research_niche": "Transformer Architectures for Sequence Modeling",
    "keywords": ["transformer", "attention mechanism", "..."],
    "indexed_at": "2026-03-04T14:30:00Z"
  }
}
```

## Integration with D-LOCKSS

The D-LOCKSS monitor can export a payload CID list via its dashboard
(`/api/payload-cids`). Feed this list to the indexer, then place the resulting
`keyword_index.json` in the monitor's data directory (`~/.dlockss-monitor/`)
to enable keyword search in the monitor dashboard.
