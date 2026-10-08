package main

import (
	"database/sql"
	"log/slog"
	"sort"
	"time"
)

const (
	dnPending  = "pending"
	dnCrawling = "crawling"
	dnReady    = "ready"
	dnFailed   = "failed"
)

// DatasetRef is one dataset advertised by the datanetwork readout.
type DatasetRef struct {
	CID      string
	Name     string
	Clusters []string
}

// NetworkFile is one UnixFS entry inside a dataset.
type NetworkFile struct {
	Path string `json:"path"`
	Name string `json:"name"`
	CID  string `json:"cid"`
	Size int64  `json:"size"`
	Dir  bool   `json:"dir"`
}

// NetworkDataset is a datanetwork dataset and the files found inside it.
type NetworkDataset struct {
	CID       string        `json:"cid"`
	Name      string        `json:"name"`
	Clusters  []string      `json:"clusters"`
	Status    string        `json:"status"`
	Error     string        `json:"error,omitempty"`
	FileCount int           `json:"file_count"`
	Files     []NetworkFile `json:"files"`
	SyncedAt  time.Time     `json:"synced_at,omitempty"`
}

// UpsertDatasets records the current readout. Datasets that are not ready are
// returned so the caller can walk them. Datasets absent from found are removed.
func (s *Store) UpsertDatasets(found []DatasetRef) []DatasetRef {
	byCID := make(map[string]DatasetRef, len(found))
	for _, d := range found {
		if d.CID == "" {
			continue
		}
		if prev, ok := byCID[d.CID]; ok {
			d.Clusters = mergeClusters(prev.Clusters, d.Clusters)
			if d.Name == "" {
				d.Name = prev.Name
			}
		}
		byCID[d.CID] = d
	}

	var walk []DatasetRef
	err := s.write(func(tx *sql.Tx) error {
		existing := map[string]string{}
		rows, err := tx.Query("SELECT cid, status FROM network_datasets")
		if err != nil {
			return err
		}
		for rows.Next() {
			var cid, status string
			if err := rows.Scan(&cid, &status); err != nil {
				rows.Close()
				return err
			}
			existing[cid] = status
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}

		for cid, d := range byCID {
			clusters := marshalJSON(normalizeClusters(d.Clusters))
			status, ok := existing[cid]
			if !ok {
				if _, err := tx.Exec(
					`INSERT INTO network_datasets(cid, name, clusters, status) VALUES(?,?,?,?)`,
					cid, d.Name, clusters, dnPending); err != nil {
					return err
				}
				status = dnPending
			} else if _, err := tx.Exec(
				`UPDATE network_datasets SET name=?, clusters=? WHERE cid=?`,
				d.Name, clusters, cid); err != nil {
				return err
			}
			if status != dnReady {
				walk = append(walk, d)
			}
		}
		for cid := range existing {
			if _, ok := byCID[cid]; ok {
				continue
			}
			if _, err := tx.Exec("DELETE FROM network_files WHERE dataset_cid=?", cid); err != nil {
				return err
			}
			if _, err := tx.Exec("DELETE FROM network_datasets WHERE cid=?", cid); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		slog.Error("upsert datasets failed", "error", err)
		return nil
	}
	sort.Slice(walk, func(i, j int) bool {
		if walk[i].Name == walk[j].Name {
			return walk[i].CID < walk[j].CID
		}
		return walk[i].Name < walk[j].Name
	})
	return walk
}

func (s *Store) MarkDatasetCrawling(cid string) {
	err := s.write(func(tx *sql.Tx) error {
		_, err := tx.Exec(
			`UPDATE network_datasets SET status=?, error='' WHERE cid=?`,
			dnCrawling, cid)
		return err
	})
	if err != nil {
		slog.Error("mark dataset crawling failed", "cid", cid, "error", err)
	}
}

// SaveDatasetFiles replaces a dataset's file list and marks it ready.
// note is kept when the listing was truncated.
func (s *Store) SaveDatasetFiles(cid string, files []NetworkFile, note string) {
	fileCount := 0
	for _, f := range files {
		if !f.Dir {
			fileCount++
		}
	}
	err := s.write(func(tx *sql.Tx) error {
		if _, err := tx.Exec("DELETE FROM network_files WHERE dataset_cid=?", cid); err != nil {
			return err
		}
		for _, f := range files {
			dir := 0
			if f.Dir {
				dir = 1
			}
			if _, err := tx.Exec(
				`INSERT INTO network_files(dataset_cid, path, name, cid, size, is_dir) VALUES(?,?,?,?,?,?)`,
				cid, f.Path, f.Name, f.CID, f.Size, dir); err != nil {
				return err
			}
		}
		_, err := tx.Exec(
			`UPDATE network_datasets SET status=?, error=?, file_count=?, synced_at=? WHERE cid=?`,
			dnReady, note, fileCount, nano(time.Now()), cid)
		return err
	})
	if err != nil {
		slog.Error("save dataset files failed", "cid", cid, "error", err)
	}
}

func (s *Store) MarkDatasetFailed(cid, reason string) {
	err := s.write(func(tx *sql.Tx) error {
		_, err := tx.Exec(
			`UPDATE network_datasets SET status=?, error=?, synced_at=? WHERE cid=?`,
			dnFailed, reason, nano(time.Now()), cid)
		return err
	})
	if err != nil {
		slog.Error("mark dataset failed", "cid", cid, "error", err)
	}
}

// Datanetwork returns every known dataset with its files.
func (s *Store) Datanetwork() []NetworkDataset {
	rows, err := s.db.Query(`
SELECT cid, name, clusters, status, error, file_count, synced_at
FROM network_datasets
ORDER BY name, cid`)
	if err != nil {
		slog.Error("list datasets failed", "error", err)
		return nil
	}
	defer rows.Close()

	var out []NetworkDataset
	for rows.Next() {
		var d NetworkDataset
		var clusters string
		var synced int64
		if err := rows.Scan(&d.CID, &d.Name, &clusters, &d.Status, &d.Error, &d.FileCount, &synced); err != nil {
			slog.Error("scan dataset failed", "error", err)
			return out
		}
		d.Clusters = parseStrings(clusters)
		d.SyncedAt = fromNano(synced)
		d.Files = s.datasetFiles(d.CID)
		out = append(out, d)
	}
	return out
}

// FileLabel is the catalog path or name for a file CID, used when indexing.
func (s *Store) FileLabel(cid string) string {
	var name, path string
	err := s.db.QueryRow(`
SELECT name, path FROM network_files
WHERE cid=? AND is_dir=0
ORDER BY path LIMIT 1`, cid).Scan(&name, &path)
	if err != nil {
		return ""
	}
	if path != "" {
		return path
	}
	return name
}

func (s *Store) datasetFiles(datasetCID string) []NetworkFile {
	rows, err := s.db.Query(`
SELECT path, name, cid, size, is_dir
FROM network_files
WHERE dataset_cid=?
ORDER BY path`, datasetCID)
	if err != nil {
		slog.Error("list dataset files failed", "error", err)
		return nil
	}
	defer rows.Close()
	var out []NetworkFile
	for rows.Next() {
		var f NetworkFile
		var dir int
		if err := rows.Scan(&f.Path, &f.Name, &f.CID, &f.Size, &dir); err != nil {
			slog.Error("scan dataset file failed", "error", err)
			return out
		}
		f.Dir = dir != 0
		out = append(out, f)
	}
	if out == nil {
		out = []NetworkFile{}
	}
	return out
}

func normalizeClusters(in []string) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, c := range in {
		if c == "" {
			continue
		}
		if _, ok := seen[c]; ok {
			continue
		}
		seen[c] = struct{}{}
		out = append(out, c)
	}
	sort.Strings(out)
	if out == nil {
		out = []string{}
	}
	return out
}

func mergeClusters(a, b []string) []string {
	return normalizeClusters(append(append([]string{}, a...), b...))
}
