package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestParseDatanetworkPage(t *testing.T) {
	const body = `
<h2 class="font-mono">iosp-nodes</h2>
<span class="text-sm text-ink">genesis</span><span class="font-mono text-xs text-ink-mute">QmZHJCiSjH<!-- -->…</span>
<span class="text-sm text-ink">catchup-test</span><span class="font-mono text-xs text-ink-mute">QmaaDUFxax<!-- -->…</span>
<h2 class="font-mono">iosp-laptops</h2>
<span class="text-sm text-ink">genesis</span><span class="font-mono text-xs text-ink-mute">QmZHJCiSjH<!-- -->…</span>
<script>QmZHJCiSjHJgLihXM9EpC1RswkMptueuTnuf6LA5KmXyNj QmaaDUFxaxDwS8eF8icZP76hZfgvLFMBozqTtT6k1vwvv9</script>
`
	got := parseDatanetworkPage(body)
	if len(got) != 2 {
		t.Fatalf("datasets = %d, want 2 (%v)", len(got), got)
	}
	if got[0].Name != "genesis" || got[0].CID != "QmZHJCiSjHJgLihXM9EpC1RswkMptueuTnuf6LA5KmXyNj" {
		t.Fatalf("first dataset = %+v", got[0])
	}
	if strings.Join(got[0].Clusters, ",") != "iosp-laptops,iosp-nodes" {
		t.Fatalf("genesis clusters = %v", got[0].Clusters)
	}
	if got[1].Name != "catchup-test" || len(got[1].Clusters) != 1 || got[1].Clusters[0] != "iosp-nodes" {
		t.Fatalf("second dataset = %+v", got[1])
	}
}

func TestAsText(t *testing.T) {
	if text, ok := asText([]byte("station,level\nA,1\n")); !ok || text == "" {
		t.Fatal("csv should be text")
	}
	if _, ok := asText([]byte{'g', 'i', 'f', 0, 1, 2}); ok {
		t.Fatal("binary should not be text")
	}
	if _, ok := asText(nil); !ok {
		t.Fatal("empty file should be text")
	}
}

func TestDatanetworkStoreRoundTrip(t *testing.T) {
	s := newTestStore(t)
	walk := s.UpsertDatasets([]DatasetRef{
		{CID: "QmDataset", Name: "genesis", Clusters: []string{"iosp-nodes"}},
	})
	if len(walk) != 1 {
		t.Fatalf("walk = %d, want 1", len(walk))
	}
	s.SaveDatasetFiles("QmDataset", []NetworkFile{
		{Path: "notes.txt", Name: "notes.txt", CID: "QmFile", Size: 12},
	}, "")
	if !s.IndexedCID("QmDataset") || !s.IndexedCID("QmFile") {
		t.Fatal("dataset and file CIDs should be servable")
	}
	got := s.Datanetwork()
	if len(got) != 1 || got[0].FileCount != 1 || got[0].Status != dnReady {
		t.Fatalf("catalog = %+v", got)
	}
	if len(got[0].Files) != 1 || got[0].Files[0].Path != "notes.txt" {
		t.Fatalf("files = %+v", got[0].Files)
	}

	again := s.UpsertDatasets([]DatasetRef{
		{CID: "QmDataset", Name: "genesis", Clusters: []string{"iosp-nodes", "iosp-laptops"}},
	})
	if len(again) != 0 {
		t.Fatalf("ready dataset was queued again: %+v", again)
	}
	if clusters := s.Datanetwork()[0].Clusters; strings.Join(clusters, ",") != "iosp-laptops,iosp-nodes" {
		t.Fatalf("clusters = %v", clusters)
	}

	if left := s.UpsertDatasets(nil); len(left) != 0 || len(s.Datanetwork()) != 0 {
		t.Fatalf("removed dataset still present: %v", s.Datanetwork())
	}
	if s.IndexedCID("QmFile") {
		t.Fatal("file CID should leave the index with its dataset")
	}
}

func TestWalkUnixFS(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/ls"):
			switch r.URL.Query().Get("arg") {
			case "QmRoot":
				fmt.Fprint(w, `{"Objects":[{"Links":[
					{"Name":"readme.txt","Hash":"QmFile","Size":12,"Type":2},
					{"Name":"nested","Hash":"QmDir","Size":40,"Type":1}
				]}]}`)
			case "QmDir":
				fmt.Fprint(w, `{"Objects":[{"Links":[
					{"Name":"gauge.csv","Hash":"QmCSV","Size":99,"Type":2}
				]}]}`)
			case "QmOnly":
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprint(w, `{"Message":"this dag node is not a directory"}`)
			default:
				http.NotFound(w, r)
			}
		case strings.HasSuffix(r.URL.Path, "/files/stat"):
			arg := r.URL.Query().Get("arg")
			if strings.Contains(arg, "QmOnly") {
				fmt.Fprint(w, `{"Size":7,"Type":"file"}`)
				return
			}
			fmt.Fprint(w, `{"Size":0,"Type":"directory"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	files, truncated, err := walkUnixFS(srv.URL, "QmRoot", 4, 10)
	if err != nil {
		t.Fatal(err)
	}
	if truncated || len(files) != 2 {
		t.Fatalf("files=%d truncated=%v (%+v)", len(files), truncated, files)
	}
	if files[0].Path != "readme.txt" || files[0].Size != 12 || files[0].Dir {
		t.Fatalf("readme = %+v", files[0])
	}
	if files[1].Path != "nested/gauge.csv" || files[1].CID != "QmCSV" || files[1].Size != 99 {
		t.Fatalf("csv = %+v", files[1])
	}

	one, _, err := walkUnixFS(srv.URL, "QmOnly", 4, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(one) != 1 || one[0].Path != "" || one[0].Size != 7 || one[0].Dir {
		t.Fatalf("single file = %+v", one)
	}
}
