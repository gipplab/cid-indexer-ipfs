package main

import (
	"fmt"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	return NewStore(t.TempDir())
}

func mkEntry(cid, title, field, sub string, kws ...string) *IndexEntry {
	return &IndexEntry{
		CID:        cid,
		Title:      title,
		BroadField: field,
		SubTopic:   sub,
		Keywords:   kws,
		IndexedAt:  time.Now(),
	}
}

func TestSearchPagePagination(t *testing.T) {
	s := newTestStore(t)
	for i := 0; i < 25; i++ {
		s.Add(mkEntry(fmt.Sprintf("doc%02d", i), fmt.Sprintf("Document %d", i), "Computer Science", "Machine Learning", "machine", "learning"))
	}

	if got := s.Search("machine"); len(got) != 25 {
		t.Fatalf("Search(machine) = %d results, want 25", len(got))
	}

	page1, total := s.SearchPage("machine", 0, 10)
	if total != 25 {
		t.Fatalf("page1 total = %d, want 25", total)
	}
	if len(page1) != 10 {
		t.Fatalf("page1 len = %d, want 10", len(page1))
	}

	lastPage, total := s.SearchPage("machine", 20, 10)
	if total != 25 {
		t.Fatalf("lastPage total = %d, want 25", total)
	}
	if len(lastPage) != 5 {
		t.Fatalf("lastPage len = %d, want 5", len(lastPage))
	}

	if empty, _ := s.SearchPage("nonexistentterm", 0, 10); len(empty) != 0 {
		t.Fatalf("search for missing term returned %d results", len(empty))
	}
}

func TestSearchAndSemantics(t *testing.T) {
	s := newTestStore(t)
	s.Add(mkEntry("a", "Alpha paper", "Physics", "Optics", "alpha", "shared"))
	s.Add(mkEntry("b", "Beta paper", "Biology", "Genetics", "beta", "shared"))

	if _, total := s.SearchPage("shared", 0, 10); total != 2 {
		t.Fatalf("search 'shared' total = %d, want 2", total)
	}
	if _, total := s.SearchPage("alpha shared", 0, 10); total != 1 {
		t.Fatalf("search 'alpha shared' total = %d, want 1", total)
	}
}

func TestSuggest(t *testing.T) {
	s := newTestStore(t)
	s.Add(mkEntry("a", "A", "Computer Science", "Machine Learning", "machine learning", "neural networks"))
	s.Add(mkEntry("b", "B", "Computer Science", "Machine Learning", "machine learning"))

	suggestions := s.Suggest("machine")
	found := false
	for _, sug := range suggestions {
		if sug.Keyword == "machine learning" {
			found = true
			if sug.CIDCount != 2 {
				t.Fatalf("'machine learning' count = %d, want 2", sug.CIDCount)
			}
		}
	}
	if !found {
		t.Fatalf("expected 'machine learning' in suggestions, got %+v", suggestions)
	}
}

func TestArchiveLifecycleAndMembership(t *testing.T) {
	s := newTestStore(t)
	docs := []string{"d1", "d2", "d3"}

	s.AddArchive("arch", "alice")
	s.SetArchiveDocs("arch", docs)
	for _, dc := range docs {
		s.Add(mkEntry(dc, "Title "+dc, "Computer Science", "Systems", "ipfs"))
	}
	s.FinalizeArchive("arch")

	a, members, ok := s.GetArchive("arch")
	if !ok {
		t.Fatal("GetArchive(arch) not found")
	}
	if a.Indexed != 3 || a.DocCount != 3 || a.Status != archiveDone {
		t.Fatalf("archive state = indexed:%d count:%d status:%s, want 3/3/done", a.Indexed, a.DocCount, a.Status)
	}
	if len(members) != 3 {
		t.Fatalf("archive members = %d, want 3", len(members))
	}

	if got := s.ArchivesContaining("d1"); len(got) != 1 || got[0] != "arch" {
		t.Fatalf("ArchivesContaining(d1) = %v, want [arch]", got)
	}

	// Search results should carry archive membership.
	res, _ := s.SearchPage("ipfs", 0, 10)
	if len(res) != 3 {
		t.Fatalf("search 'ipfs' = %d, want 3", len(res))
	}
	for _, e := range res {
		if len(e.Archives) != 1 || e.Archives[0] != "arch" {
			t.Fatalf("entry %s archives = %v, want [arch]", e.CID, e.Archives)
		}
		refs := s.ArchiveRefs(e.Archives)
		if len(refs) != 1 || refs[0].CID != "arch" {
			t.Fatalf("entry %s refs = %+v, want one ref to arch", e.CID, refs)
		}
	}
}

func TestDeleteArchiveKeepsSharedDocs(t *testing.T) {
	s := newTestStore(t)

	s.AddArchive("A", "")
	s.SetArchiveDocs("A", []string{"d1", "d2", "d3"})
	s.AddArchive("B", "")
	s.SetArchiveDocs("B", []string{"d2", "d4"})
	for _, dc := range []string{"d1", "d2", "d3", "d4"} {
		s.Add(mkEntry(dc, "Title "+dc, "Field", "Sub", "kw"))
	}
	s.FinalizeArchive("A")
	s.FinalizeArchive("B")

	if !s.DeleteArchive("A") {
		t.Fatal("DeleteArchive(A) returned false")
	}
	if _, _, ok := s.GetArchive("A"); ok {
		t.Fatal("archive A still present after delete")
	}

	// d1 and d3 were unique to A -> deleted; d2 shared with B -> kept.
	if s.DeleteDocument("d1") {
		t.Fatal("d1 should have been deleted with archive A")
	}
	if got := s.ArchivesContaining("d2"); len(got) != 1 || got[0] != "B" {
		t.Fatalf("ArchivesContaining(d2) = %v, want [B]", got)
	}
	res, total := s.SearchPage("kw", 0, 10)
	if total != 2 {
		t.Fatalf("remaining docs after delete = %d, want 2 (d2,d4)", total)
	}
	_ = res
}

func TestFailuresAndRetry(t *testing.T) {
	s := newTestStore(t)
	for i := 0; i < maxRetries; i++ {
		s.RecordFailure("bad", "boom")
	}
	failures := s.Failures()
	if len(failures) != 1 || failures[0].CID != "bad" {
		t.Fatalf("Failures = %+v, want one perma-failure 'bad'", failures)
	}
	if !s.RetryFailure("bad") {
		t.Fatal("RetryFailure(bad) returned false")
	}
	if len(s.Failures()) != 0 {
		t.Fatal("failure record should be cleared after retry")
	}
}
