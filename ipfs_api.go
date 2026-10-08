package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func kuboCall(apiBase, cmd string, q url.Values, timeout time.Duration) (*http.Response, error) {
	u := strings.TrimRight(apiBase, "/") + "/api/v0/" + cmd
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequest(http.MethodPost, u, nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: timeout}
	return client.Do(req)
}

func readKuboErr(resp *http.Response) error {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	var e struct {
		Message string `json:"Message"`
	}
	if json.Unmarshal(b, &e) == nil && e.Message != "" {
		return errors.New(e.Message)
	}
	msg := strings.TrimSpace(string(b))
	if msg == "" {
		msg = resp.Status
	}
	return errors.New(msg)
}

func kuboIsDir(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "directory")
}

type kuboLink struct {
	Name string
	CID  string
	Size int64
	Dir  bool
}

func kuboTypeDir(raw json.RawMessage) bool {
	s := strings.ToLower(strings.Trim(string(raw), `"`))
	return s == "1" || s == "directory" || s == "dir"
}

func kuboLS(api, ipfsPath string, timeout time.Duration) ([]kuboLink, error) {
	q := url.Values{}
	q.Set("arg", ipfsPath)
	resp, err := kuboCall(api, "ls", q, timeout)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, readKuboErr(resp)
	}
	var parsed struct {
		Objects []struct {
			Links []struct {
				Name string          `json:"Name"`
				Hash string          `json:"Hash"`
				Size int64           `json:"Size"`
				Type json.RawMessage `json:"Type"`
			} `json:"Links"`
		} `json:"Objects"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, err
	}
	if len(parsed.Objects) == 0 {
		return nil, nil
	}
	out := make([]kuboLink, 0, len(parsed.Objects[0].Links))
	for _, l := range parsed.Objects[0].Links {
		if l.Hash == "" {
			continue
		}
		out = append(out, kuboLink{Name: l.Name, CID: l.Hash, Size: l.Size, Dir: kuboTypeDir(l.Type)})
	}
	return out, nil
}

// kuboCat reads a UnixFS file. n > 0 limits the bytes requested from Kubo.
// n == 0 reads up to maxFetchSize+1.
func kuboCat(api, ipfsPath string, n int64, timeout time.Duration) ([]byte, error) {
	rc, err := kuboCatStream(api, ipfsPath, n, timeout)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	limit := int64(maxFetchSize + 1)
	if n > 0 && n < limit {
		limit = n
	}
	return io.ReadAll(io.LimitReader(rc, limit))
}

func kuboCatStream(api, ipfsPath string, n int64, timeout time.Duration) (io.ReadCloser, error) {
	q := url.Values{}
	q.Set("arg", ipfsPath)
	if n > 0 {
		q.Set("length", strconv.FormatInt(n, 10))
	}
	resp, err := kuboCall(api, "cat", q, timeout)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		err := readKuboErr(resp)
		resp.Body.Close()
		return nil, err
	}
	return resp.Body, nil
}

func ipfsPathHref(base, name string) string {
	var b strings.Builder
	b.WriteString("/ipfs/")
	first := true
	for _, seg := range strings.Split(base, "/") {
		if seg == "" {
			continue
		}
		if !first {
			b.WriteByte('/')
		}
		first = false
		b.WriteString(url.PathEscape(seg))
	}
	if !first {
		b.WriteByte('/')
	}
	b.WriteString(url.PathEscape(name))
	return b.String()
}

func writeKuboDir(w http.ResponseWriter, base string, links []kuboLink) {
	var b strings.Builder
	b.WriteString("<!DOCTYPE html><html><body><ul>")
	base = strings.Trim(base, "/")
	for _, l := range links {
		if l.Name == "" || l.Name == "." || l.Name == ".." || strings.ContainsAny(l.Name, "/\\") {
			continue
		}
		fmt.Fprintf(&b, `<li><a href="%s">%s</a></li>`, ipfsPathHref(base, l.Name), html.EscapeString(l.Name))
	}
	b.WriteString("</ul></body></html>")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(b.String()))
}
