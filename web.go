package main

import (
	"bytes"
	"embed"
	"log/slog"
	"sync"
	"text/template"
)

//go:embed web/*.html
var webFS embed.FS

var (
	webOnce sync.Once
	webTmpl = make(map[string]*template.Template)
)

func renderPage(name, gateway string) string {
	webOnce.Do(func() {
		for _, page := range []string{"dashboard", "admin"} {
			data, err := webFS.ReadFile("web/" + page + ".html")
			if err != nil {
				slog.Error("failed to load web template", "page", page, "error", err)
				continue
			}
			t, err := template.New(page).Parse(string(data))
			if err != nil {
				slog.Error("failed to parse web template", "page", page, "error", err)
				continue
			}
			webTmpl[page] = t
		}
	})

	t := webTmpl[name]
	if t == nil {
		return "<!DOCTYPE html><html><body><p>page not found</p></body></html>"
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, map[string]string{"Gateway": gateway}); err != nil {
		slog.Error("failed to render page", "page", name, "error", err)
		return "<!DOCTYPE html><html><body><p>render error</p></body></html>"
	}
	return buf.String()
}
