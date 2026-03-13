package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultModel   = "llama-3.3-70b-instruct"
	defaultAPIBase = "https://chat-ai.academiccloud.de/v1"

	maxFetchSize = 20 * 1024 * 1024 // 20 MB
	maxTextLen   = 100_000

	fetchTimeout   = 90 * time.Second
	convertTimeout = 60 * time.Second
	llmTimeout     = 120 * time.Second

	maxRateLimitRetries = 5
	baseBackoff         = 2 * time.Second

	keywordPrompt = `Analyze the following academic document text. Extract the following information and return ONLY a valid JSON object with these fields:
- "title": the document title (string)
- "broad_field": the broad academic/research field (string, e.g. "Computer Science", "Biology", "Physics", "Economics")
- "sub_topic": the sub-topic within that field (string, e.g. "Machine Learning", "Genomics", "Quantum Computing")
- "research_niche": the specific research niche (string, e.g. "Transformer Architectures for NLP", "CRISPR Gene Editing in Plants")
- "keywords": the 10 most important keywords or key phrases (array of exactly 10 lowercase strings)
Example: {"title":"Attention Is All You Need","broad_field":"Computer Science","sub_topic":"Machine Learning","research_niche":"Transformer Architectures for Sequence Modeling","keywords":["transformer","attention mechanism","self-attention","neural networks","sequence modeling","encoder-decoder","natural language processing","deep learning","machine translation","positional encoding"]}

Document text:
`
)

// Pipeline fetches PDFs from IPFS, converts them to markdown, and extracts
// structured metadata via an OpenAI-compatible LLM API.
//
// All workers share a single rate limiter so concurrent requests stay within
// the API's rate window.
type Pipeline struct {
	APIKey  string
	APIBase string
	Model   string
	Gateway string

	limiterOnce sync.Once
	limiter     chan struct{}
}

func (p *Pipeline) initLimiter() {
	p.limiterOnce.Do(func() {
		p.limiter = make(chan struct{}, 1)
		go func() {
			// One token per second — shared across all workers.
			tick := time.NewTicker(1 * time.Second)
			defer tick.Stop()
			for range tick.C {
				select {
				case p.limiter <- struct{}{}:
				default:
				}
			}
		}()
	})
}

// acquireToken blocks until the shared rate limiter grants a slot.
func (p *Pipeline) acquireToken() {
	p.initLimiter()
	<-p.limiter
}

// Process fetches the CID from IPFS, converts the PDF to markdown, and
// extracts keywords. Returns nil (no error) for non-PDF content.
func (p *Pipeline) Process(cid string) (*IndexEntry, error) {
	data, isPDF, err := p.fetchFromIPFS(cid)
	if err != nil {
		return nil, fmt.Errorf("fetch: %w", err)
	}
	if !isPDF {
		slog.Debug("skipping non-PDF content", "cid", cid)
		return nil, nil
	}

	slog.Info("converting PDF", "cid", cid, "bytes", len(data))
	markdown, err := p.convertPDFWithRetry(data, cid)
	if err != nil {
		return nil, fmt.Errorf("convert: %w", err)
	}

	slog.Info("extracting keywords", "cid", cid, "markdown_len", len(markdown))
	result, err := p.extractKeywordsWithRetry(markdown, cid)
	if err != nil {
		return nil, fmt.Errorf("extract: %w", err)
	}

	return &IndexEntry{
		CID:           cid,
		Title:         result.Title,
		BroadField:    result.BroadField,
		SubTopic:      result.SubTopic,
		ResearchNiche: result.ResearchNiche,
		Keywords:      result.Keywords,
		IndexedAt:     time.Now(),
	}, nil
}

func (p *Pipeline) fetchFromIPFS(cid string) ([]byte, bool, error) {
	reqURL := p.Gateway + "/ipfs/" + url.PathEscape(cid)
	client := &http.Client{Timeout: fetchTimeout}
	resp, err := client.Get(reqURL)
	if err != nil {
		return nil, false, fmt.Errorf("gateway request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, false, fmt.Errorf("gateway returned %s", resp.Status)
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxFetchSize+1))
	if err != nil {
		return nil, false, fmt.Errorf("read body: %w", err)
	}
	if len(data) > maxFetchSize {
		return nil, false, fmt.Errorf("file exceeds %d MB limit", maxFetchSize/(1024*1024))
	}

	isPDF := len(data) >= 4 && string(data[:4]) == "%PDF"
	return data, isPDF, nil
}

func (p *Pipeline) convertPDFWithRetry(pdfData []byte, cid string) (string, error) {
	for attempt := 0; attempt <= maxRateLimitRetries; attempt++ {
		p.acquireToken()
		markdown, retryAfter, err := p.convertPDF(pdfData)
		if err == nil {
			return markdown, nil
		}
		if retryAfter == 0 {
			return "", err
		}
		wait := backoffDuration(attempt, retryAfter)
		slog.Warn("rate limited on convert, backing off", "cid", cid, "attempt", attempt+1, "wait", wait)
		time.Sleep(wait)
	}
	return "", fmt.Errorf("rate limited after %d retries", maxRateLimitRetries)
}

// convertPDF returns (markdown, retryAfterSeconds, error).
// retryAfter > 0 signals a 429 that should be retried.
func (p *Pipeline) convertPDF(pdfData []byte) (string, time.Duration, error) {
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	part, err := writer.CreateFormFile("document", "paper.pdf")
	if err != nil {
		return "", 0, fmt.Errorf("create form: %w", err)
	}
	if _, err := part.Write(pdfData); err != nil {
		return "", 0, fmt.Errorf("write form: %w", err)
	}
	writer.Close()

	req, err := http.NewRequest("POST", p.APIBase+"/documents/convert", &buf)
	if err != nil {
		return "", 0, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+p.APIKey)
	req.Header.Set("Accept", "application/json")

	client := &http.Client{Timeout: convertTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("HTTP: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", 0, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode == 429 {
		return "", parseRetryAfter(resp), fmt.Errorf("rate limited (429)")
	}
	if resp.StatusCode != http.StatusOK {
		return "", 0, fmt.Errorf("status %d: %s", resp.StatusCode, truncate(string(body), 200))
	}

	var cr struct {
		Markdown string `json:"markdown"`
	}
	if err := json.Unmarshal(body, &cr); err != nil {
		return "", 0, fmt.Errorf("parse response: %w", err)
	}

	markdown := strings.TrimSpace(cr.Markdown)
	if markdown == "" {
		return "", 0, fmt.Errorf("empty markdown from conversion")
	}
	if len(markdown) > maxTextLen {
		markdown = markdown[:maxTextLen]
	}
	return markdown, 0, nil
}

func (p *Pipeline) extractKeywordsWithRetry(markdown, cid string) (*extractionResult, error) {
	for attempt := 0; attempt <= maxRateLimitRetries; attempt++ {
		p.acquireToken()
		result, retryAfter, err := p.extractKeywords(markdown)
		if err == nil {
			return result, nil
		}
		if retryAfter == 0 {
			return nil, err
		}
		wait := backoffDuration(attempt, retryAfter)
		slog.Warn("rate limited on extract, backing off", "cid", cid, "attempt", attempt+1, "wait", wait)
		time.Sleep(wait)
	}
	return nil, fmt.Errorf("rate limited after %d retries", maxRateLimitRetries)
}

// extractKeywords returns (result, retryAfter, error).
func (p *Pipeline) extractKeywords(markdown string) (*extractionResult, time.Duration, error) {
	temp := 0.1
	reqBody := chatRequest{
		Model: p.Model,
		Messages: []chatMessage{{
			Role:    "user",
			Content: keywordPrompt + markdown,
		}},
		Temperature: &temp,
		MaxTokens:   512,
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", p.APIBase+"/chat/completions", bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, 0, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+p.APIKey)

	client := &http.Client{Timeout: llmTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("HTTP: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode == 429 {
		return nil, parseRetryAfter(resp), fmt.Errorf("rate limited (429)")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, 0, fmt.Errorf("API status %d: %s", resp.StatusCode, truncate(string(respBody), 200))
	}

	var chatResp chatResponse
	if err := json.Unmarshal(respBody, &chatResp); err != nil {
		return nil, 0, fmt.Errorf("parse response: %w", err)
	}
	if chatResp.Error != nil {
		return nil, 0, fmt.Errorf("API error: %s", chatResp.Error.Message)
	}
	if len(chatResp.Choices) == 0 || chatResp.Choices[0].Message.Content == "" {
		return nil, 0, fmt.Errorf("empty LLM response")
	}

	result, err := parseExtraction(chatResp.Choices[0].Message.Content)
	return result, 0, err
}

// parseRetryAfter reads the Retry-After or ratelimit-reset header.
// Returns at least 1s so the caller always has a usable duration.
func parseRetryAfter(resp *http.Response) time.Duration {
	for _, hdr := range []string{"Retry-After", "retry-after", "ratelimit-reset"} {
		if v := resp.Header.Get(hdr); v != "" {
			if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
				return time.Duration(secs) * time.Second
			}
		}
	}
	return 1 * time.Second
}

// backoffDuration returns an exponential backoff, respecting the server's
// Retry-After hint as a floor.
func backoffDuration(attempt int, serverHint time.Duration) time.Duration {
	exp := baseBackoff
	for i := 0; i < attempt; i++ {
		exp *= 2
	}
	if exp > 60*time.Second {
		exp = 60 * time.Second
	}
	if serverHint > exp {
		return serverHint
	}
	return exp
}

func parseExtraction(raw string) (*extractionResult, error) {
	text := strings.TrimSpace(raw)
	text = strings.TrimPrefix(text, "```json")
	text = strings.TrimPrefix(text, "```")
	text = strings.TrimSuffix(text, "```")
	text = strings.TrimSpace(text)

	start := strings.Index(text, "{")
	end := strings.LastIndex(text, "}")
	if start == -1 || end == -1 || end <= start {
		return nil, fmt.Errorf("no JSON object in response")
	}

	var result extractionResult
	if err := json.Unmarshal([]byte(text[start:end+1]), &result); err != nil {
		return nil, fmt.Errorf("parse JSON: %w", err)
	}

	result.Title = strings.TrimSpace(result.Title)
	result.BroadField = strings.TrimSpace(result.BroadField)
	result.SubTopic = strings.TrimSpace(result.SubTopic)
	result.ResearchNiche = strings.TrimSpace(result.ResearchNiche)

	cleaned := make([]string, 0, 10)
	for _, kw := range result.Keywords {
		if kw = strings.TrimSpace(kw); kw != "" {
			cleaned = append(cleaned, strings.ToLower(kw))
			if len(cleaned) == 10 {
				break
			}
		}
	}
	if len(cleaned) == 0 {
		return nil, fmt.Errorf("no keywords extracted")
	}
	result.Keywords = cleaned
	return &result, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// OpenAI-compatible types

type chatRequest struct {
	Model       string        `json:"model"`
	Messages    []chatMessage `json:"messages"`
	Temperature *float64      `json:"temperature,omitempty"`
	MaxTokens   int           `json:"max_tokens,omitempty"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatResponse struct {
	Choices []struct {
		Message chatMessage `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

type extractionResult struct {
	Title         string   `json:"title"`
	BroadField    string   `json:"broad_field"`
	SubTopic      string   `json:"sub_topic"`
	ResearchNiche string   `json:"research_niche"`
	Keywords      []string `json:"keywords"`
}
