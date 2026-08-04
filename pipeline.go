package main

import (
	"bytes"
	"encoding/json"
	"errors"
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

// ErrRateLimited indicates a transient HTTP 429 that should be retried.
var ErrRateLimited = errors.New("rate limited")

const (
	defaultModel   = "qwen3.6-35b-a3b"
	defaultAPIBase = "https://chat-ai.academiccloud.de/v1"
	defaultTemp    = 0.2
	// Convert is capped at 1 concurrent request on the SAIA documents endpoint.
	defaultConvertRPS = 1
	defaultChatRPS    = 4

	maxFetchSize      = 20 * 1024 * 1024 // 20 MB
	defaultMaxTextLen = 16_000           // chars of markdown sent to the LLM for extraction

	fetchTimeout          = 90 * time.Second
	defaultConvertTimeout = 180 * time.Second // large PDFs can take a while to convert
	llmTimeout            = 120 * time.Second

	maxTransientRetries = 2 // extra attempts for 5xx / network hiccups
	baseBackoff         = 2 * time.Second
	maxBackoff          = 60 * time.Second // hard ceiling on a short-backoff sleep

	// Long reset windows mean the endpoint quota is exhausted; pause the pool
	// until reset rather than re-probing every minute.
	quotaPauseThreshold = 2 * time.Minute
	maxCooldown         = 24 * time.Hour

	keywordPrompt = `Analyze the following academic document text. Extract the following information and return ONLY a valid JSON object with these fields:
- "title": the document title (string)
- "broad_field": the broad academic/research field (string, e.g. "Computer Science", "Biology", "Physics", "Economics")
- "sub_topic": the sub-topic within that field (string, e.g. "Machine Learning", "Genomics", "Quantum Computing")
- "keywords": the 10 most important keywords or key phrases (array of exactly 10 lowercase strings)
Example: {"title":"Attention Is All You Need","broad_field":"Computer Science","sub_topic":"Machine Learning","keywords":["transformer","attention mechanism","self-attention","neural networks","sequence modeling","encoder-decoder","natural language processing","deep learning","machine translation","positional encoding"]}

Document text:
`
)

// retryClass categorizes a failed API call for the retry loop.
type retryClass int

const (
	retryNone retryClass = iota
	retryRateLimit
	retryTransient
)

// isServerError reports whether an HTTP status is a retryable 5xx.
func isServerError(code int) bool { return code >= 500 && code <= 599 }

// Pipeline fetches PDFs from IPFS, converts them to markdown, and extracts metadata via an LLM API.
type Pipeline struct {
	APIKey      string
	APIBase     string
	Model       string
	Gateway     string
	Temperature float64
	ConvertRPS  int           // /documents/convert requests per second (0 = default)
	ChatRPS     int           // /chat/completions requests per second (0 = default)
	MaxTextLen  int           // max chars of markdown sent to the LLM (0 = default)
	ConvertTO   time.Duration // PDF-convert HTTP timeout (0 = default)

	limiterOnce    sync.Once
	convertLimiter *rateLimiter
	chatLimiter    *rateLimiter

	// Log rate-limit headers once per endpoint to avoid flooding logs during 429 storms.
	convertRLOnce sync.Once
	chatRLOnce    sync.Once
}

// rateLimitHeaders summarizes rate-limit headers from a 429 response.
func rateLimitHeaders(resp *http.Response) string {
	keys := []string{
		"Retry-After",
		"RateLimit-Limit", "RateLimit-Remaining", "RateLimit-Reset",
		"X-RateLimit-Limit", "X-RateLimit-Remaining", "X-RateLimit-Reset",
		"X-Ratelimit-Limit-Requests", "X-Ratelimit-Remaining-Requests", "X-Ratelimit-Reset-Requests",
		"X-Ratelimit-Limit-Tokens", "X-Ratelimit-Remaining-Tokens", "X-Ratelimit-Reset-Tokens",
	}
	var parts []string
	for _, k := range keys {
		if v := resp.Header.Get(k); v != "" {
			parts = append(parts, k+"="+v)
		}
	}
	if len(parts) == 0 {
		return "(none)"
	}
	return strings.Join(parts, " ")
}

// rateLimiter is a shared worker gate with optional cooldown after 429s.
// When hold is set, acquire/release bracket an in-flight call (convert).
// Otherwise tokens refill at rps (chat).
type rateLimiter struct {
	tokens chan struct{}
	hold   bool

	mu         sync.Mutex
	blockUntil time.Time
}

// newRateLimiter issues rps tokens per second with a burst capacity.
func newRateLimiter(rps, burst int) *rateLimiter {
	if rps <= 0 {
		rps = 1
	}
	if burst < 1 {
		burst = 1
	}
	rl := &rateLimiter{tokens: make(chan struct{}, burst)}
	for i := 0; i < burst; i++ {
		rl.tokens <- struct{}{} // prime the burst
	}
	interval := time.Second / time.Duration(rps)
	go func() {
		tick := time.NewTicker(interval)
		defer tick.Stop()
		for range tick.C {
			select {
			case rl.tokens <- struct{}{}:
			default:
			}
		}
	}()
	return rl
}

// newConcurrencyLimiter caps in-flight operations at n.
func newConcurrencyLimiter(n int) *rateLimiter {
	if n < 1 {
		n = 1
	}
	rl := &rateLimiter{tokens: make(chan struct{}, n), hold: true}
	for i := 0; i < n; i++ {
		rl.tokens <- struct{}{}
	}
	return rl
}

// acquire blocks until any cooldown has elapsed and a token is available.
func (rl *rateLimiter) acquire() {
	for {
		rl.mu.Lock()
		wait := time.Until(rl.blockUntil)
		rl.mu.Unlock()
		if wait <= 0 {
			break
		}
		time.Sleep(wait)
	}
	<-rl.tokens
}

func (rl *rateLimiter) release() {
	if !rl.hold {
		return
	}
	select {
	case rl.tokens <- struct{}{}:
	default:
	}
}

// penalize pauses all workers for at least d after a rate-limit response.
func (rl *rateLimiter) penalize(d time.Duration) {
	if d <= 0 {
		return
	}
	until := time.Now().Add(d)
	rl.mu.Lock()
	if until.After(rl.blockUntil) {
		rl.blockUntil = until
	}
	rl.mu.Unlock()
}

func (p *Pipeline) initLimiters() {
	p.limiterOnce.Do(func() {
		convertRPS := p.ConvertRPS
		if convertRPS <= 0 {
			convertRPS = defaultConvertRPS
		}
		chatRPS := p.ChatRPS
		if chatRPS <= 0 {
			chatRPS = defaultChatRPS
		}
		p.convertLimiter = newConcurrencyLimiter(convertRPS)
		p.chatLimiter = newRateLimiter(chatRPS, 1)
	})
}

// Process fetches a CID from IPFS, converts PDF to markdown, and extracts keywords.
// Returns nil, nil for non-PDF content.
func (p *Pipeline) Process(cid string) (*IndexEntry, error) {
	data, isPDF, err := p.fetchFromIPFS(cid)
	if err != nil {
		return nil, fmt.Errorf("fetch: %w", err)
	}
	if !isPDF {
		slog.Debug("skipping non-PDF content", "cid", cid)
		return nil, nil
	}

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
		CID:        cid,
		Title:      result.Title,
		BroadField: result.BroadField,
		SubTopic:   result.SubTopic,
		Keywords:   result.Keywords,
		IndexedAt:  time.Now(),
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
	p.initLimiters()
	rateLimitTries, transientTries := 0, 0
	for {
		p.convertLimiter.acquire()
		slog.Info("converting PDF", "cid", cid, "bytes", len(pdfData))
		markdown, class, retryAfter, err := p.convertPDF(pdfData)
		p.convertLimiter.release()
		if err == nil {
			return markdown, nil
		}
		switch class {
		case retryRateLimit:
			// Long reset = quota window exhausted; pause the pool until then.
			if retryAfter > quotaPauseThreshold {
				pause := capCooldown(retryAfter)
				slog.Warn("convert quota exhausted, pausing pool until reset",
					"cid", cid, "pause", pause.Round(time.Second),
					"resume_at", time.Now().Add(pause).Format(time.RFC3339))
				p.convertLimiter.penalize(pause)
				continue
			}
			wait := backoffDuration(rateLimitTries, retryAfter)
			rateLimitTries++
			slog.Warn("rate limited on convert, backing off", "cid", cid, "attempt", rateLimitTries, "wait", wait)
			p.convertLimiter.penalize(wait)
			time.Sleep(wait)
		case retryTransient:
			if transientTries >= maxTransientRetries {
				return "", err
			}
			wait := backoffDuration(transientTries, 0)
			slog.Warn("transient convert error, retrying", "cid", cid, "attempt", transientTries+1, "wait", wait, "error", err)
			transientTries++
			time.Sleep(wait)
		default:
			return "", err
		}
	}
}

// convertPDF returns (markdown, retryClass, retryAfter, error).
func (p *Pipeline) convertPDF(pdfData []byte) (string, retryClass, time.Duration, error) {
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	part, err := writer.CreateFormFile("document", "paper.pdf")
	if err != nil {
		return "", retryNone, 0, fmt.Errorf("create form: %w", err)
	}
	if _, err := part.Write(pdfData); err != nil {
		return "", retryNone, 0, fmt.Errorf("write form: %w", err)
	}
	writer.Close()

	req, err := http.NewRequest("POST", p.APIBase+"/documents/convert", &buf)
	if err != nil {
		return "", retryNone, 0, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+p.APIKey)
	req.Header.Set("Accept", "application/json")

	client := &http.Client{Timeout: p.convertTimeout()}
	resp, err := client.Do(req)
	if err != nil {
		return "", retryTransient, 0, fmt.Errorf("HTTP: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", retryTransient, 0, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode == 429 {
		p.convertRLOnce.Do(func() {
			slog.Warn("convert endpoint rate-limit headers (logged once)", "headers", rateLimitHeaders(resp))
		})
		return "", retryRateLimit, parseRetryAfter(resp), fmt.Errorf("rate limited (429)")
	}
	if isServerError(resp.StatusCode) {
		return "", retryTransient, 0, fmt.Errorf("status %d: %s", resp.StatusCode, truncate(string(body), 200))
	}
	if resp.StatusCode != http.StatusOK {
		return "", retryNone, 0, fmt.Errorf("status %d: %s", resp.StatusCode, truncate(string(body), 200))
	}

	var cr struct {
		Markdown string `json:"markdown"`
	}
	if err := json.Unmarshal(body, &cr); err != nil {
		return "", retryNone, 0, fmt.Errorf("parse response: %w", err)
	}

	markdown := strings.TrimSpace(cr.Markdown)
	if markdown == "" {
		return "", retryNone, 0, fmt.Errorf("empty markdown from conversion")
	}
	if mt := p.maxText(); len(markdown) > mt {
		markdown = markdown[:mt]
	}
	return markdown, retryNone, 0, nil
}

func (p *Pipeline) maxText() int {
	if p.MaxTextLen > 0 {
		return p.MaxTextLen
	}
	return defaultMaxTextLen
}

func (p *Pipeline) convertTimeout() time.Duration {
	if p.ConvertTO > 0 {
		return p.ConvertTO
	}
	return defaultConvertTimeout
}

func (p *Pipeline) extractKeywordsWithRetry(markdown, cid string) (*extractionResult, error) {
	p.initLimiters()
	rateLimitTries, transientTries := 0, 0
	for {
		p.chatLimiter.acquire()
		result, class, retryAfter, err := p.extractKeywords(markdown)
		if err == nil {
			return result, nil
		}
		switch class {
		case retryRateLimit:
			if retryAfter > quotaPauseThreshold {
				pause := capCooldown(retryAfter)
				slog.Warn("extract quota exhausted, pausing pool until reset",
					"cid", cid, "pause", pause.Round(time.Second),
					"resume_at", time.Now().Add(pause).Format(time.RFC3339))
				p.chatLimiter.penalize(pause)
				continue
			}
			wait := backoffDuration(rateLimitTries, retryAfter)
			rateLimitTries++
			slog.Warn("rate limited on extract, backing off", "cid", cid, "attempt", rateLimitTries, "wait", wait)
			p.chatLimiter.penalize(wait)
			time.Sleep(wait)
		case retryTransient:
			if transientTries >= maxTransientRetries {
				return nil, err
			}
			wait := backoffDuration(transientTries, 0)
			slog.Warn("transient extract error, retrying", "cid", cid, "attempt", transientTries+1, "wait", wait, "error", err)
			transientTries++
			time.Sleep(wait)
		default:
			return nil, err
		}
	}
}

// extractKeywords returns (result, retryClass, retryAfter, error).
func (p *Pipeline) extractKeywords(markdown string) (*extractionResult, retryClass, time.Duration, error) {
	temp := p.Temperature
	// Disable thinking mode so max_tokens is spent on the JSON answer.
	thinkingOff := false
	reqBody := chatRequest{
		Model: p.Model,
		Messages: []chatMessage{{
			Role:    "user",
			Content: keywordPrompt + markdown,
		}},
		Temperature:        &temp,
		MaxTokens:          1024,
		ResponseFormat:     &responseFormat{Type: "json_object"},
		EnableThinking:     &thinkingOff,
		ChatTemplateKwargs: map[string]interface{}{"enable_thinking": false},
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, retryNone, 0, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", p.APIBase+"/chat/completions", bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, retryNone, 0, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+p.APIKey)

	client := &http.Client{Timeout: llmTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, retryTransient, 0, fmt.Errorf("HTTP: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, retryTransient, 0, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode == 429 {
		p.chatRLOnce.Do(func() {
			slog.Warn("chat endpoint rate-limit headers (logged once)", "headers", rateLimitHeaders(resp))
		})
		return nil, retryRateLimit, parseRetryAfter(resp), fmt.Errorf("rate limited (429)")
	}
	if isServerError(resp.StatusCode) {
		return nil, retryTransient, 0, fmt.Errorf("API status %d: %s", resp.StatusCode, truncate(string(respBody), 200))
	}
	if resp.StatusCode != http.StatusOK {
		return nil, retryNone, 0, fmt.Errorf("API status %d: %s", resp.StatusCode, truncate(string(respBody), 200))
	}

	var chatResp chatResponse
	if err := json.Unmarshal(respBody, &chatResp); err != nil {
		return nil, retryNone, 0, fmt.Errorf("parse response: %w", err)
	}
	if chatResp.Error != nil {
		return nil, retryNone, 0, fmt.Errorf("API error: %s", chatResp.Error.Message)
	}
	if len(chatResp.Choices) == 0 {
		return nil, retryTransient, 0, fmt.Errorf("empty LLM response")
	}
	content := strings.TrimSpace(chatResp.Choices[0].Message.Content)
	if content == "" {
		return nil, retryTransient, 0, fmt.Errorf("empty LLM response")
	}

	result, err := parseExtraction(content)
	return result, retryNone, 0, err
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

// capCooldown bounds a quota-reset pause to maxCooldown, guarding against an
// absurd or malformed Retry-After while still honoring multi-hour windows.
func capCooldown(d time.Duration) time.Duration {
	if d > maxCooldown {
		return maxCooldown
	}
	return d
}

// backoffDuration returns an exponential backoff, using the server's
// Retry-After hint as a floor, but never sleeping longer than maxBackoff.
// Capping is important because some endpoints return a multi-minute reset
// window, which would otherwise freeze a worker for the whole window.
func backoffDuration(attempt int, serverHint time.Duration) time.Duration {
	exp := baseBackoff
	for i := 0; i < attempt; i++ {
		exp *= 2
		if exp >= maxBackoff {
			exp = maxBackoff
			break
		}
	}
	wait := exp
	if serverHint > wait {
		wait = serverHint
	}
	if wait > maxBackoff {
		wait = maxBackoff
	}
	return wait
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

// OpenAI-compatible API types.

type chatRequest struct {
	Model              string                 `json:"model"`
	Messages           []chatMessage          `json:"messages"`
	Temperature        *float64               `json:"temperature,omitempty"`
	MaxTokens          int                    `json:"max_tokens,omitempty"`
	ResponseFormat     *responseFormat        `json:"response_format,omitempty"`
	EnableThinking     *bool                  `json:"enable_thinking,omitempty"`
	ChatTemplateKwargs map[string]interface{} `json:"chat_template_kwargs,omitempty"`
}

type responseFormat struct {
	Type string `json:"type"`
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
	Title      string   `json:"title"`
	BroadField string   `json:"broad_field"`
	SubTopic   string   `json:"sub_topic"`
	Keywords   []string `json:"keywords"`
}
