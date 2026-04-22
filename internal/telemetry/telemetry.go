// Package telemetry provides write-ahead log (WAL) telemetry with background
// cloud flush. All operations are fail-open: telemetry errors never crash or
// block the main request path.
package telemetry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

// maxFileSize is the maximum JSONL file size before rotation (10 MB).
const maxFileSize = 10 * 1024 * 1024

// maxBatchSize is the maximum number of events per POST.
const maxBatchSize = 100

// ghTokenCacheDuration is how long a cached GitHub token remains valid.
const ghTokenCacheDuration = 30 * time.Minute

// Event represents a single telemetry event written to the WAL.
type Event struct {
	Type             string `json:"type"`
	Kit              string `json:"kit"`
	Ts               string `json:"ts"`
	RequestModel     string `json:"requestModel"`
	RoutedModel      string `json:"routedModel"`
	Scenario         string `json:"scenario"`
	Streaming        bool   `json:"streaming"`
	MessageCount     int    `json:"messageCount"`
	ToolDefCount     int    `json:"toolDefCount"`
	InputTokens      int    `json:"inputTokens"`
	OutputTokens     int    `json:"outputTokens"`
	CachedTokens     int    `json:"cachedTokens"`
	LatencyMs        int64  `json:"latencyMs"`
	Success          bool   `json:"success"`
	FallbackAttempts int    `json:"fallbackAttempts"`
	FallbackModel    string `json:"fallbackModel,omitempty"`
	ErrorType        string `json:"errorType,omitempty"`
	ToolCallCount    int    `json:"toolCallCount"`
	Hostname         string `json:"hostname"`
	Platform         string `json:"platform"`
}

// Writer handles WAL-based telemetry with background flushing.
type Writer struct {
	mu       sync.Mutex
	filePath string
	dir      string
	logger   *slog.Logger

	// Flusher state
	endpoint string
	getToken func() string
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// New creates a new telemetry Writer. The JSONL file is stored at
// ~/.model-router/telemetry.jsonl.
func New(logger *slog.Logger) *Writer {
	if logger == nil {
		logger = slog.Default()
	}
	home, _ := os.UserHomeDir()
	dir := filepath.Join(home, ".model-router")
	return &Writer{
		filePath: filepath.Join(dir, "telemetry.jsonl"),
		dir:      dir,
		logger:   logger,
	}
}

// WriteEvent appends a JSON line to the WAL file. It is safe for concurrent
// use. Errors are logged but never returned — telemetry is fail-open.
func (w *Writer) WriteEvent(ev Event) {
	defer func() {
		if r := recover(); r != nil {
			w.logger.Warn("telemetry WriteEvent panic recovered", "error", r)
		}
	}()

	ev.Type = "model-router:request"
	ev.Kit = "theonekit-model-router"
	if ev.Ts == "" {
		ev.Ts = time.Now().UTC().Format(time.RFC3339)
	}
	if ev.Hostname == "" {
		ev.Hostname, _ = os.Hostname()
	}
	if ev.Platform == "" {
		ev.Platform = runtime.GOOS
	}

	data, err := json.Marshal(ev)
	if err != nil {
		w.logger.Warn("telemetry: marshal event failed", "error", err)
		return
	}
	data = append(data, '\n')

	w.mu.Lock()
	defer w.mu.Unlock()

	// Ensure directory exists.
	if err := os.MkdirAll(w.dir, 0755); err != nil {
		w.logger.Warn("telemetry: mkdir failed", "error", err)
		return
	}

	// Check file size and rotate if needed.
	if info, err := os.Stat(w.filePath); err == nil && info.Size() >= maxFileSize {
		w.rotateLocked()
	}

	f, err := os.OpenFile(w.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		w.logger.Warn("telemetry: open file failed", "error", err)
		return
	}
	defer f.Close()

	// Use flock for concurrent write safety.
	if err := flock(f); err != nil {
		w.logger.Warn("telemetry: flock failed", "error", err)
		// Continue anyway — better to write without lock than not at all.
	}
	defer funlock(f)

	if _, err := f.Write(data); err != nil {
		w.logger.Warn("telemetry: write failed", "error", err)
	}
}

// rotateLocked renames the current JSONL file with a timestamp suffix.
// Caller must hold w.mu.
func (w *Writer) rotateLocked() {
	ts := time.Now().UTC().Format("20060102T150405Z")
	rotated := w.filePath + "." + ts
	if err := os.Rename(w.filePath, rotated); err != nil {
		w.logger.Warn("telemetry: rotate failed", "error", err)
	}
}

// StartFlusher launches a background goroutine that flushes the WAL to the
// remote endpoint at the given interval.
func (w *Writer) StartFlusher(endpoint string, intervalSec int, getToken func() string) {
	w.endpoint = endpoint
	w.getToken = getToken
	if intervalSec <= 0 {
		intervalSec = 60
	}
	w.stopCh = make(chan struct{})
	w.doneCh = make(chan struct{})

	go func() {
		defer close(w.doneCh)
		ticker := time.NewTicker(time.Duration(intervalSec) * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				w.flush()
			case <-w.stopCh:
				w.flush() // final drain
				return
			}
		}
	}()
}

// Flush performs an immediate synchronous flush (called on shutdown).
func (w *Writer) Flush() {
	if w.stopCh != nil {
		close(w.stopCh)
		<-w.doneCh
		w.stopCh = nil
	} else {
		w.flush()
	}
}

// FlushPending flushes any events left over from a previous crash. Call on
// startup before starting the flusher.
func (w *Writer) FlushPending() {
	w.flush()
}

// flush reads the WAL, sends batches to the endpoint, and truncates on success.
func (w *Writer) flush() {
	defer func() {
		if r := recover(); r != nil {
			w.logger.Warn("telemetry flush panic recovered", "error", r)
		}
	}()

	if w.endpoint == "" {
		return
	}

	w.mu.Lock()
	data, err := os.ReadFile(w.filePath)
	w.mu.Unlock()

	if err != nil {
		if !os.IsNotExist(err) {
			w.logger.Debug("telemetry: read WAL failed", "error", err)
		}
		return
	}

	lines := splitLines(data)
	if len(lines) == 0 {
		return
	}

	token := ""
	if w.getToken != nil {
		token = w.getToken()
	}

	// Send in batches of maxBatchSize.
	flushed := 0
	for i := 0; i < len(lines); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(lines) {
			end = len(lines)
		}
		batch := lines[i:end]
		if err := w.sendBatch(batch, token); err != nil {
			w.logger.Debug("telemetry: flush batch failed", "error", err, "batch_size", len(batch))
			break // keep remaining for retry
		}
		flushed = end
	}

	if flushed == 0 {
		return
	}

	// Remove flushed lines from the file.
	w.mu.Lock()
	defer w.mu.Unlock()

	if flushed >= len(lines) {
		// All flushed — truncate.
		os.Truncate(w.filePath, 0)
	} else {
		// Partial flush — rewrite remaining lines.
		remaining := lines[flushed:]
		var buf bytes.Buffer
		for _, line := range remaining {
			buf.Write(line)
			buf.WriteByte('\n')
		}
		os.WriteFile(w.filePath, buf.Bytes(), 0644)
	}

	w.logger.Debug("telemetry: flushed events", "count", flushed)
}

// sendBatch POSTs a batch of JSONL lines to the telemetry endpoint.
func (w *Writer) sendBatch(lines [][]byte, token string) error {
	var events []json.RawMessage
	for _, line := range lines {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		events = append(events, json.RawMessage(line))
	}
	if len(events) == 0 {
		return nil
	}

	payload, err := json.Marshal(map[string]interface{}{
		"events": events,
	})
	if err != nil {
		return fmt.Errorf("marshal batch: %w", err)
	}

	req, err := http.NewRequest("POST", w.endpoint, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("POST failed: %w", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode >= 400 {
		return fmt.Errorf("endpoint returned %d", resp.StatusCode)
	}
	return nil
}

// splitLines splits data into non-empty lines.
func splitLines(data []byte) [][]byte {
	raw := bytes.Split(data, []byte("\n"))
	var lines [][]byte
	for _, line := range raw {
		if len(bytes.TrimSpace(line)) > 0 {
			lines = append(lines, line)
		}
	}
	return lines
}

// --- GitHub token caching ---

var (
	ghTokenMu    sync.Mutex
	ghTokenCache string
	ghTokenTime  time.Time
)

// GHToken returns a GitHub auth token via "gh auth token", cached for 30 min.
// Returns empty string on failure (fail-open).
func GHToken() string {
	ghTokenMu.Lock()
	defer ghTokenMu.Unlock()

	// Check memory cache.
	if ghTokenCache != "" && time.Since(ghTokenTime) < ghTokenCacheDuration {
		return ghTokenCache
	}

	// Check file cache.
	home, _ := os.UserHomeDir()
	cacheFile := filepath.Join(home, ".model-router", ".gh-token-cache")
	if data, err := os.ReadFile(cacheFile); err == nil {
		parts := strings.SplitN(string(data), "\n", 2)
		if len(parts) == 2 {
			if ts, err := time.Parse(time.RFC3339, strings.TrimSpace(parts[0])); err == nil {
				if time.Since(ts) < ghTokenCacheDuration {
					tok := strings.TrimSpace(parts[1])
					if tok != "" {
						ghTokenCache = tok
						ghTokenTime = ts
						return tok
					}
				}
			}
		}
	}

	// Run gh auth token.
	out, err := exec.Command("gh", "auth", "token").Output()
	if err != nil {
		return ""
	}
	tok := strings.TrimSpace(string(out))
	if tok == "" {
		return ""
	}

	// Update caches.
	now := time.Now().UTC()
	ghTokenCache = tok
	ghTokenTime = now

	// Write file cache (best-effort).
	os.MkdirAll(filepath.Dir(cacheFile), 0755)
	content := now.Format(time.RFC3339) + "\n" + tok
	os.WriteFile(cacheFile, []byte(content), 0600)

	return tok
}
