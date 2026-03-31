// ccwg-db-server.go — CommonCrawl WebGraph Rank Lookup Server (Go)
//
// SETUP (one time):
//   go mod init ccwg-db-server
//   go mod tidy
//
// RUN:
//   go run ccwg-db-server.go
//
// Or compile and run separately:
//   go build -o ccwg-db-server ccwg-db-server.go
//   ./ccwg-db-server
//
// REQUIRES:
//   Go 1.11+ with CGo enabled (default). Requires a C compiler (gcc or cc)
//   and the SQLite3 development library:
//     macOS:   already installed (ships with the OS)
//     Debian:  apt install libsqlite3-dev gcc
//     RHEL:    yum install sqlite-devel gcc
//     FreeBSD: pkg install sqlite3
//
// DESCRIPTION:
//   Single-file HTTP server for CommonCrawl WebGraph rank lookups.
//   Accepts plain-text POST bodies (one hostname per line, max 10), looks
//   up each hostname in two sets of sharded SQLite databases (host + domain),
//   and returns structured JSON with historical rank data.
//
//   Based on ApiServer.php but without rate limiting.
//
// ARCHITECTURE:
//   Go's net/http server handles each incoming request in its own goroutine
//   automatically. All goroutines share a single set of 64 SQLite database
//   handles (32 domain + 32 host) opened at startup. The database/sql
//   package manages its own connection pool and is safe for concurrent use.
//
// DATA FLOW:
//   1. Client POSTs plain text to / (one hostname per line, max 10).
//   2. Each hostname is normalized: lowercase, strip trailing dot, reverse
//      dot notation (e.g. "Google.COM" -> "com.google").
//   3. The reversed key is DJB2-hashed to select a shard (0-31).
//   4. Both the host DB and domain DB for that shard are queried.
//   5. Results are returned as JSON keyed by the original submitted hostname.

package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// --- Network ---
const listenHost = "127.0.0.1"
const listenPort = 8999

// --- Database paths ---
var domainDbDir = "/home/oa_adm/cc-webgraph/data-merged-domains"
var hostDbDir = "/home/oa_adm/cc-webgraph/data-merged-hosts"

// --- Sharding ---
const shardCount = 32
const shardBitMask = 0x1F
const djb2Seed = 5381
const djb2U32Mask = 0xFFFFFFFF

// --- Request limits ---
const maxHostsPerRequest = 10

// --- Access logging ---
var logDir = "logs"

const logFlushSizeBytes = 6144
const logFlushIntervalS = 300

// --- SQLite tuning ---
const sqliteCacheSizeKB = 65536
const sqliteMmapSize = 268435456

// Entry represents one crawl period's rank data.
type Entry struct {
	YearMonth string `json:"year_month"`
	HcPos     int    `json:"hc_pos"`
	HcVal     int    `json:"hc_val"`
	PrPos     int    `json:"pr_pos"`
	PrVal     int    `json:"pr_val"`
	HcValNorm int    `json:"hc_val_norm"`
	PrValNorm int    `json:"pr_val_norm"`
	NHosts    *int   `json:"n_hosts,omitempty"`
}

// HostResult holds host-level and domain-level rank data.
type HostResult struct {
	Host   []Entry `json:"host"`
	Domain []Entry `json:"domain"`
}

// DbPool holds all shard database connections.
type DbPool struct {
	domainDbs [shardCount]*sql.DB
	hostDbs   [shardCount]*sql.DB
}

// AccessLogger buffers log lines and flushes periodically.
type AccessLogger struct {
	mu       sync.Mutex
	filePath string
	buffer   string
}

func djb2Hash(s string) uint32 {
	var h uint32 = djb2Seed
	for i := 0; i < len(s); i++ {
		h = ((h << 5) + h + uint32(s[i])) & djb2U32Mask
	}
	return h
}

func normalize(hostname string) string {
	hostname = strings.ToLower(strings.TrimSpace(hostname))
	hostname = strings.TrimRight(hostname, ".")
	parts := strings.Split(hostname, ".")
	for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
		parts[i], parts[j] = parts[j], parts[i]
	}
	return strings.Join(parts, ".")
}

func parseTsvData(rawData string) []Entry {
	var entries []Entry
	lines := strings.Split(rawData, "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) < 7 {
			continue
		}

		hcPos, _ := strconv.Atoi(fields[1])
		hcVal, _ := strconv.Atoi(fields[2])
		prPos, _ := strconv.Atoi(fields[3])
		prVal, _ := strconv.Atoi(fields[4])
		hcValNorm, _ := strconv.Atoi(fields[5])
		prValNorm, _ := strconv.Atoi(fields[6])

		entry := Entry{
			YearMonth: fields[0],
			HcPos:     hcPos,
			HcVal:     hcVal,
			PrPos:     prPos,
			PrVal:     prVal,
			HcValNorm: hcValNorm,
			PrValNorm: prValNorm,
		}

		if len(fields) >= 8 {
			n, _ := strconv.Atoi(fields[7])
			entry.NHosts = &n
		}

		entries = append(entries, entry)
	}
	return entries
}

//------------------------------------------------------------------------------
// DbPool
//------------------------------------------------------------------------------

func NewDbPool(domainDir string, hostDir string) *DbPool {
	return &DbPool{}
}

func (p *DbPool) OpenAll(domainDir string, hostDir string) {
	for i := 0; i < shardCount; i++ {
		p.domainDbs[i] = openShard(domainDir, "domain", i)
		p.hostDbs[i] = openShard(hostDir, "host", i)
	}
}

func (p *DbPool) CloseAll() {
	for i := 0; i < shardCount; i++ {
		if p.domainDbs[i] != nil {
			p.domainDbs[i].Close()
		}
		if p.hostDbs[i] != nil {
			p.hostDbs[i].Close()
		}
	}
}

func (p *DbPool) LookupHost(shard uint32, reversedHost string) (string, bool) {
	return queryDb(p.hostDbs[shard], reversedHost)
}

func (p *DbPool) LookupDomain(shard uint32, reversedHost string) (string, bool) {
	return queryDb(p.domainDbs[shard], reversedHost)
}

func openShard(dir string, dbType string, shard int) *sql.DB {
	path := filepath.Join(dir, fmt.Sprintf("%s.%d.db", dbType, shard))
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil
	}

	dsn := fmt.Sprintf("file:%s?mode=ro", path)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil
	}

	db.Exec(fmt.Sprintf("PRAGMA cache_size = -%d", sqliteCacheSizeKB))
	db.Exec(fmt.Sprintf("PRAGMA mmap_size = %d", sqliteMmapSize))

	return db
}

func queryDb(db *sql.DB, reversedHost string) (string, bool) {
	if db == nil {
		return "", false
	}

	var data string
	err := db.QueryRow(
		"SELECT data FROM host_data WHERE host = ? LIMIT 1",
		reversedHost,
	).Scan(&data)
	if err != nil {
		return "", false
	}
	return data, true
}

//------------------------------------------------------------------------------
// AccessLogger
//------------------------------------------------------------------------------

func NewAccessLogger() *AccessLogger {
	os.MkdirAll(logDir, 0755)
	ts := time.Now().Format("20060102-150405")
	pid := os.Getpid()
	path := filepath.Join(logDir, fmt.Sprintf("CCWG-DB-%s-%d.log", ts, pid))
	return &AccessLogger{filePath: path}
}

func (l *AccessLogger) Append(ip string, hostnames []string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	ts := time.Now().Format("2006-01-02 15:04:05")
	hostsStr := strings.Join(hostnames, " ")
	l.buffer += fmt.Sprintf("%s\t%s\t%s\n", ts, ip, hostsStr)

	if len(l.buffer) >= logFlushSizeBytes {
		l.flushLocked()
	}
}

func (l *AccessLogger) Flush() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.flushLocked()
}

func (l *AccessLogger) flushLocked() {
	if l.buffer == "" {
		return
	}
	f, err := os.OpenFile(l.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err == nil {
		f.WriteString(l.buffer)
		f.Close()
	}
	l.buffer = ""
}

func (l *AccessLogger) StartFlushTimer() {
	go func() {
		ticker := time.NewTicker(time.Duration(logFlushIntervalS) * time.Second)
		for range ticker.C {
			l.Flush()
		}
	}()
}

//------------------------------------------------------------------------------
// HTTP handler
//------------------------------------------------------------------------------

type LookupServer struct {
	pool   *DbPool
	logger *AccessLogger
}

func (s *LookupServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	if r.Method == http.MethodOptions {
		w.WriteHeader(200)
		return
	}

	if r.URL.Path != "/" {
		w.WriteHeader(404)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Not found. Use POST /",
		})
		return
	}

	if r.Method != http.MethodPost {
		w.WriteHeader(405)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Method not allowed. Use POST.",
		})
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil || len(strings.TrimSpace(string(body))) == 0 {
		w.WriteHeader(400)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error":           fmt.Sprintf("Request must contain 1-%d hostnames, one per line", maxHostsPerRequest),
			"max_per_request": maxHostsPerRequest,
		})
		return
	}

	lines := strings.Split(string(body), "\n")
	var hostnames []string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			hostnames = append(hostnames, line)
		}
	}

	hostCount := len(hostnames)
	if hostCount < 1 || hostCount > maxHostsPerRequest {
		w.WriteHeader(400)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error":           fmt.Sprintf("Request must contain 1-%d hostnames, one per line", maxHostsPerRequest),
			"max_per_request": maxHostsPerRequest,
		})
		return
	}

	ip := r.Header.Get("X-Real-IP")
	if ip == "" {
		ip = r.Header.Get("X-Forwarded-For")
	}
	if ip == "" {
		ip = r.RemoteAddr
	}
	if idx := strings.Index(ip, ","); idx != -1 {
		ip = strings.TrimSpace(ip[:idx])
	}

	s.logger.Append(ip, hostnames)

	results := make(map[string]HostResult)
	for _, hostname := range hostnames {
		reversed := normalize(hostname)
		h := djb2Hash(reversed)
		shard := h & shardBitMask

		var hostData []Entry
		var domainData []Entry

		if raw, ok := s.pool.LookupHost(shard, reversed); ok {
			hostData = parseTsvData(raw)
		}
		if hostData == nil {
			hostData = []Entry{}
		}

		if raw, ok := s.pool.LookupDomain(shard, reversed); ok {
			domainData = parseTsvData(raw)
		}
		if domainData == nil {
			domainData = []Entry{}
		}

		results[hostname] = HostResult{
			Host:   hostData,
			Domain: domainData,
		}
	}

	w.WriteHeader(200)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"results": results,
	})
}

//------------------------------------------------------------------------------
// Main
//------------------------------------------------------------------------------

func main() {
	fmt.Println("=== CommonCrawl WebGraph DB Server (Go) ===")
	fmt.Printf("Listening on %s:%d\n", listenHost, listenPort)
	fmt.Printf("Domain DB dir: %s\n", domainDbDir)
	fmt.Printf("Host DB dir:   %s\n", hostDbDir)
	fmt.Printf("Max per request: %d\n", maxHostsPerRequest)
	fmt.Println()

	pool := &DbPool{}
	pool.OpenAll(domainDbDir, hostDbDir)
	defer pool.CloseAll()

	logger := NewAccessLogger()
	logger.StartFlushTimer()

	srv := &LookupServer{pool: pool, logger: logger}

	addr := fmt.Sprintf("%s:%d", listenHost, listenPort)
	httpServer := &http.Server{
		Addr:         addr,
		Handler:      srv,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	fmt.Printf("Server started on %s\n", addr)
	if err := httpServer.ListenAndServe(); err != nil {
		fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
		os.Exit(1)
	}
}
