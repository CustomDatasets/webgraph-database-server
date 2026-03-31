// ccwg-db-client.go — Test client for ccwg-db-server.go (Go, stdlib only)
//
// RUN:
//   go run ccwg-db-client.go
//
// Or compile and run separately:
//   go build -o ccwg-db-client ccwg-db-client.go
//   ./ccwg-db-client
//
// REQUIRES:
//   Go 1.11+ (uses only standard library: net/http, encoding/json, etc.)
//
// DESCRIPTION:
//   Sends a POST request to the local ccwg-db-server with the hostnames
//   listed below (one per line) and prints the JSON response with pretty
//   formatting. Start ccwg-db-server.go first in another terminal.

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

// --- Hostnames to look up (one per line) ---
var hostnames = "google.com\nreddit.com"

// --- Server endpoint ---
var serverURL = "http://127.0.0.1:8999/"

func main() {
	client := &http.Client{Timeout: 30 * time.Second}

	resp, err := client.Post(serverURL, "text/plain", bytes.NewBufferString(hostnames))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read response: %v\n", err)
		os.Exit(1)
	}

	if resp.StatusCode != 200 {
		fmt.Fprintf(os.Stderr, "HTTP %d\n", resp.StatusCode)
	}

	// Pretty-print JSON response
	var parsed interface{}
	if json.Unmarshal(body, &parsed) == nil {
		pretty, err := json.MarshalIndent(parsed, "", "  ")
		if err == nil {
			fmt.Println(string(pretty))
			return
		}
	}

	// Fallback: print raw
	fmt.Println(string(body))
}
