package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestVersionJSON(t *testing.T) {
	var out strings.Builder
	if err := executeWithIO(context.Background(), []string{"version", "--json"}, &out, &out); err != nil {
		t.Fatalf("version: %v", err)
	}
	var got versionInfo
	if err := json.Unmarshal([]byte(out.String()), &got); err != nil {
		t.Fatalf("decode version: %v", err)
	}
	if got.SchemaVersion != 1 || got.Module == "" || got.GoVersion == "" || len(got.SHA256) != 64 || got.Executable == "" {
		t.Fatalf("version info = %+v", got)
	}
}
