package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
)

type versionCommand struct {
	JSON bool `help:"Write JSON output."`
}

type versionInfo struct {
	SchemaVersion int    `json:"schema_version"`
	Module        string `json:"module"`
	Version       string `json:"version"`
	GoVersion     string `json:"go_version"`
	VCSRevision   string `json:"vcs_revision,omitempty"`
	VCSTime       string `json:"vcs_time,omitempty"`
	VCSDirty      bool   `json:"vcs_dirty"`
	Executable    string `json:"executable"`
	SHA256        string `json:"sha256"`
}

func (c *versionCommand) Run(g *globals) error {
	info, err := runningVersionInfo()
	if err != nil {
		return err
	}
	if c.JSON {
		return writeJSON(g.Out, info)
	}
	_, _ = fmt.Fprintf(g.Out, "%s %s\ngo: %s\nrevision: %s (%s, dirty=%t)\nexecutable: %s\nsha256: %s\n", info.Module, info.Version, info.GoVersion, info.VCSRevision, info.VCSTime, info.VCSDirty, info.Executable, info.SHA256)
	return nil
}

func runningVersionInfo() (versionInfo, error) {
	info := versionInfo{SchemaVersion: 1, GoVersion: runtime.Version()}
	if build, ok := debug.ReadBuildInfo(); ok {
		info.Module, info.Version = build.Main.Path, build.Main.Version
		for _, setting := range build.Settings {
			switch setting.Key {
			case "vcs.revision":
				info.VCSRevision = setting.Value
			case "vcs.time":
				info.VCSTime = setting.Value
			case "vcs.modified":
				info.VCSDirty, _ = strconv.ParseBool(setting.Value)
			}
		}
	}
	executable, err := os.Executable()
	if err != nil {
		return versionInfo{}, fmt.Errorf("resolve executable: %w", err)
	}
	info.Executable = executable
	info.SHA256, err = fileSHA256(executable)
	if err != nil {
		return versionInfo{}, err
	}
	return info, nil
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path) //nolint:gosec // the path is the current executable returned by os.Executable
	if err != nil {
		return "", fmt.Errorf("open running executable: %w", err)
	}
	defer func() { _ = f.Close() }()
	hash := sha256.New()
	if _, err := io.Copy(hash, f); err != nil {
		return "", fmt.Errorf("hash running executable: %w", err)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
