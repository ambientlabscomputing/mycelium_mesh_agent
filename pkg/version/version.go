package version

import (
	"fmt"
	"runtime"
)

// Version is the semantic version of MMA. Set via -ldflags at build time.
var Version = "dev"

// Commit is the git commit hash. Set via -ldflags at build time.
var Commit = "unknown"

// BuildDate is the build timestamp. Set via -ldflags at build time.
var BuildDate = "unknown"

// Info returns structured version information
type Info struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildDate string `json:"built_at"`
	GoVersion string `json:"go_version"`
	Platform  string `json:"platform"`
}

// GetInfo returns the version information
func GetInfo() Info {
	return Info{
		Version:   Version,
		Commit:    Commit,
		BuildDate: BuildDate,
		GoVersion: runtime.Version(),
		Platform:  fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
	}
}

// String returns a human-readable version string
func String() string {
	if Commit != "unknown" && len(Commit) >= 7 {
		return fmt.Sprintf("%s (commit %s)", Version, Commit[:7])
	}
	return Version
}
