package version
package version

import (
	"fmt"
	"runtime"






































}	return Version	}		return fmt.Sprintf("%s (commit %s)", Version, Commit[:7])	if Commit != "unknown" && len(Commit) >= 7 {func String() string {// String returns a human-readable version string}	}		Platform:  fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),		GoVersion: runtime.Version(),		BuildDate: BuildDate,		Commit:    Commit,		Version:   Version,	return Info{func GetInfo() Info {// GetInfo returns the version information}	Platform  string `json:"platform"`	GoVersion string `json:"go_version"`	BuildDate string `json:"built_at"`	Commit    string `json:"commit"`	Version   string `json:"version"`type Info struct {// Info returns structured version informationvar BuildDate = "unknown"// BuildDate is the build timestamp. Set via -ldflags at build time.var Commit = "unknown"// Commit is the git commit hash. Set via -ldflags at build time.var Version = "dev"// Version is the semantic version of MMA. Set via -ldflags at build time.)