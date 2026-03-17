package exposure

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
)

// StartURLProxy starts an in-process httputil.ReverseProxy that forwards all
// traffic to targetURL. It listens on a random loopback port and returns the
// bound address ("127.0.0.1:PORT") plus a stop function.
//
// The caller must invoke stop() when the tunnel is closed to release the port.
func StartURLProxy(ctx context.Context, targetURL string) (localAddr string, stop func(), err error) {
	parsed, err := url.Parse(targetURL)
	if err != nil {
		return "", nil, fmt.Errorf("StartURLProxy: invalid target URL %q: %w", targetURL, err)
	}

	proxy := httputil.NewSingleHostReverseProxy(parsed)
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, e error) {
		slog.Default().Error("url proxy: upstream error", "target", targetURL, "error", e)
		http.Error(w, "bad gateway", http.StatusBadGateway)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, fmt.Errorf("StartURLProxy: listen: %w", err)
	}

	srv := &http.Server{Handler: proxy}
	go func() {
		if serveErr := srv.Serve(ln); serveErr != nil && serveErr != http.ErrServerClosed {
			slog.Default().Error("url proxy: serve error", "error", serveErr)
		}
	}()

	stopFn := func() {
		if shutErr := srv.Shutdown(context.Background()); shutErr != nil {
			slog.Default().Warn("url proxy: shutdown error", "error", shutErr)
		}
	}

	// If the parent context is already cancelled, stop immediately.
	go func() {
		<-ctx.Done()
		stopFn()
	}()

	return ln.Addr().String(), stopFn, nil
}
