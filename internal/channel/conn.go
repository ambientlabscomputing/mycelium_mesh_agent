package channel

import (
	"bufio"
	"io"
	"net"
	"os"
)

// bufferedConn wraps a net.Conn and prefixes its reads with a bufio.Reader's
// buffered data (HTTP response bytes already consumed during the upgrade).
type bufferedConn struct {
	net.Conn
	br *bufio.Reader
}

func newBufferedConn(c net.Conn, br *bufio.Reader) net.Conn {
	if br.Buffered() == 0 {
		return c
	}
	return &bufferedConn{Conn: c, br: br}
}

func (bc *bufferedConn) Read(b []byte) (int, error) {
	if bc.br != nil && bc.br.Buffered() > 0 {
		n, err := bc.br.Read(b)
		if bc.br.Buffered() == 0 {
			bc.br = nil
		}
		return n, err
	}
	return bc.Conn.Read(b)
}

func (bc *bufferedConn) WriteTo(w io.Writer) (int64, error) {
	return io.Copy(w, struct{ io.Reader }{bc})
}

// loadFileBytes reads an entire file's contents.
func loadFileBytes(path string) ([]byte, error) {
	return os.ReadFile(path)
}
