package util

import (
	"bufio"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"strings"
)

// Call fn repeatedly until an error is returned; then send the error
// on the given channel and return
func ErrToChannel(fn func() error, ch chan<- error) {
	var err error
	for err = fn(); err == nil; err = fn() {
	}
	ch <- err
}

// Automatically chooses between unix sockets and tcp sockets for
// listening
func AutoListen(location string) (net.Listener, error) {
	if strings.Contains(location, "/") {
		return net.Listen("unix", location)
	}
	return net.Listen("tcp", location)
}

// Automatically chooses between unix sockets and tcp sockets for
// dialing.
func AutoDial(location string) (net.Conn, error) {
	if strings.Contains(location, "/") {
		return net.Dial("unix", location)
	}
	return net.Dial("tcp", location)
}

// Flush buffers, returning any error encountered
type Flusher interface {
	Flush() error
}

type bufWriteConn struct {
	io.ReadCloser
	Flusher
	io.Writer
}

func NewBufferedReadWriteCloser(rwc io.ReadWriteCloser) io.ReadWriteCloser {
	bw := bufio.NewWriter(rwc)
	return &bufWriteConn{rwc, bw, bw}
}

type SSLMode string

const (
	SSLDisable SSLMode = "disable"
	SSLAllow           = "allow"
	SSLPrefer          = "prefer"
	SSLRequire         = "require"
)

type SSLConfig struct {
	Mode   SSLMode
	Config tls.Config
}

func NegotiateTLS(c net.Conn, config *SSLConfig) (net.Conn, error) {
	sslmode := config.Mode
	if sslmode != SSLDisable {
		// send an SSLRequest message
		// length: int32(8)
		// code:   int32(80877103)
		c.Write([]byte{0x00, 0x00, 0x00, 0x08,
			0x04, 0xd2, 0x16, 0x2f})

		sslResponse := make([]byte, 1)
		bytesRead, err := io.ReadFull(c, sslResponse)
		if bytesRead != 1 || err != nil {
			return nil, errors.New("Could not read response to SSL Request")
		}

		if sslResponse[0] == 'S' {
			return tls.Client(c, &config.Config), nil
		} else if sslResponse[0] == 'N' && sslmode != SSLAllow &&
			sslmode != SSLPrefer {
			// reject; we require ssl
			return nil, errors.New("SSL required but declined by server.")
		} else {
			return c, nil
		}
	}

	return c, nil
}
