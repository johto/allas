package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
)

type ListenConfig struct {
	Port int
	Host string

	KeepAlive bool
}

func (lc ListenConfig) Listen() (net.Listener, error) {
	var listener net.Listener
	var err error
	if lc.Host[0] == '/' {
		var fi os.FileInfo

		// See if the socket file exists already.  Since we can't guarantee the
		// socket to be closed in every case (such as in the event of an
		// unexpected panic or a crash), we need to be prepared to deal with
		// the case where the socket we want to bind to already exists.
		// However, try not to be too careless, and only remove the file if it
		// pre-exists as a UNIX socket; we wouldn't want to remove regular
		// files or directories, for example.
		fi, err = os.Stat(lc.Host)
		if err == nil {
			if fi.Mode() & os.ModeSocket > 0 {
				_ = os.Remove(lc.Host)
			} else {
				return nil, fmt.Errorf("file %q already exists and is not a UNIX socket", lc.Host)
			}
		}

		listener, err = net.Listen("unix", lc.Host)
		if err != nil {
			return nil, err
		}
	} else {
		var socket string
		if lc.Host == "*" {
			lc.Host = ""
		}
		socket = net.JoinHostPort(lc.Host, strconv.Itoa(lc.Port))
		listener, err = net.Listen("tcp", socket)
		if err != nil {
			return nil, err
		}
	}
	return listener, nil
}

func (lc ListenConfig) MaybeEnableKeepAlive(c net.Conn) {
	// Try and keepalives if they were asked for
	if lc.KeepAlive {
		tcpConn, ok := c.(*net.TCPConn)
		if ok {
			_ = tcpConn.SetKeepAlive(true)
		}
	}
}
