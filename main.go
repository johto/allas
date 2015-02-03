package main

import (
	"github.com/johto/notifyutils/notifydispatcher"
	"github.com/lib/pq"

	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

func printUsage() {
    fmt.Fprintf(os.Stderr, `Usage:
  %s [--help] configfile

Options:
  --help                display this help and exit
`, os.Args[0])
}

func main() {
	InitErrorLog(os.Stdout)

	if len(os.Args) != 2 {
		printUsage()
		os.Exit(1)
	} else if os.Args[1] == "--help" {
		printUsage()
		os.Exit(1)
	}

	err := readConfigFile(os.Args[1])
	if err != nil {
		elog.Fatalf("error while reading configuration file: %s", err)
	}
	if len(Config.Databases) == 0 {
		elog.Fatalf("at least one database must be configured")
	}

	listenAddr := net.JoinHostPort(Config.ListenAddress, strconv.Itoa(Config.ListenPort))
	tcpAddr, err := net.ResolveTCPAddr("tcp", listenAddr)
	if err != nil {
		elog.Fatalf("could not resolve listen address: %s", err)
	}
	l, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		elog.Fatalf("could not open listen socket: %s", err)
	}

	var m sync.Mutex
	var connStatusNotifier chan struct{}

	listenerStateChange := func(ev pq.ListenerEventType, err error) {
		switch ev {
		case pq.ListenerEventConnectionAttemptFailed:
			elog.Warningf("Listener: could not connect to the database: %s", err.Error())

		case pq.ListenerEventDisconnected:
			elog.Warningf("Listener: lost connection to the database: %s", err.Error())
			m.Lock()
			close(connStatusNotifier)
			connStatusNotifier = nil
			m.Unlock()

		case pq.ListenerEventReconnected,
			pq.ListenerEventConnected:
			elog.Logf("Listener: connected to the database")
			m.Lock()
			connStatusNotifier = make(chan struct{})
			m.Unlock()
		}
	}

	// make sure pq.Listener doesn't pick up any env variables
	os.Clearenv()

	clientConnectionString := fmt.Sprintf("fallback_application_name=allas %s", Config.ClientConnInfo)
	listener := pq.NewListener(clientConnectionString,
		250*time.Millisecond, 5*time.Minute,
		listenerStateChange)

	nd := notifydispatcher.NewNotifyDispatcher(listener)
	nd.SetBroadcastOnConnectionLoss(false)
	nd.SetSlowReaderEliminationStrategy(notifydispatcher.NeglectSlowReaders)

	for {
		c, err := l.Accept()
		if err != nil {
			panic(err)
		}

		var myConnStatusNotifier chan struct{}

		m.Lock()
		if connStatusNotifier == nil {
			m.Unlock()
			go RejectFrontendConnection(c)
			continue
		} else {
			myConnStatusNotifier = connStatusNotifier
		}
		m.Unlock()

		newConn := NewFrontendConnection(c, nd, myConnStatusNotifier)
		go newConn.mainLoop(Config.Databases)
	}
}
