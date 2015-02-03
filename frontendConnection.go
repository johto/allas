package main

import (
	fbbuf "github.com/uhoh-itsmaciek/femebe/buf"
	fbcore "github.com/uhoh-itsmaciek/femebe/core"
	fbproto "github.com/uhoh-itsmaciek/femebe/proto"

	"github.com/johto/notifyutils/notifydispatcher"
	"github.com/lib/pq"

	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
)

var (
	errGracefulTermination  = errors.New("graceful termination")
	errClientCouldNotKeepUp = errors.New("client could not keep up")
	errLostServerConnection = errors.New("lost server connection")
)

type FrontendConnection struct {
	// immutable
	remoteAddr string

	stream     *fbcore.MessageStream
	dispatcher *notifydispatcher.NotifyDispatcher

	connStatusNotifier chan struct{}
	notify             chan *pq.Notification
	queryResultCh      chan QueryResult

	// owned by queryProcessingMainLoop until queryResultCh has been closed
	listenChannels map[string]struct{}

	lock sync.Mutex
	err  error
}

func initFatalMessage(message *fbcore.Message, sqlstate, errorMessage string) {
	buf := &bytes.Buffer{}
	buf.WriteByte('S')
	fbbuf.WriteCString(buf, "FATAL")
	buf.WriteByte('C')
	fbbuf.WriteCString(buf, sqlstate)
	buf.WriteByte('M')
	fbbuf.WriteCString(buf, errorMessage)
	buf.WriteByte('\x00')

	message.InitFromBytes(fbproto.MsgErrorResponseE, buf.Bytes())
}

func RejectFrontendConnection(c net.Conn) {
	var message fbcore.Message
	initFatalMessage(&message, "57A01", "no server connection available")

	_, _ = message.WriteTo(c)
	_ = c.Close()
}

func (c FrontendConnection) String() string {
	return c.remoteAddr
}

func NewFrontendConnection(c net.Conn, dispatcher *notifydispatcher.NotifyDispatcher, connStatusNotifier chan struct{}) *FrontendConnection {
	fc := &FrontendConnection{
		remoteAddr: c.RemoteAddr().String(),

		stream:     fbcore.NewFrontendStream(c),
		dispatcher: dispatcher,

		connStatusNotifier: connStatusNotifier,
		notify:             make(chan *pq.Notification, 32),
		queryResultCh:      make(chan QueryResult, 4),

		listenChannels: make(map[string]struct{}),
	}
	return fc
}

func (c *FrontendConnection) WriteAndFlush(msg *fbcore.Message) error {
	err := c.WriteMessage(msg)
	if err != nil {
		return err
	}
	return c.FlushStream()
}

func (c *FrontendConnection) fatal(err error) {
	c.setSessionError(err)

	var sqlstate, errorMessage string

	switch err {
	case errLostServerConnection:
		sqlstate = "57A02"
		errorMessage = "terminating connection because the server connection was lost"
	case errClientCouldNotKeepUp:
		sqlstate = "57A03"
		errorMessage = "terminating connection because the client could not keep up"
	default:
		panic(err)
	}

	var message fbcore.Message
	initFatalMessage(&message, sqlstate, errorMessage)

	// We don't care about the errors at this point; the connection might be
	// gone already.
	_ = c.WriteAndFlush(&message)
}

func (c *FrontendConnection) auth(dbcfg VirtualDatabaseConfiguration, sm *fbproto.StartupMessage) bool {
	authFailed := func(sqlstate, format string, v ...interface{}) bool {
		var msg fbcore.Message
		message := fmt.Sprintf(format, v...)
		initFatalMessage(&msg, sqlstate, message)
		_ = c.WriteMessage(&msg)
		_ = c.FlushStream()
		return false
	}

	username, ok := sm.Params["user"]
	if !ok {
		return authFailed("08P01", `required startup parameter "user" nor present in startup packet`)
	}
	dbname, ok := sm.Params["database"]
	if !ok {
		dbname = username
	}
	authMethod, ok := dbcfg.FindDatabase(dbname)
	if !ok {
		return authFailed("3D000", "database %q does not exist", dbname)
	}

	switch authMethod {
	case "trust":
		return true
	case "md5":
		// handled below
	default:
		elog.Errorf("unrecognized authentication method %q", authMethod)
		return authFailed("XX000", "internal error")
	}

	salt := make([]byte, 4)
	_, err := rand.Read(salt)
	if err != nil {
		elog.Errorf("could not generate random salt: %s", err)
		return authFailed("XX000", "internal error")
	}

	var msg fbcore.Message
	buf := &bytes.Buffer{}
	fbbuf.WriteInt32(buf, 5)
	buf.Write(salt)
	msg.InitFromBytes(fbproto.MsgAuthenticationMD5PasswordR, buf.Bytes())
	err = c.WriteAndFlush(&msg)
	if err != nil {
		elog.Logf("error during startup sequence: %s", err)
		return false
	}
	err = c.stream.Next(&msg)
	if err == io.EOF {
		elog.Debugf("EOF during startup sequence")
		return false
	} else if err != nil {
		elog.Logf("error during startup sequence: %s", err)
		return false
	}
	if msg.MsgType() != fbproto.MsgPasswordMessageP {
		return authFailed("08P01", "unexpected response %x", msg.MsgType())
	}
	// don't bother with messages which are clearly too big
	if msg.Size() > 100 {
		return authFailed("28001", "password authentication failed for user %q", username)
	}
	password, err := msg.Force()
	if err != nil {
		elog.Logf("error during startup sequence: %s", err)
		return false
	}
	success, err := dbcfg.MD5Auth(dbname, username, salt, password)
	if err != nil {
		elog.Logf("error during startup sequence: %s", err)
		return false
	}
	if !success {
		return authFailed("28001", "password authentication failed for user %q", username)
	}
	return true
}

func (c *FrontendConnection) startup(dbcfg VirtualDatabaseConfiguration) bool {
	var message fbcore.Message
	var err error

	for {
		err = c.stream.Next(&message)
		if err != nil {
			elog.Logf("error while reading startup packet: %s", err)
			return false
		}
		if fbproto.IsStartupMessage(&message) {
			break
		} else if fbproto.IsSSLRequest(&message) {
			_, err = message.Force()
			if err != nil {
				elog.Logf("error while reading SSLRequest: %s", err)
				return false
			}
			err = c.stream.SendSSLRequestResponse(fbcore.RejectSSLRequest)
			if err != nil {
				elog.Logf("error during startup sequence: %s", err)
				return false
			}
			err = c.stream.Flush()
			if err != nil {
				elog.Logf("error during startup sequence: %s", err)
			}
		} else if fbproto.IsCancelRequest(&message) {
			_ = c.stream.Close()
			return false
		} else {
			elog.Warningf("unrecognized message %#v", message)
			return false
		}
	}
	sm, err := fbproto.ReadStartupMessage(&message)
	if err != nil {
		elog.Logf("error while reading startup packet: %s", err)
		return false
	}

	if !c.auth(dbcfg, sm) {
		// error already logged
		_ = c.stream.Close()
		return false
	}

	fbproto.InitAuthenticationOk(&message)
	err = c.WriteMessage(&message)
	if err != nil {
		elog.Logf("error during startup sequence: %s", err)
		return false
	}

	fbproto.InitReadyForQuery(&message, fbproto.RfqIdle)
	err = c.WriteMessage(&message)
	if err != nil {
		elog.Logf("error during startup sequence: %s", err)
		return false
	}

	err = c.FlushStream()
	if err != nil {
		elog.Logf("error during startup sequence: %s", err)
		return false
	}
	return true
}

func (c *FrontendConnection) WriteMessage(msg *fbcore.Message) error {
	return c.stream.Send(msg)
}

func (c *FrontendConnection) FlushStream() error {
	return c.stream.Flush()
}

// Implements Frontend.Listen.
func (c *FrontendConnection) Listen(channel string) error {
	c.listenChannels[channel] = struct{}{}
	err := c.dispatcher.Listen(channel, c.notify)
	if err != nil && err != notifydispatcher.ErrChannelAlreadyActive {
		return err
	}
	return nil
}

// Implements Frontend.Unlisten.
func (c *FrontendConnection) Unlisten(channel string) error {
	delete(c.listenChannels, channel)
	err := c.dispatcher.Unlisten(channel, c.notify)
	if err != nil && err != notifydispatcher.ErrChannelNotActive {
		return err
	}
	return nil
}

// Implements Frontend.UnlistenAll.
func (c *FrontendConnection) UnlistenAll() error {
	var firstErr error
	for channel := range c.listenChannels {
		err := c.Unlisten(channel)
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	c.listenChannels = make(map[string]struct{})
	return firstErr
}

// Non-nil return value means we should kill this client
func (c *FrontendConnection) processQuery(query string) error {
	q, err := ParseQuery(query)
	if err != nil {
		c.queryResultCh <- NewErrorResponse("42601", err.Error())
		return nil
	}

	result, err := q.Process(c)
	if err != nil {
		return err
	}
	c.queryResultCh <- result
	return nil
}

// This is the main loop for processing messages from the frontend.  Note that
// we must *never* send anything directly to the connection; all communication
// must go through queryResultCh.  We're also not responsible for doing any
// cleanup in any case; that'll all be handled by mainLoop.
func (c *FrontendConnection) queryProcessingMainLoop() {
sessionLoop:
	for {
		var message fbcore.Message

		err := c.stream.Next(&message)
		if err != nil {
			c.setSessionError(err)
			break sessionLoop
		}

		switch message.MsgType() {
		case fbproto.MsgQueryQ:
			query, err := fbproto.ReadQuery(&message)
			if err != nil {
				c.setSessionError(err)
				break sessionLoop
			}
			err = c.processQuery(query.Query)
			if err != nil {
				c.setSessionError(err)
				break sessionLoop
			}

		case fbproto.MsgTerminateX:
			c.setSessionError(errGracefulTermination)
			break sessionLoop

		default:
			panic(fmt.Sprintf("oopsie daisies %+#v", message))
		}
	}

	// wake mainLoop to clean up
	close(c.queryResultCh)
}

func (c *FrontendConnection) sendNotification(n *pq.Notification) error {
	var message fbcore.Message

	buf := &bytes.Buffer{}
	fbbuf.WriteInt32(buf, int32(n.BePid))
	fbbuf.WriteCString(buf, n.Channel)
	fbbuf.WriteCString(buf, n.Extra)
	message.InitFromBytes(fbproto.MsgNotificationResponseA, buf.Bytes())

	err := c.stream.Send(&message)
	if err != nil {
		return err
	}
	return c.stream.Flush()
}

func (c *FrontendConnection) setSessionError(err error) {
	c.lock.Lock()
	if c.err == nil {
		c.err = err
	}
	c.lock.Unlock()
}

func (c *FrontendConnection) mainLoop(dbcfg VirtualDatabaseConfiguration) {
	if !c.startup(dbcfg) {
		return
	}

	go c.queryProcessingMainLoop()

mainLoop:
	for {
		select {
		case n := <-c.notify:
			if len(c.notify) >= cap(c.notify)-1 {
				c.fatal(errClientCouldNotKeepUp)
				break mainLoop
			}

			if err := c.sendNotification(n); err != nil {
				c.setSessionError(err)
				break mainLoop
			}
		case _ = <-c.connStatusNotifier:
			c.fatal(errLostServerConnection)
			break mainLoop

		case res, ok := <-c.queryResultCh:
			if !ok {
				// queryProcessingMainLoop terminated, we're done
				break mainLoop
			}

			err := res.Respond(c)
			if err != nil {
				c.setSessionError(err)
				break mainLoop
			}
		}
	}

	_ = c.stream.Close()
	// wait for queryProcessingMainLoop to finish
	for range c.queryResultCh {
	}

	// Done with this client.  Log the error if necessary.
	switch c.err {
	case errLostServerConnection:
		// Already logged, no need to recite the fact that we're throwing
		// everyone out.
	case errGracefulTermination:
		// This is fine
	default:
		elog.Logf("client %s disconnected: %s\n", c, c.err)
	}
}
