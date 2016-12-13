package main

import (
	fbbuf "github.com/uhoh-itsmaciek/femebe/buf"
	fbcore "github.com/uhoh-itsmaciek/femebe/core"
	fbproto "github.com/uhoh-itsmaciek/femebe/proto"

	"github.com/johto/notifyutils/notifydispatcher"
	"github.com/lib/pq"

	"bytes"
	"bufio"
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

// QueryResult + Sync (yes/no)
type queryResultSync struct {
	Result QueryResult
	Sync bool
}

type frontendConnectionIO struct {
	// Reader reads directly from the wrapped Conn
	io.Reader

	// Writing is buffered to avoid superfluous system calls
	io.Writer

	c net.Conn
	bufw *bufio.Writer
}
func (fcio *frontendConnectionIO) Read(p []byte) (n int, err error) {
	return fcio.c.Read(p)
}
func (fcio *frontendConnectionIO) Write(p []byte) (n int, err error) {
	return fcio.bufw.Write(p)
}
func (fcio *frontendConnectionIO) Flush() error {
	return fcio.bufw.Flush()
}
func (fcio *frontendConnectionIO) Close() error {
	return fcio.c.Close()
}

type FrontendConnection struct {
	// immutable
	remoteAddr string

	stream     *fbcore.MessageStream
	dispatcher *notifydispatcher.NotifyDispatcher

	connStatusNotifier chan struct{}
	notify             chan *pq.Notification
	queryResultCh      chan queryResultSync

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
	io := &frontendConnectionIO{
		c: c,
		bufw: bufio.NewWriterSize(c, 128),
	}

	fc := &FrontendConnection{
		remoteAddr: c.RemoteAddr().String(),

		stream:     fbcore.NewFrontendStream(io),
		dispatcher: dispatcher,

		connStatusNotifier: connStatusNotifier,
		notify:             make(chan *pq.Notification, 32),
		queryResultCh:      make(chan queryResultSync, 8),

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

func (c *FrontendConnection) startup(startupParameters map[string]string, dbcfg VirtualDatabaseConfiguration) bool {
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
			err = c.FlushStream()
			if err != nil {
				elog.Logf("error during startup sequence: %s", err)
			}
		} else if fbproto.IsCancelRequest(&message) {
			_ = c.stream.Close()
			return false
		} else {
			elog.Warningf("unrecognized frontend message type 0x%x during startup", message.MsgType())
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

	for k, v := range startupParameters {
		buf := &bytes.Buffer{}
		fbbuf.WriteCString(buf, k)
		fbbuf.WriteCString(buf, v)
		message.InitFromBytes(fbproto.MsgParameterStatusS, buf.Bytes())
		err = c.WriteMessage(&message)
		if err != nil {
			elog.Logf("error during startup sequence: %s", err)
			return false
		}
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

func (c *FrontendConnection) sendReadyForQuery() error {
	var message fbcore.Message

	fbproto.InitReadyForQuery(&message, fbproto.RfqIdle)
	err := c.WriteMessage(&message)
	if err != nil {
		return err
	}
	return c.FlushStream()
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

func (c *FrontendConnection) readParseMessage(msg *fbcore.Message) (queryString string, err error) {
	statementName, err := fbbuf.ReadCString(msg.Payload())
	if err != nil {
		return "", err
	}
	if statementName != "" {
		return "", fmt.Errorf("attempted to use statement name %q; only unnamed statements are supported", statementName)
	}
	queryString, err = fbbuf.ReadCString(msg.Payload())
	if err != nil {
		return "", err
	}
	numParamTypes, err := fbbuf.ReadInt16(msg.Payload())
	if err != nil {
		return "", err
	}
	if numParamTypes != 0 {
		return "", fmt.Errorf("attempted to prepare a statement with %d param types", numParamTypes)
	}
	// TODO: ensure we're at the end of the packet
	return queryString, nil
}

func (c *FrontendConnection) readExecuteMessage(msg *fbcore.Message) error {
	statementName, err := fbbuf.ReadCString(msg.Payload())
	if err != nil {
		return err
	}
	if statementName != "" {
		return fmt.Errorf("attempted to use statement name %q; only unnamed statements are supported", statementName)
	}
	// ignore maxRowCount
	_, err = fbbuf.ReadInt32(msg.Payload())
	// TODO: ensure we're at the end of the packet
	return err
}

func (c *FrontendConnection) readDescribeMessage(msg *fbcore.Message) (byte, error) {
	typ, err := fbbuf.ReadByte(msg.Payload())
	if err != nil {
		return 0, err
	}
	if typ != 'S' && typ != 'P' {
		return 0, fmt.Errorf("invalid type %q", typ)
	}
	statementName, err := fbbuf.ReadCString(msg.Payload())
	if err != nil {
		return 0, err
	}
	if statementName != "" {
		return 0, fmt.Errorf("tried to use statement/portal name %q; only unnamed statements and portals are supported", statementName)
	}
	// TODO: ensure we're at the end of the packet
	return typ, nil
}

func (c *FrontendConnection) readBindMessage(msg *fbcore.Message) error {
	portalName, err := fbbuf.ReadCString(msg.Payload())
	if err != nil {
		return err
	}
	if portalName != "" {
		return fmt.Errorf("attempted to bind to a named portal %q; only the unnamed portal is supported", portalName)
	}
	statementName, err := fbbuf.ReadCString(msg.Payload())
	if err != nil {
		return err
	}
	if statementName != "" {
		return fmt.Errorf("attempted to bind statement %q, even though it has not been parsed yet", statementName)
	}
	numParamFormats, err := fbbuf.ReadInt16(msg.Payload())
	if err != nil {
		return err
	}
	if numParamFormats != 0 {
		return fmt.Errorf("the number of parameter formats (%d) does not match the number of parameters in the query (0)", numParamFormats)
	}
	numParameters, err := fbbuf.ReadInt16(msg.Payload())
	if err != nil {
		return err
	}
	if numParameters != 0 {
		return fmt.Errorf("the number of parameters provided by the client (%d) does not match the number of parameters in the query (0)", numParameters)
	}
	// TODO: ensure we're at the end of the packet
	return nil
}

func (c *FrontendConnection) discardUntilSync() error {
	var message fbcore.Message

	for {
		err := c.stream.Next(&message)
		if err != nil {
			return err
		}

		switch message.MsgType() {
		case fbproto.MsgSyncS:
			_, err = message.Force()
			return nil
		default:
			_, err = message.Force()
		}
		if err != nil {
			return err
		}
	}
}

// This is the main loop for processing messages from the frontend.  Note that
// we must *never* send anything directly to the connection; all communication
// must go through queryResultCh.  We're also not responsible for doing any
// cleanup in any case; that'll all be handled by mainLoop.
func (c *FrontendConnection) queryProcessingMainLoop() {
	var unnamedStatement FrontendQuery

	var queryResult QueryResult
	var sendReadyForQuery bool

sessionLoop:
	for {
		var message fbcore.Message

		err := c.stream.Next(&message)
		if err != nil {
			c.setSessionError(err)
			break sessionLoop
		}

		queryResult = nil
		sendReadyForQuery = false

		switch message.MsgType() {
		case fbproto.MsgParseP:
			queryString, err := c.readParseMessage(&message)
			if err != nil {
				c.setSessionError(err)
				break sessionLoop
			}
			unnamedStatement, err = ParseQuery(queryString)
			if err != nil {
				queryResult = NewErrorResponse("42601", err.Error())
				c.queryResultCh <- queryResultSync{queryResult, false}

				err = c.discardUntilSync()
				if err != nil {
					c.setSessionError(err)
					break sessionLoop
				}

				queryResult = NewNopResponder()
				sendReadyForQuery = true
			} else {
				queryResult = NewParseComplete()
				sendReadyForQuery = false
			}

		case fbproto.MsgExecuteE:
			err = c.readExecuteMessage(&message)
			if unnamedStatement == nil {
				err = fmt.Errorf("attempted to execute the unnamed prepared statement when one does not exist")
				c.setSessionError(err)
				break sessionLoop
			}
			queryResult, err = unnamedStatement.Process(c)
			if err != nil {
				c.setSessionError(err)
				break sessionLoop
			}
			sendReadyForQuery = false
			// Disallow reuse; not exactly following the protocol to the letter,
			// but apps reusing the unnamed statement should not exist, either.
			unnamedStatement = nil

		case fbproto.MsgDescribeD:
			_, err = c.readDescribeMessage(&message)
			if err != nil {
				c.setSessionError(err)
				break sessionLoop
			}
			if unnamedStatement == nil {
				err = fmt.Errorf("attempted to describe the unnamed prepared statement when one does not exist")
				c.setSessionError(err)
				break sessionLoop
			}
			queryResult = unnamedStatement.Describe()
			sendReadyForQuery = false

		case fbproto.MsgBindB:
			err = c.readBindMessage(&message)
			if err != nil {
				c.setSessionError(err)
				break sessionLoop
			}
			queryResult = NewBindComplete()
			sendReadyForQuery = false

		case fbproto.MsgSyncS:
			queryResult = NewNopResponder()
			sendReadyForQuery = true

		case fbproto.MsgQueryQ:
			query, err := fbproto.ReadQuery(&message)
			if err != nil {
				c.setSessionError(err)
				break sessionLoop
			}
			q, err := ParseQuery(query.Query)
			if err != nil {
				queryResult = NewErrorResponse("42601", err.Error())
			} else {
				resultDescription := q.Describe()
				// Special case in SimpleQuery processing: we only send the
				// Describe() response over if it's a RowDescription.  This is
				// somewhat magical and very weird, but that's what the upstream
				// server does, so we ought to do the same thing here.
				if DescriptionIsRowDescription(resultDescription) {
					c.queryResultCh <- queryResultSync{resultDescription, false}
				}
				queryResult, err = q.Process(c)
				if err != nil {
					c.setSessionError(err)
					break sessionLoop
				}
			}
			sendReadyForQuery = true
			// Simple Query also clears the unnamed statement; this matches
			// what Postgres does.
			unnamedStatement = nil

		case fbproto.MsgTerminateX:
			c.setSessionError(errGracefulTermination)
			break sessionLoop

		default:
			c.setSessionError(fmt.Errorf("unrecognized frontend message type 0x%x", message.MsgType()))
			break sessionLoop
		}

		if queryResult != nil {
			c.queryResultCh <- queryResultSync{queryResult, sendReadyForQuery}
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

	return c.WriteAndFlush(&message)
}

func (c *FrontendConnection) setSessionError(err error) {
	c.lock.Lock()
	if c.err == nil {
		c.err = err
	}
	c.lock.Unlock()
}

func (c *FrontendConnection) mainLoop(startupParameters map[string]string, dbcfg VirtualDatabaseConfiguration) {
	if !c.startup(startupParameters, dbcfg) {
		return
	}

	go c.queryProcessingMainLoop()

mainLoop:
	for {
		select {
		case n := <-c.notify:
			if len(c.notify) >= cap(c.notify)-1 {
				MetricSlowClientsTerminated.Inc()
				c.fatal(errClientCouldNotKeepUp)
				break mainLoop
			}

			if err := c.sendNotification(n); err != nil {
				c.setSessionError(err)
				break mainLoop
			}
			MetricNotificationsDispatched.Inc()
		case _ = <-c.connStatusNotifier:
			c.fatal(errLostServerConnection)
			break mainLoop

		case resSync, ok := <-c.queryResultCh:
			if !ok {
				// queryProcessingMainLoop terminated, we're done
				break mainLoop
			}

			err := resSync.Result.Respond(c)
			if err != nil {
				c.setSessionError(err)
				break mainLoop
			}
			if resSync.Sync {
				err = c.sendReadyForQuery()
				if err != nil {
					c.setSessionError(err)
					break mainLoop
				}
			}
		}
	}

	_ = c.stream.Close()
	// wait for queryProcessingMainLoop to finish
	for _ = range c.queryResultCh {
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

	// finally, close all the channels the client was listening on
	for channel := range c.listenChannels {
		err := c.dispatcher.Unlisten(channel, c.notify)
		if err != nil {
			elog.Warningf("could not unlisten: %s\n", err)
		}
	}
	c.listenChannels = nil
}
