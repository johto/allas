package main

import (
	fbbuf "github.com/uhoh-itsmaciek/femebe/buf"
	fbcore "github.com/uhoh-itsmaciek/femebe/core"
	fbproto "github.com/uhoh-itsmaciek/femebe/proto"

	"bytes"
)

// These are the different query results

type QueryResult interface {
	Respond(f Frontend) error
}

// commandComplete is a CommandComplete + ReadyForQuery.  The representation is
// a string, which is the command tag.
type commandComplete string

func (qr commandComplete) Respond(f Frontend) error {
	var message fbcore.Message

	fbproto.InitCommandComplete(&message, string(qr))
	err := f.WriteMessage(&message)
	if err != nil {
		return err
	}
	return sendReadyForQuery(f)
}

// emptyQueryResponse is an EmptyQueryResponse + ReadyForQuery.
type emptyQueryResponse struct{}

func (eqr emptyQueryResponse) Respond(f Frontend) error {
	var message fbcore.Message

	message.InitFromBytes(fbproto.MsgEmptyQueryResponseI, nil)
	err := f.WriteMessage(&message)
	if err != nil {
		return err
	}
	return sendReadyForQuery(f)
}

// errorResponse is an ErrorResponse + ReadyForQuery.  The representation is a
// string, which is the error message.
type errorResponse struct {
	sqlstate     string
	errorMessage string
}

func (qr errorResponse) Respond(f Frontend) error {
	var message fbcore.Message

	buf := &bytes.Buffer{}
	buf.WriteByte('S')
	fbbuf.WriteCString(buf, "ERROR")
	buf.WriteByte('C')
	fbbuf.WriteCString(buf, qr.sqlstate)
	buf.WriteByte('M')
	fbbuf.WriteCString(buf, qr.errorMessage)
	buf.WriteByte('\x00')

	message.InitFromBytes(fbproto.MsgErrorResponseE, buf.Bytes())

	err := f.WriteMessage(&message)
	if err != nil {
		return err
	}
	return sendReadyForQuery(f)
}

func NewErrorResponse(sqlstate, errorMessage string) QueryResult {
	return errorResponse{sqlstate, errorMessage}
}

// Helper functions for query results

func sendReadyForQuery(f Frontend) error {
	var message fbcore.Message

	fbproto.InitReadyForQuery(&message, fbproto.RfqIdle)
	err := f.WriteMessage(&message)
	if err != nil {
		return err
	}
	return f.FlushStream()
}

// Different query types below

type FrontendQuery interface {
	Process(fe Frontend) (QueryResult, error)
}

type listenRequest struct {
	channel string
}

func (q listenRequest) Process(fe Frontend) (QueryResult, error) {
	err := fe.Listen(q.channel)
	if err != nil {
		// This should probably never happen, right?  It's OK to just kill the
		// frontend?
		return nil, err
	}
	return commandComplete("LISTEN"), nil
}

func NewListenRequest(channel string) FrontendQuery {
	return listenRequest{channel}
}

type unlistenRequest struct {
	channel  string
	wildcard bool
}

func (q unlistenRequest) Process(fe Frontend) (QueryResult, error) {
	var err error

	if q.wildcard {
		err = fe.UnlistenAll()
	} else {
		err = fe.Unlisten(q.channel)
	}
	if err != nil {
		// XXX see ListenRequest.Process
		return nil, err
	}
	return commandComplete("UNLISTEN"), nil
}

func NewUnlistenRequest(channel string) FrontendQuery {
	return unlistenRequest{channel, false}
}

func NewWildcardUnlistenRequest() FrontendQuery {
	return unlistenRequest{"", true}
}

type emptyQuery struct {
}

func (q emptyQuery) Process(fe Frontend) (QueryResult, error) {
	return emptyQueryResponse{}, nil
}

func NewEmptyQuery() FrontendQuery {
	return emptyQuery{}
}

type trivialSelectResult struct {
}

func (r trivialSelectResult) Respond(f Frontend) error {
	var msg fbcore.Message

	tupleDesc := fbproto.FieldDescription{
		Name:       "?column?",
		TableOid:   0,
		TableAttNo: 0,
		TypeOid:    23,
		TypLen:     4,
		Atttypmod:  0,
		Format:     0,
	}
	fbproto.InitRowDescription(&msg, []fbproto.FieldDescription{tupleDesc})
	err := f.WriteMessage(&msg)
	if err != nil {
		return err
	}

	buf := &bytes.Buffer{}
	fbbuf.WriteInt16(buf, 1)
	fbbuf.WriteInt32(buf, 1)
	buf.WriteByte('1')
	msg.InitFromBytes(fbproto.MsgDataRowD, buf.Bytes())
	err = f.WriteMessage(&msg)
	if err != nil {
		return err
	}

	return commandComplete("SELECT").Respond(f)
}

type trivialSelect struct {
}

func (q trivialSelect) Process(fe Frontend) (QueryResult, error) {
	return trivialSelectResult{}, nil
}

func NewTrivialSelect() FrontendQuery {
	return trivialSelect{}
}
