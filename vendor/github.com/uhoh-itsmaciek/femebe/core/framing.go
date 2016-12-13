package core

import (
	"encoding/binary"
	"errors"
	"github.com/uhoh-itsmaciek/femebe/buf"
	"io"
	"io/ioutil"
)

var ErrTooLarge = errors.New("Message buffering size limit exceeded")

const MsgTypeFirst = '\000'

type Message struct {
	// Constant-width header
	msgType byte
	sz      uint32

	buffered buf.Reader
	union    io.Reader

	// The rest of the message yet to be read.
	future io.Reader
}

func (m *Message) MsgType() byte {
	return m.msgType
}

func (m *Message) Payload() io.Reader {
	return m.union
}

func (m *Message) Size() uint32 {
	return m.sz
}

func (m *Message) IsBuffered() bool {
	return m.future == nil
}

func (m *Message) Discard() error {
	if m.IsBuffered() {
		return nil
	}
	_, err := io.Copy(ioutil.Discard, m.future)
	m.future = nil
	return err
}

func (m *Message) Force() ([]byte, error) {
	if m.IsBuffered() {
		return m.buffered.Bytes(), nil
	}

	payloadSz := m.Size() - 4
	curBuf := m.buffered.Bytes()
	var payload []byte

	// Try to reuse the buffer if possible
	if uint32(cap(curBuf)) >= payloadSz {
		payload = curBuf[:payloadSz]
	} else {
		payload = make([]byte, payloadSz)
	}
	_, err := io.ReadFull(m.union, payload)

	m.buffered.InitReader(payload)
	m.future = nil

	return m.buffered.Bytes(), err
}

func (m *Message) WriteTo(w io.Writer) (_ int64, err error) {
	var bufBack [4]byte
	var totalN int64

	if mt := m.MsgType(); mt != MsgTypeFirst {
		n, err := w.Write([]byte{mt})
		totalN += int64(n)
		if err != nil {
			return totalN, err
		}
	}

	// Write message size integer to the stream

	buf := bufBack[:]
	binary.BigEndian.PutUint32(buf, m.Size())
	nMsgSz, err := w.Write(buf)
	totalN += int64(nMsgSz)
	if err != nil {
		return totalN, err
	}

	// Write the actual payload
	var nPayload int64

	if m.future == nil {
		// Fast path for fully buffered messages
		var nPayloadSm int
		nPayloadSm, err = w.Write(m.buffered.Bytes())
		nPayload = int64(nPayloadSm)
	} else {
		// Slow generic path
		nPayload, err = io.Copy(w, m.Payload())
	}

	totalN += nPayload
	return totalN, err
}

func (m *Message) baseInitMessage(msgType byte, size uint32) {
	m.msgType = msgType
	m.sz = size
}

func (m *Message) InitFromBytes(msgType byte, payload []byte) {
	m.baseInitMessage(msgType, uint32(len(payload))+4)
	m.future = nil
	m.buffered.InitReader(payload)
	m.union = &m.buffered
}

func (m *Message) InitPromise(msgType byte, size uint32,
	buffered []byte, r io.Reader) {
	m.baseInitMessage(msgType, size)
	m.buffered.InitReader(buffered)

	remaining := int64(size - 4 - uint32(len(buffered)))
	m.future = io.LimitReader(r, remaining)

	m.union = io.MultiReader(&m.buffered, m.future)
}

func (m *Message) InitFromMessage(src *Message) {
	payloadBytes, err := src.Force()
	if err != nil {
		panic(err)
	}
	dstBytes := make([]byte, len(payloadBytes), len(payloadBytes))
	copy(dstBytes, payloadBytes)
	m.InitFromBytes(src.MsgType(), dstBytes)
}
