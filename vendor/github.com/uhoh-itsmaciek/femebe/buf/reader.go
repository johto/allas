// The same as the standard Reader, except allowing for no-allocation
// re-initialization, access to the underlying byte slice, and removal
// of all rune manipulation.  These alterations bear the copyright
// listed in the accompanying LICENSE file.
//
// Otherwise, the original copyright (referring to the original Go
// LICENSE file) is:
//
// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package buf

import (
	"errors"
	"io"
)

// re-initializes a reader
func (r *Reader) InitReader(b []byte) {
	r.s = b
	r.i = 0
}

// Like bytes.Buffer.Next
func (r *Reader) Next(n int) []byte {
	oldI := r.i
	r.i += n
	return r.s[oldI:r.i]
}

// Returns the complete slice underlying the Reader
func (r *Reader) Bytes() []byte {
	return r.s
}

// A Reader implements the io.Reader, io.ReaderAt, io.Seeker, and
// io.ByteScanner interfaces by reading from a byte slice.  Unlike a
// Buffer, a Reader is read-only and supports seeking.
type Reader struct {
	s []byte
	i int // current reading index
}

// Len returns the number of bytes of the unread portion of the
// slice.
func (r *Reader) Len() int {
	if r.i >= len(r.s) {
		return 0
	}
	return len(r.s) - r.i
}

func (r *Reader) Read(b []byte) (n int, err error) {
	if len(b) == 0 {
		return 0, nil
	}
	if r.i >= len(r.s) {
		return 0, io.EOF
	}
	n = copy(b, r.s[r.i:])
	r.i += n
	return
}

func (r *Reader) ReadAt(b []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, errors.New("bytes: invalid offset")
	}
	if off >= int64(len(r.s)) {
		return 0, io.EOF
	}
	n = copy(b, r.s[int(off):])
	if n < len(b) {
		err = io.EOF
	}
	return
}

func (r *Reader) ReadByte() (b byte, err error) {
	if r.i >= len(r.s) {
		return 0, io.EOF
	}
	b = r.s[r.i]
	r.i++
	return
}

func (r *Reader) UnreadByte() error {
	if r.i <= 0 {
		return errors.New("bytes.Reader: at beginning of slice")
	}
	r.i--
	return nil
}

// Seek implements the io.Seeker interface.
func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case 0:
		abs = offset
	case 1:
		abs = int64(r.i) + offset
	case 2:
		abs = int64(len(r.s)) + offset
	default:
		return 0, errors.New("bytes: invalid whence")
	}
	if abs < 0 {
		return 0, errors.New("bytes: negative position")
	}
	if abs >= 1<<31 {
		return 0, errors.New("bytes: position out of range")
	}
	r.i = int(abs)
	return abs, nil
}

// NewReader returns a new Reader reading from b.
func NewReader(b []byte) *Reader { return &Reader{b, 0} }
