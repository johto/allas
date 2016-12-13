package buf

import (
	"errors"
	"io"
)

// Idea: Change this so that the writer can only write infrount of the read pointer once
// but can write anywhere behind the Read pointer since the protocol does not need to have
// seek access...
// Result: not so much... To many edge cases

// Idea: compacting buffer

// A Reader implements the io.Reader, io.ReaderAt, io.Seeker, and
// io.ByteScanner interfaces by reading from a byte slice.  Unlike a
// Buffer, a Reader is read-only and supports seeking.

//should probably be int64s
//Should add left counter
type PackBuffer struct {
	r int
	w int
	s []byte
}

func NewPackBuffer(size int) *PackBuffer {
	b := &PackBuffer{0, 0, make([]byte, size, size)}
	return b
}

// re-initializes a reader
func (b *PackBuffer) InitPackBuffer(bin []byte) {
	b.r = 0
	b.w = 0
	b.s = bin
}

// Like bytes.Buffer.Next
func (b *PackBuffer) Next(n int) []byte {
	oldR := b.r
	b.r += n
	return b.s[oldR:b.r]
}

// Returns the complete slice underlying the Reader
func (b *PackBuffer) Bytes() []byte {
	return b.s
}

// Len returns the number of bytes of the unread portion of the
// slice.
// Note readLeft now does what this function did...
func (b *PackBuffer) Width() int {
	return len(b.s)
}

//The amount left to be read
func (b *PackBuffer) ReadLen() int {
	if b.w >= len(b.s) {
		return 0
	}
	return (b.w - b.r)
}

//the amount of room left to write before there is no room or buffer needs to be compacted
func (b *PackBuffer) WriteLen() int {
	if b.w >= len(b.s) {
		return 0
	}
	return len(b.s) - b.w
}

func (b *PackBuffer) Read(outB []byte) (n int, err error) {
	if len(outB) == 0 {
		return 0, nil
	}
	if b.r >= len(b.s) {
		return 0, io.EOF
	}
	if b.r == b.w {
		return 0, io.EOF
	}
	n = copy(outB, b.s[b.r:b.w])
	b.r += n
	return n, nil
}

//This should not be supported because the data may have been compacted...
func (b *PackBuffer) ReadAt(outB []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, errors.New("bytes: invalid offset")
	}
	if off >= int64(len(b.s)) {
		return 0, io.EOF
	}
	n = copy(outB, b.s[int(off):])
	if n < len(outB) {
		err = io.EOF
	}
	return
}

func (b *PackBuffer) ReadByte() (outB byte, err error) {
	if b.r >= len(b.s) {
		return 0, io.EOF
	}
	outB = b.s[b.r]
	b.r++
	return
}

func (b *PackBuffer) UnreadByte() error {
	if b.r <= 0 {
		return errors.New("bytes.Reader: at beginning of slice")
	}
	b.r--
	return nil
}

// Seek implements the io.Seeker interface.
// This again feels like you don't know if your buffer has been compacted...
func (b *PackBuffer) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case 0:
		abs = offset
	case 1:
		abs = int64(b.r) + offset
	case 2:
		abs = int64(len(b.s)) + offset
	default:
		return 0, errors.New("bytes: invalid whence")
	}
	if abs < 0 {
		return 0, errors.New("bytes: negative position")
	}
	if abs >= 1<<31 {
		return 0, errors.New("bytes: position out of range")
	}
	b.r = int(abs)
	return abs, nil
}

//Compacts and sets read and write cursors to where it belongs
func (b *PackBuffer) Compact(squash bool) (err error) {
	if squash {
		b.r = 0
		b.w = 0
	} else {
		if b.r == b.w {
			b.r = 0
			b.w = 0
		} else {
			var temp []byte
			count := copy(temp, b.s[b.r:b.w])
			if count == 0 {
				return
			}
			copy(b.s, temp)
			b.w = b.r - b.w
			b.r = 0
		}
	}
	return
}

//Writes without compacting if possible, if needed it does, if there is no room
//then it returns an error
func (b *PackBuffer) Write(p []byte) (n int, err error) {

	space := b.WriteLen()
	if space < len(p) {
		n = copy(b.s[b.w:], p)
		b.w = b.w + n
	} else {
		b.Compact(false)
		space = b.WriteLen()
		if space > len(p) {
			n = copy(b.s[b.w:], p[:])
		} else {
			n = copy(b.s[b.w:], p[:space])
		}
	}
	if n == 0 {
		return 0, errors.New("No room left in buffer")
	}
	return n, nil
}

func (b *PackBuffer) Close() error {
	b.s = nil
	return nil
}

func (b *PackBuffer) ReadPos() int {
	return b.r
}

func (b *PackBuffer) WritePos() int {
	return b.w
}
