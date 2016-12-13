package buf

import (
	"bytes"
	"encoding/binary"
	"io"
)

// WriteInt16 writes the bytes of the int16 val to the io.Writer w
// in big-endian byte order. It returns the number of bytes written
// and any error encountered in writing. Like io.Writer, it will
// return a non-nil error if not all bytes are written.
func WriteInt16(w io.Writer, val int16) (n int, err error) {
	var be [2]byte
	valBytes := be[0:2]
	binary.BigEndian.PutUint16(valBytes, uint16(val))
	return w.Write(valBytes)
}

// WriteInt32 writes the bytes of the int32 val to the io.Writer w
// in big-endian byte order. It returns the number of bytes written
// and any error encountered in writing. Like io.Writer, it will
// return a non-nil error if not all bytes are written.
func WriteInt32(w io.Writer, val int32) (n int, err error) {
	return WriteUint32(w, uint32(val))
}

// WriteUint32 writes the bytes of the uint32 val to the io.Writer w
// in big-endian byte order. It returns the number of bytes written
// and any error encountered in writing. Like io.Writer, it will
// return a non-nil error if not all bytes are written.
func WriteUint32(w io.Writer, val uint32) (n int, err error) {
	var be [4]byte
	valBytes := be[0:4]
	binary.BigEndian.PutUint32(valBytes, val)

	return w.Write(valBytes)
}

// WriteCString writes the bytes of the string val to the io.Writer w
// in UTF-8 encoding, followed by a null termination byte (i.e., the
// standard C representation of a string). Like io.Writer, it will
// return a non-nil error if not all bytes are written.
func WriteCString(w io.Writer, val string) (n int, err error) {
	n, err = w.Write([]byte(val))
	if err != nil {
		return n, err
	}
	_, err = w.Write([]byte{'\000'})
	return n + 1, err
}

// ReadInt16 reads a 16-bit signed integer from the io.Reader r in
// big-endian byte order. Note that if an error is encountered when
// reading, it will be returned along with the value 0. An EOF error
// is returned when no bytes could be read; an UnexpectedEOF if some
// bytes were read first.
func ReadInt16(r io.Reader) (int16, error) {
	var be [2]byte
	valBytes := be[0:2]
	if _, err := io.ReadFull(r, valBytes); err != nil {
		return 0, err
	}

	return int16(binary.BigEndian.Uint16(valBytes)), nil
}

// ReadUint16 reads a 16-bit unsigned integer from the io.Reader r in
// big-endian byte order. Note that if an error is encountered when
// reading, it will be returned along with the value 0. An EOF error
// is returned when no bytes could be read; an UnexpectedEOF if some
// bytes were read first.
func ReadUint16(r io.Reader) (uint16, error) {
	var be [2]byte
	valBytes := be[0:2]
	if _, err := io.ReadFull(r, valBytes); err != nil {
		return 0, err
	}

	return uint16(binary.BigEndian.Uint16(valBytes)), nil
}

// ReadInt32 reads a 32-bit signed integer from the io.Reader r in
// big-endian byte order. Note that if an error is encountered when
// reading, it will be returned along with the value 0. An EOF error
// is returned when no bytes could be read; an UnexpectedEOF if some
// bytes were read first.
func ReadInt32(r io.Reader) (int32, error) {
	var be [4]byte
	valBytes := be[0:4]
	if _, err := io.ReadFull(r, valBytes); err != nil {
		return 0, err
	}

	return int32(binary.BigEndian.Uint32(valBytes)), nil
}

// ReadUint32 reads a 32-bit unsigned integer from the io.Reader r in
// big-endian byte order. Note that if an error is encountered when
// reading, it will be returned along with the value 0. An EOF error
// is returned when no bytes could be read; an UnexpectedEOF if some
// bytes were read first.
func ReadUint32(r io.Reader) (ret uint32, err error) {
	var be [4]byte
	valBytes := be[0:4]
	if _, err = io.ReadFull(r, valBytes); err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint32(valBytes), nil
}

// ReadUint32FromBuffer reads a 32-bit unsigned integer from the
// bytes.Buffer r in big-endian byte order.
func ReadUint32FromBuffer(r *bytes.Buffer) uint32 {
	return binary.BigEndian.Uint32(r.Next(4))
}

// ReadCString reads a null-terminated string in UTF-8 encoding from
// the io.Reader r. If an error is encountered in decoding, it returns
// an empty string and the error.
func ReadCString(r io.Reader) (s string, err error) {
	var be [1]byte
	charBuf := be[0:1]

	var accum bytes.Buffer

	for {
		n, err := r.Read(charBuf)

		if err != nil {
			return "", err
		}

		// Handle the case of no error, yet no bytes were
		// retrieved.
		if n < 1 {
			continue
		}

		switch charBuf[0] {
		case '\000':
			return string(accum.Bytes()), nil
		default:
			accum.Write(charBuf)
		}
	}
}

// ReadByte reads a single byte from the io.Reader r. If an error is
// encountered in reading, it returns 0 and the error.
func ReadByte(r io.Reader) (ret byte, err error) {
	var be [1]byte
	valBytes := be[0:1]

	if _, err = io.ReadFull(r, valBytes); err != nil {
		return 0, err
	}

	return valBytes[0], nil
}
