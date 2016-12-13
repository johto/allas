package error

import (
	"fmt"
)

type ErrTooBig struct {
	error
}

type ErrWrongSize struct {
	error
}

type ErrStartupVersion struct {
	error
}

type ErrStartupFmt struct {
	error
}

type ErrBadTypeCode struct {
	error
}

func TooBig(format string, args ...interface{}) ErrTooBig {
	return ErrTooBig{fmt.Errorf(format, args...)}
}

func WrongSize(format string, args ...interface{}) ErrWrongSize {
	return ErrWrongSize{fmt.Errorf(format, args...)}
}

func StartupVersion(format string, args ...interface{}) ErrStartupVersion {
	return ErrStartupVersion{fmt.Errorf(format, args...)}
}

func StartupFmt(format string, args ...interface{}) ErrStartupFmt {
	return ErrStartupFmt{fmt.Errorf(format, args...)}
}

func BadTypeCode(code byte) ErrBadTypeCode {
	return ErrBadTypeCode{fmt.Errorf("Invalid message type %v", code)}
}
