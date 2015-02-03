package main

import (
	fbcore "github.com/uhoh-itsmaciek/femebe/core"

	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
)

type Frontend interface {
	// Write msg to the frontend's message stream.  The message might not be
	// sent before FlushStream is called.
	WriteMessage(msg *fbcore.Message) error
	FlushStream() error

	// Starts delivering notifications on the specified channel, or returns an
	// error if the listen request could not be satisfied.  If the frontend is
	// already listening on the channel, error will be nil.
	Listen(channel string) error

	Unlisten(channel string) error

	UnlistenAll() error
}

type AuthConfig struct {
	method   string
	user     string
	password string
}

type virtualDatabase struct {
	name string
	auth AuthConfig
}

type VirtualDatabaseConfiguration []virtualDatabase

func (c VirtualDatabaseConfiguration) find(dbname string) *virtualDatabase {
	for _, db := range c {
		if db.name == dbname {
			return &db
		}
	}
	return nil
}

// Finds a database and returns the authentication method
func (c VirtualDatabaseConfiguration) FindDatabase(name string) (authMethod string, ok bool) {
	db := c.find(name)
	if db == nil {
		return "", false
	}
	return db.auth.method, true
}

func (c VirtualDatabaseConfiguration) MD5Auth(dbname string, username string, salt []byte, password []byte) (success bool, err error) {
	if !bytes.HasPrefix(password, []byte{'m', 'd', '5'}) {
		return false, nil
	}
	password = password[3:]

	db := c.find(dbname)
	if db == nil {
		return false, fmt.Errorf("internal error: database %q disappeared", dbname)
	}

	if db.auth.user != username {
		return false, nil
	}

	md5 := func(input []byte) []byte {
		s := md5.Sum(input)
		return []byte(hex.EncodeToString(s[:]))
	}
	expected := md5(append(md5([]byte(db.auth.password+username)), salt...))
	expected = append(expected, 0)
	return bytes.Compare(expected, password) == 0, nil
}
