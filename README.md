allas
=====

Introduction
------------

_allas_ is a connection pooler for [PostgreSQL](http://www.postgresql.org)
which only supports LISTEN / NOTIFY.  The idea is to allow the application to
use e.g. pgbouncer in transaction pooling mode while only receiving
notifications through _allas_.

How to build
------------

Having installed a reasonably modern version of Go, run: `go get
github.com/johto/allas`.  This should produce a binary under your $GOPATH.

Configuration
-------------

TODO
