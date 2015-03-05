allas
=====

Introduction
------------

_allas_ is a connection pooler for [PostgreSQL](http://www.postgresql.org)
which only supports LISTEN / NOTIFY.  The idea is to allow the application to
use e.g. pgbouncer in transaction pooling mode while only receiving
notifications through _allas_.  _allas_ only uses a single PostgreSQL
connection and takes care of LISTENing and UNLISTENing appropriately on that
connection to make sure all of its clients get the set of notifications they're
interested in.

How to build
------------

Having installed a reasonably modern version of Go, run: `go get
github.com/johto/allas`.  This should produce a binary under `$GOPATH/bin`.

Configuration
-------------

The configuration file is a simple JSON object.  It is not documented in detail
here since it will likely be replaced with something better in the near future.
Here's an example configuration file:

```
{
    "listen": {
        "port": 6433,
        "host": "localhost"
    },
    "connect": "host=localhost port=5432 sslmode=disable",
    "startup_parameters": {
        "server_version": "9.1.15"
    },
    "databases": [
        {
            "name": "allas",
            "auth": {
                "method": "md5",
				"user": "allas",
				"password": "s3cret"
            }
        }
    ]
}
```

"connect" is a [pq](http://godoc.org/github.com/lib/pq) connection string.  It
supports many of libpq's options.
