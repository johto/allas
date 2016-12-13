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

The configuration file uses a JSON format and is organized into sections.  The
top level structure is a JSON object, with the following keys ("sections"):

###### listen

`listen` specifies how `allas` listens to new connections.  It has three possible options:

  1. **port** (integer) specifies the port to listen on.
  2. **host** (string) specifies the address to listen on.  The asterisk
  (`"*"`) can be used to listen on all TCP interfaces, or an absolute path can
  be used to listen on a UNIX domain socket.
  3. **keepalive** (boolean) specifies whether TCP keepalives should be enabled or not.

###### connect

`connect` is a [pq](http://godoc.org/github.com/lib/pq) connection string.  It
supports many of libpq's options.

###### startup\_parameters

`startup_parameters` is a JSON object specifying the list of "startup
parameters" (such as the server's version number) to send to each client when
they connect.

###### databases

`databases` is an array of JSON objects with the following keys:

  1. **name** (string) specifies the name of the database.
  2. **auth** (object) is described in the section `Database
  authentication`, below.

#### Database authentication

The `auth` key of a database configuration section is a JSON object with a
combination of the following keys:

  1. **method** (string) is the authentication method used.  There are only two
  values values: "trust" and "md5".  Both match their respective counterpart in
  PostgreSQL HBA configuration.
  2. **user** (string) is the user name the user has to pass to match the
  authentication method.
  3. **password** (string) is a clear-text copy of the password the client
  should use for authentication.

Configuration example
---------------------

Here's an example configuration file:

```
{
    "listen": {
        "port": 6433,
        "host": "localhost"
    },
    "connect": "host=localhost port=5432 sslmode=disable",
    "startup_parameters": {
        "server_version": "9.1.24"
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

