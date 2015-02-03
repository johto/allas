package main

import (
	"testing"
)

func TestQueryParser(t *testing.T) {
	var tests = []struct {
		input   string
		outcome string
		err     string
	}{
		{"", "EmptyQuery", ""},
		{"--", "EmptyQuery", ""},
		{"/**/", "EmptyQuery", ""},
		{"/*/**/*/", "EmptyQuery", ""},
		{"/*/**/", "error", errQueryParserUnexpectedEOF.Error()},
		{"\xFF\x00", "error", errQueryParserInputNotUtf8.Error()},
		{"-", "error", errQueryParserUnexpectedEOF.Error()},
		{"select", "error", errQueryParserUnexpectedEOF.Error()},
		{"select 1", "TrivialSelect", ""},
		{"select 1;", "TrivialSelect", ""},
		{"select 1 ", "TrivialSelect", ""},
		{"select 1 --", "TrivialSelect", ""},
		{"select 1 /*/**/*/", "TrivialSelect", ""},
		{"select 1;  ", "TrivialSelect", ""},
		{"select 1;  f", "error", "garbage after semicolon"},
		{"select 2", "error", "parse error"},
		{`listen "foo"`, "ListenRequest", ""},
		{`listen *`, "error", `parse error: unexpected token "asterisk"`},
		{`unlisten *`, "UnlistenRequest", ""},
		{"notify", "error", `parse error at or near "notify"`},
	}

	for n, ts := range tests {
		q, err := ParseQuery(ts.input)
		switch ts.outcome {
		case "error":
			if err == nil {
				t.Errorf("test %d failed: err is nil; expected %s", ts.err)
			} else if err.Error() != ts.err {
				t.Errorf("test %d failed: err %q != %q", n, err.Error(), ts.err)
			}
		case "EmptyQuery":
			if err != nil {
				t.Errorf("test %d failed: unexpected error %q", n, err)
			} else {
				_, ok := q.(emptyQuery)
				if !ok {
					t.Errorf("test %d failed: unexpected msg %+#v; was expecting EmptyQuery", n, q)
				}
			}
		case "ListenRequest":
			if err != nil {
				t.Errorf("test %d failed: unexpected error %q", n, err)
			} else {
				_, ok := q.(listenRequest)
				if !ok {
					t.Errorf("test %d failed: unexpected msg %+#v; was expecting ListenRequest", n, q)
				}
			}
		case "UnlistenRequest":
			if err != nil {
				t.Errorf("test %d failed: unexpected error %q", n, err)
			} else {
				_, ok := q.(unlistenRequest)
				if !ok {
					t.Errorf("test %d failed: unexpected msg %+#v; was expecting UnlistenRequest", n, q)
				}
			}
		case "TrivialSelect":
			if err != nil {
				t.Errorf("test %d failed: unexpected error %q", n, err)
			} else {
				_, ok := q.(trivialSelect)
				if !ok {
					t.Errorf("test %d failed: unexpected msg %+#v; was expecting TrivialSelect", n, q)
				}
			}

		default:
			t.Fatalf("unexpected outcome %q", ts.outcome)
		}

	}
}
