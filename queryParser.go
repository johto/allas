package main

/*
 * This file contains a parser for a really small subset of the Postgres SQL
 * dialect.  The objective is to only support LISTEN, UNLISTEN and trivial
 * "ping"-type SELECT statements.  Many queries accepted by Postgres proper are
 * rejected, but that's fine for our purposes -- in fact, this parser probably
 * tries to support way too many corner cases already.
 */

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

/* possible token types returned by nextToken() */
type queryParserTokenType int

const (
	tokEOF queryParserTokenType = iota
	tokIdentifier
	tokDigit
	tokSemicolon
	tokStar
)

const (
	flagAllowEOF               uint32 = 1
	flagAllowQuotedIdentifiers        = 2
)

func (t queryParserTokenType) String() string {
	switch t {
	case tokEOF:
		return "EOF"
	case tokIdentifier:
		return "identifier"
	case tokDigit:
		return "digit"
	case tokSemicolon:
		return "semicolon"
	case tokStar:
		return "asterisk"
	default:
		panic(fmt.Sprintf("unrecognized token type %d", t))
	}
}

type queryParserToken struct {
	typ     queryParserTokenType
	payload string
}

// maximum query length, in characters
const maxQuerySize = 512

var (
	errQueryParserInputNotUtf8  = errors.New("invalid input syntax for encoding UTF-8")
	errQueryParserUnexpectedEOF = errors.New("unexpected EOF")
	errQueryTooLong             = errors.New("query length exceeds maximum allowed size")
)

// These should match src/backend/parser/scan.l
const whiteSpaceCharacters string = " \t\n\r\f"

func ParseQuery(rawinput string) (q FrontendQuery, err error) {
	var token queryParserToken

	if !utf8.ValidString(rawinput) {
		return nil, errQueryParserInputNotUtf8
	}
	if len(rawinput) >= maxQuerySize {
		return nil, errQueryTooLong
	}

	// hack for JDBC versions 9.1 through 9.3
	if rawinput == "SET extra_float_digits = 3" {
		return NewNopSetCommand(), nil
	}

	input := []rune(rawinput)
	input, err = nextToken(input, &token, flagAllowEOF)
	if err != nil {
		return nil, err
	} else if token.typ == tokEOF {
		return NewEmptyQuery(), nil
	} else if token.typ == tokSemicolon {
		return NewEmptyQuery(), semicolonOrEOF(input)
	} else if token.typ != tokIdentifier {
		return nil, fmt.Errorf("unexpected token type %q", token.typ)
	}

	switch token.payload {
	case "select":
		return parseSelect(input)
	case "listen":
		return parseListen(input)
	case "unlisten":
		return parseUnlisten(input)
	default:
		return nil, fmt.Errorf("parse error at or near %q", token.payload)
	}
}

func parseSelect(input []rune) (q FrontendQuery, err error) {
	var token queryParserToken

	input, err = nextToken(input, &token, 0)
	if err != nil {
		return nil, err
	} else if token.typ != tokDigit {
		return nil, fmt.Errorf("unexpected token type %q", token.typ)
	}

	/* must be at EOF */
	return NewTrivialSelect(), semicolonOrEOF(input)
}

func semicolonOrEOF(input []rune) error {
	var token queryParserToken

	input, err := nextToken(input, &token, flagAllowEOF)
	if err != nil {
		return err
	} else if token.typ == tokEOF {
		return nil
	} else if token.typ != tokSemicolon {
		return fmt.Errorf("unexpected data after query string")
	}

	input, err = nextToken(input, &token, flagAllowEOF)
	if err != nil {
		return err
	} else if token.typ == tokEOF {
		return nil
	} else {
		return fmt.Errorf("garbage after semicolon")
	}
}

func unexpectedToken(token queryParserToken) error {
	return fmt.Errorf("parse error: unexpected token %q", token.typ.String())
}

func parseListen(input []rune) (q FrontendQuery, err error) {
	var token queryParserToken

	input, err = nextToken(input, &token, flagAllowQuotedIdentifiers)
	if err != nil {
		return nil, err
	} else if token.typ == tokIdentifier {
		return NewListenRequest(token.payload), semicolonOrEOF(input)
	} else {
		return nil, unexpectedToken(token)
	}
}

func parseUnlisten(input []rune) (q FrontendQuery, err error) {
	var token queryParserToken

	input, err = nextToken(input, &token, flagAllowQuotedIdentifiers)
	if err != nil {
		return nil, err
	} else if token.typ == tokStar {
		return NewWildcardUnlistenRequest(), semicolonOrEOF(input)
	} else if token.typ == tokIdentifier {
		return NewUnlistenRequest(token.payload), semicolonOrEOF(input)
	} else {
		return nil, unexpectedToken(token)
	}
}

func nextToken(input []rune, token *queryParserToken, flags uint32) (rest []rune, err error) {

foundComment:
	input = stripLeadingWhitespace(input)
	if len(input) == 0 {
		if flags&flagAllowEOF > 0 {
			token.typ = tokEOF
			return nil, nil
		} else {
			return nil, errQueryParserUnexpectedEOF
		}
	}

	r := input[0]
	if flags&flagAllowQuotedIdentifiers > 0 && r == '"' {
		return readQuotedIdentifier(input[1:], token)
	} else if isIdentifierStart(r) {
		return readIdentifier(input, token)
	} else if r == '1' {
		return readDigit(input, token)
	} else if r == '-' || r == '/' {
		input, err = readCommentOrError(input)
		if err != nil {
			return nil, err
		}
		goto foundComment
	} else if r == ';' {
		token.typ = tokSemicolon
		return input[1:], nil
	} else if r == '*' {
		token.typ = tokStar
		return input[1:], nil
	} else {
		return nil, errors.New("parse error")
	}
}

func readCommentOrError(input []rune) (rest []rune, err error) {
	if len(input) < 2 {
		return nil, errQueryParserUnexpectedEOF
	}
	if input[0] == '-' && input[1] == '-' {
		input = input[2:]
		for len(input) > 0 && input[0] != '\r' && input[0] != '\n' {
			input = input[1:]
		}
		return input, err
	} else if input[0] == '/' && input[1] == '*' {
		input = input[2:]
		for {
			if len(input) < 2 {
				return nil, errQueryParserUnexpectedEOF
			}
			if input[0] == '*' && input[1] == '/' {
				return input[2:], nil
			} else if input[0] == '/' && input[1] == '*' {
				/* C-style comments nest; recurse */
				input, err = readCommentOrError(input)
				if err != nil {
					return nil, err
				}
			} else {
				input = input[1:]
			}
		}
	} else {
		return nil, fmt.Errorf("parse error at or near %q", string(input[:2]))
	}
}

func readDigit(input []rune, token *queryParserToken) (rest []rune, err error) {
	if len(input) == 1 {
		token.typ = tokDigit
		return nil, nil
	}

	/*
	 * We're not at the end yet; we allow anything *except* another digit.  In
	 * reality we only support whitespace and/or comments at the end, but this
	 * is not the place to enforce that.
	 */
	r := input[1]
	if r >= '0' && r <= '9' {
		return nil, errors.New("unexpected integer")
	} else {
		token.typ = tokDigit
		return input[1:], nil
	}
}

func isIdentifierStart(r rune) bool {
	if r == '_' {
		return true
	} else if r >= 'A' && r <= 'Z' {
		return true
	} else if r >= 'a' && r <= 'z' {
		return true
	} else if r >= '\200' && r <= '\377' {
		return true
	}
	return false
}

func isIdentifierContinuation(r rune) bool {
	if isIdentifierStart(r) {
		return true
	} else if r >= '0' && r <= '9' {
		return true
	} else if r == '$' {
		return true
	}
	return false
}

func readQuotedIdentifier(input []rune, token *queryParserToken) (rest []rune, err error) {
	var identifier string
	for {
		if len(input) < 1 {
			return nil, errQueryParserUnexpectedEOF
		} else if input[0] == '"' {
			input = input[1:]
			break
		} else if input[0] == '\\' {
			if len(input) < 2 {
				return nil, errQueryParserUnexpectedEOF
			}
			if input[1] != '\\' && input[1] != '"' {
				return nil, fmt.Errorf("unexpected escape character '%c'", input[1])
			}
			identifier += string(input[1])
			input = input[2:]
		} else {
			identifier += string(input[0])
			input = input[1:]
		}
	}
	token.payload = identifier
	token.typ = tokIdentifier
	return input, nil
}

func readIdentifier(input []rune, token *queryParserToken) (rest []rune, err error) {
	i := input
	input = input[1:]
	identifierLen := 1
	for len(input) > 0 && isIdentifierContinuation(input[0]) {
		identifierLen++
		input = input[1:]
	}
	token.payload = strings.ToLower(string(i[:identifierLen]))
	token.typ = tokIdentifier
	return input, nil
}

func stripLeadingWhitespace(input []rune) []rune {
	for len(input) > 0 && strings.IndexRune(whiteSpaceCharacters, input[0]) != -1 {
		input = input[1:]
	}
	return input
}
