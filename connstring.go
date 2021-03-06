// pg_back
//
// Copyright 2011-2021 Nicolas Thauvin and contributors. All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
//  1. Redistributions of source code must retain the above copyright
//     notice, this list of conditions and the following disclaimer.
//  2. Redistributions in binary form must reproduce the above copyright
//     notice, this list of conditions and the following disclaimer in the
//     documentation and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE AUTHORS ``AS IS'' AND ANY EXPRESS OR
// IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
// OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
// IN NO EVENT SHALL THE AUTHORS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
// INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
// ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
// THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package main

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"unicode"
)

// parseConnInfo parses and converts a key=value connection string of
// PostgreSQL into a map
func parseConnInfo(connstring string) (map[string]string, error) {

	// Structure to hold the state of the parsing
	s := struct {
		expKey   bool // expect the next token is a keyword
		expSep   bool // expect the next token is the = keyword/value separator
		expVal   bool // expect the next token is a value
		inKey    bool // we are inside a keyword
		inVal    bool // we are inside a value
		inEscape bool // we have found a backslash next rune is escaped
		isQuoted bool // we are in a quoted value the end of token rune is different
	}{expKey: true} // we start by expecting a keyword

	// We store our key/value pais in a map. When a keyword is given
	// multiple times, only the value closest to the right is
	// kept. PostgreSQL behaves the same.
	pairs := make(map[string]string)
	keyword := ""
	value := ""

	for _, r := range []rune(connstring) {
		if s.expKey {
			if unicode.IsSpace(r) {
				continue
			}

			if r >= 'a' && r <= 'z' {
				keyword += string(r)
				s.expKey = false
				s.inKey = true
				continue
			}

			// Here are more strict than PostgreSQL by allowing
			// keyword to start only with lowercase ascii letters
			return pairs, fmt.Errorf("illegal keyword character")
		}

		if s.expSep {
			if unicode.IsSpace(r) {
				continue
			}

			if r == '=' {
				s.expSep = false
				s.expVal = true
				continue
			}

			return pairs, fmt.Errorf("missing \"=\" after \"%s\"", keyword)
		}

		if s.expVal {
			if unicode.IsSpace(r) {
				continue
			}

			s.expVal = false
			s.inVal = true

			if r == '\'' {
				s.isQuoted = true
				continue
			}
		}

		if s.inKey {
			if (r >= 'a' && r <= 'z') || r == '_' {
				keyword += string(r)
				continue
			}

			if unicode.IsSpace(r) {
				s.inKey = false
				s.expSep = true
				continue
			}

			if r == '=' {
				s.inKey = false
				s.expVal = true
				continue
			}

			return pairs, fmt.Errorf("illegal character in keyword")
		}

		if s.inVal {
			if r == '\\' && !s.inEscape {
				s.inEscape = true
				continue
			}

			if s.inEscape {
				s.inEscape = false
				value += string(r)
				continue
			}

			if s.isQuoted && r == '\'' {
				s.isQuoted = false
				s.inVal = false
				s.expKey = true
				pairs[keyword] = value
				keyword = ""
				value = ""
				continue
			}

			if !s.isQuoted && unicode.IsSpace(r) {
				s.inVal = false
				s.expKey = true
				pairs[keyword] = value
				keyword = ""
				value = ""
				continue
			}

			value += string(r)
		}

	}

	if s.expSep || s.inKey {
		return pairs, fmt.Errorf("missing value")
	}

	if s.inVal && s.isQuoted {
		return pairs, fmt.Errorf("unterminated quoted string")
	}

	if s.expVal || s.inVal {
		pairs[keyword] = value
	}

	return pairs, nil
}

func makeConnInfo(infos map[string]string) string {
	conninfo := ""

	// Map keys are randomized, sort them so that the output is always the
	// same for a given input, useful for unit tests.
	keywords := make([]string, 0, len(infos))
	for k := range infos {
		keywords = append(keywords, k)
	}
	sort.Strings(keywords)

	for i, k := range keywords {
		// single quotes and backslashes must be escaped
		value := strings.ReplaceAll(infos[k], "\\", "\\\\")
		value = strings.ReplaceAll(value, "'", "\\'")

		// empty values or values containing space, the equal sign or single quotes
		// must be single quoted
		if len(infos[k]) == 0 || strings.ContainsAny(infos[k], "\t\n\v\f\r ='") {
			value = "'" + value + "'"
		}

		if i < (len(infos) - 1) {
			conninfo += fmt.Sprintf("%v=%v ", k, value)
		} else {
			conninfo += fmt.Sprintf("%v=%v", k, value)
		}
	}
	return conninfo
}

func prepareConnInfo(host string, port int, username string, dbname string) string {
	var conninfo string

	// dbname may be a connstring. The database name option, usually -d for
	// PostgreSQL binaires accept a connection string. We do a simple check
	// for a = sign. If someone has a database name containing a space, one
	// can still dump it by giving us connstring.
	if strings.Contains(dbname, "=") {
		conninfo = fmt.Sprintf("%v ", dbname)
	} else {

		if host != "" {
			conninfo += fmt.Sprintf("host=%v ", host)
		} else {
			// driver lib/pq defaults to localhost for the host, so
			// we have to check PGHOST and fallback to the unix
			// socket directory to avoid overriding PGHOST
			if _, ok := os.LookupEnv("PGHOST"); !ok {
				conninfo += "host=/tmp "
			}
		}
		if port != 0 {
			conninfo += fmt.Sprintf("port=%v ", port)
		}
		if username != "" {
			conninfo += fmt.Sprintf("user=%v ", username)
		}
		if dbname != "" {
			conninfo += fmt.Sprintf("dbname=%v ", dbname)
		}
	}

	if !strings.Contains(conninfo, "application_name") {
		l.Verboseln("using pg_back as application_name")
		conninfo += "application_name=pg_back"
	}

	return conninfo
}
