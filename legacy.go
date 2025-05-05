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
	"io"
	"os"
	"strings"

	"github.com/anmitsu/go-shlex"
)

// Read the input file and return all lines that look like legacy configuration
// options
func readLegacyConf(f io.Reader) ([]string, error) {
	var lines []string

	data, err := io.ReadAll(f)
	if err != nil {
		return lines, fmt.Errorf("could not read file: %w", err)
	}

	lines = make([]string, 0)
	buf := make([]byte, 0)
	for _, b := range data {
		if b == '\n' {
			if len(buf) > 0 {
				line := strings.Trim(string(buf), " \t\r\v")
				if strings.HasPrefix(line, "PGBK_") || strings.HasPrefix(line, "SIGNATURE_ALGO=") {
					lines = append(lines, line)
				}
			}
			buf = make([]byte, 0)
			continue
		}
		buf = append(buf, b)
	}

	return lines, nil
}

// Remove the end comment of a line while taking single and double quoted
// strings into account
func stripEndComment(in string) string {
	buf := make([]byte, 0, len(in))

	s := struct {
		inSQuote bool
		inDQuote bool
		inEscape bool
	}{}

out:
	for _, b := range []byte(in) {
		switch b {
		case '"':
			if !s.inSQuote {
				if s.inDQuote {
					if s.inEscape {
						s.inEscape = false
					} else {
						s.inDQuote = false
					}
				} else {
					s.inDQuote = true
				}
			} else {
				if s.inEscape {
					s.inEscape = false
				}
			}

		case '\'':
			if !s.inDQuote {
				if s.inSQuote {
					if s.inEscape {
						s.inEscape = false
					} else {
						s.inSQuote = false
					}
				} else {
					s.inSQuote = true
				}
			} else {
				if s.inEscape {
					s.inEscape = false
				}
			}

		case '\\':
			if s.inEscape {
				s.inEscape = false
			} else {
				s.inEscape = true
			}

		case '#':
			if !s.inDQuote && !s.inSQuote {
				break out
			}
		}

		buf = append(buf, b)
	}

	return strings.Trim(string(buf), " \t\v")
}

func convertLegacyConf(oldConf []string) string {
	var result string

	table := map[string]string{
		"PGBK_BIN":                   "bin_directory",
		"PGBK_BACKUP_DIR":            "backup_directory",
		"PGBK_PURGE":                 "purge_older_than",
		"PGBK_PURGE_MIN_KEEP":        "purge_min_keep",
		"PGBK_DBLIST":                "include_dbs",
		"PGBK_EXCLUDE":               "exclude_dbs",
		"PGBK_STANDBY_PAUSE_TIMEOUT": "pause_timeout",
		"PGBK_HOSTNAME":              "host",
		"PGBK_PORT":                  "port",
		"PGBK_USERNAME":              "user",
		"PGBK_CONNDB":                "dbname",
		"PGBK_PRE_BACKUP_COMMAND":    "pre_backup_hook",
		"PGBK_POST_BACKUP_COMMAND":   "post_backup_hook",
		"SIGNATURE_ALGO":             "checksum_algorithm",
	}

	for _, line := range oldConf {

		tokens := strings.SplitN(line, "=", 2)
		value := stripEndComment(tokens[1])

		switch tokens[0] {
		case "PGBK_TIMESTAMP":
			// Detect to legacy format, otherwise discard the value
			// for the new rfc3339
			if strings.Trim(value, "'\"") == "%Y-%m-%d_%H-%M-%S" {
				result += fmt.Sprintln("timestamp_format", "=", "legacy")
			} else {
				result += fmt.Sprintln("timestamp_format", "=", "rfc3339")
			}

		case "PGBK_OPTS":
			// Parse the elements with shlex to keeps spaces when
			// it is a shell array. When the value is not a shell
			// array we need to trim quotes to ensure shlex splits
			// the options.
			v := value
			if strings.HasPrefix(value, "(") {
				v = strings.Trim(value, "()")
			} else {
				v = strings.Trim(value, "'\"")
			}
			words, err := shlex.Split(v, true)
			if err != nil {
				l.Warnf("could not parse value of PGBK_OPTS \"%s\": %s", value, err)
				continue
			}

			// Extract the format into a distinct option, otherwise
			// a format option from pg_dump_options could interfere
			// with the computed pg_dump command which use the last
			// format option it finds
			qWords := make([]string, 0, len(words))
			expectFormat := false
			for _, w := range words {

				switch w {
				case "-Fp", "-Fplain", "--format=p", "--format=plain":
					result += fmt.Sprintln("format", "=", "plain")
					continue
				case "-Fc", "-Fcustom", "--format=c", "--format=custom":
					result += fmt.Sprintln("format", "=", "custom")
					continue
				case "-Ft", "-Ftar", "--format=t", "--format=tar":
					result += fmt.Sprintln("format", "=", "tar")
					continue
				case "-Fd", "-Fdirectory", "--format=d", "--format=directory":
					result += fmt.Sprintln("format", "=", "directory")
					continue
				case "-F", "--format":
					expectFormat = true
					continue
				}

				if expectFormat {
					expectFormat = false
					switch []byte(w)[0] {
					case 'p':
						result += fmt.Sprintln("format", "=", "plain")
					case 'c':
						result += fmt.Sprintln("format", "=", "custom")
					case 't':
						result += fmt.Sprintln("format", "=", "tar")
					case 'd':
						result += fmt.Sprintln("format", "=", "directory")
					}
					continue
				}

				// Quote tokens back so that we do not lose
				// spaces used in shell array elements
				if strings.Contains(w, " ") {
					quote := strings.ReplaceAll(w, "\\", "\\\\")
					quote = strings.ReplaceAll(quote, "\"", "\\\"")
					qWords = append(qWords, fmt.Sprintf("\"%s\"", quote))
				} else {
					qWords = append(qWords, w)
				}
			}

			result += fmt.Sprintln("pg_dump_options", "=", strings.Join(qWords, " "))

		case "PGBK_DBLIST", "PGBK_EXCLUDE":
			// The separator for lists of databases now the comma
			dbs := make([]string, 0)
			for d := range strings.SplitSeq(strings.Trim(value, "'\""), " ") {
				if len(d) > 0 {
					dbs = append(dbs, d)
				}
			}
			result += fmt.Sprintln(table[tokens[0]], "=", strings.Join(dbs, ", "))

		case "PGBK_WITH_TEMPLATES":
			// with_templates is now a boolean, the shell script
			// used the "yes" value to include templates databases
			if strings.Trim(value, "'\"") == "yes" {
				result += fmt.Sprintln("with_templates", "=", "true")
			} else {
				result += fmt.Sprintln("with_templates", "=", "false")
			}

		default:
			result += fmt.Sprintln(table[tokens[0]], "=", value)
		}
	}

	return result
}

func convertLegacyConfFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("could not convert configuration: %w", err)
	}
	defer f.Close()

	contents, err := readLegacyConf(f)
	if err != nil {
		return fmt.Errorf("could not convert configuration: %w", err)
	}

	fmt.Printf("%s", convertLegacyConf(contents))

	return nil
}
