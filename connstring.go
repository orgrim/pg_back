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
	"net/url"
	"sort"
	"strings"
	"unicode"
)

const (
	CI_KEYVAL int = iota
	CI_URI
)

type ConnInfo struct {
	Kind  int // See CI_* constants
	Infos map[string]string
}

func parseConnInfo(connstring string) (*ConnInfo, error) {
	c := ConnInfo{}

	if strings.HasPrefix(connstring, "postgresql://") {
		c.Kind = CI_URI
		i, err := parseUrlConnInfo(connstring)
		if err != nil {
			return nil, err
		}
		c.Infos = i
		return &c, nil
	}

	if strings.Contains(connstring, "=") {
		c.Kind = CI_KEYVAL
		i, err := parseKeywordConnInfo(connstring)
		if err != nil {
			return nil, err
		}
		c.Infos = i
		return &c, nil
	}

	return nil, fmt.Errorf("parseConnInfo: invalid input connection string")
}

func (c *ConnInfo) String() string {
	switch c.Kind {
	case CI_KEYVAL:
		return makeKeywordConnInfo(c.Infos)
	case CI_URI:
		return makeUrlConnInfo(c.Infos)
	}

	return ""
}

func (c *ConnInfo) Copy() *ConnInfo {
	newC := ConnInfo{
		Kind:  c.Kind,
		Infos: make(map[string]string, len(c.Infos)),
	}

	for k, v := range c.Infos {
		newC.Infos[k] = v
	}

	return &newC
}

// Set returns a pointer to a full copy of the conninfo with the key added or
// the value updated
func (c *ConnInfo) Set(keyword, value string) *ConnInfo {
	newC := c.Copy()
	newC.Infos[keyword] = value

	return newC
}

// Del returns a pointer to a full copy of the conninfo with the key removed
func (c *ConnInfo) Del(keyword string) *ConnInfo {
	newC := c.Copy()
	delete(newC.Infos, keyword)

	return newC
}

// MakeEnv return the conninfo as a list of "key=value" environment variables
// that the libpq understands, as stated in the documentation of PostgreSQL 14
func (c *ConnInfo) MakeEnv() []string {
	env := make([]string, 0, len(c.Infos))
	for k, v := range c.Infos {
		switch k {
		case "host":
			env = append(env, "PGHOST="+v)
		case "hostaddr":
			env = append(env, "PGHOSTADDR="+v)
		case "port":
			env = append(env, "PGPORT="+v)
		case "dbname":
			env = append(env, "PGDATABASE="+v)
		case "user":
			env = append(env, "PGUSER="+v)
		case "password":
			env = append(env, "PGPASSWORD="+v)
		case "passfile":
			env = append(env, "PGPASSFILE="+v)
		case "service":
			env = append(env, "PGSERVICE="+v)
		case "options":
			env = append(env, "PGOPTIONS="+v)
		case "application_name":
			env = append(env, "PGAPPNAME="+v)
		case "sslmode":
			env = append(env, "PGSSLMODE="+v)
		case "requiressl":
			env = append(env, "PGREQUIRESSL="+v)
		case "sslcert":
			env = append(env, "PGSSLCERT="+v)
		case "sslkey":
			env = append(env, "PGSSLKEY="+v)
		case "sslrootcert":
			env = append(env, "PGSSLROOTCERT="+v)
		case "sslcrl":
			env = append(env, "PGSSLCRL="+v)
		case "krbsrvname":
			env = append(env, "PGKRBSRVNAME="+v)
		case "gsslib":
			env = append(env, "PGGSSLIB="+v)
		case "connect_timeout":
			env = append(env, "PGCONNECT_TIMEOUT="+v)
		case "channel_binding":
			env = append(env, "PGCHANNELBINDING="+v)
		case "sslcompression":
			env = append(env, "PGSSLCOMPRESSION="+v)
		case "sslcrldir":
			env = append(env, "PGSSLCRLDIR="+v)
		case "sslsni":
			env = append(env, "PGSSLSNI="+v)
		case "requirepeer":
			env = append(env, "PGREQUIREPEER="+v)
		case "ssl_min_protocol_version":
			env = append(env, "PGSSLMINPROTOCOLVERSION="+v)
		case "ssl_max_protocol_version":
			env = append(env, "PGSSLMAXPROTOCOLVERSION="+v)
		case "gssencmode":
			env = append(env, "PGGSSENCMODE="+v)
		case "client_encoding":
			env = append(env, "PGCLIENTENCODING="+v)
		case "target_session_attrs":
			env = append(env, "PGTARGETSESSIONATTRS="+v)
		}
	}

	return env
}

func parseUrlConnInfo(connstring string) (map[string]string, error) {
	u, err := url.Parse(connstring)
	if err != nil {
		return nil, fmt.Errorf("parsing of URI conninfo failed: %w", err)
	}

	connInfo := make(map[string]string, 0)
	if u.Host != "" {
		fullHosts := strings.Split(u.Host, ",")
		if len(fullHosts) == 1 {
			v := u.Hostname()
			if v != "" {
				connInfo["host"] = v
			}
			v = u.Port()
			if v != "" {
				connInfo["port"] = v
			}
		} else {
			// We need to split and group hosts and ports
			// ourselves, net/url does not handle multiple hosts
			// correctly
			hosts := make([]string, 0)
			ports := make([]string, 0)
			for _, fullHost := range fullHosts {
				hostPort := make([]string, 0)
				if strings.HasPrefix(fullHost, "[") {
					// Handle literal IPv6 addresses
					hostPort = strings.Split(strings.TrimPrefix(fullHost, "["), "]:")
				} else {
					hostPort = strings.Split(fullHost, ":")
				}
				if len(hostPort) == 1 {
					hosts = append(hosts, strings.Trim(hostPort[0], "[]"))
				} else {
					hosts = append(hosts, strings.Trim(hostPort[0], "[]"))
					ports = append(ports, hostPort[1])
				}
			}
			connInfo["host"] = strings.Join(hosts, ",")
			connInfo["port"] = strings.Join(ports, ",")
		}
	}

	user := u.User.Username()
	if user != "" {
		connInfo["user"] = user
	}

	password, set := u.User.Password()
	if password != "" && set {
		connInfo["password"] = password
	}

	dbname := strings.TrimPrefix(u.Path, "/")
	if dbname != "" {
		connInfo["dbname"] = dbname
	}

	for k, vs := range u.Query() {
		if k == "" {
			continue
		}
		connInfo[k] = strings.Join(vs, ",")
	}

	return connInfo, nil
}

// parseKeywordConnInfo parses and converts a key=value connection string of
// PostgreSQL into a map
func parseKeywordConnInfo(connstring string) (map[string]string, error) {

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

func makeKeywordConnInfo(infos map[string]string) string {
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

func makeUrlConnInfo(infos map[string]string) string {
	u := &url.URL{
		Scheme: "postgresql",
	}

	// create user info
	username, hasUser := infos["user"]
	pass, hasPass := infos["password"]

	var user *url.Userinfo
	if hasPass {
		user = url.UserPassword(username, pass)
	} else if hasUser {
		user = url.User(username)
	}
	u.User = user

	// Manage host:port list with commas. When the hosts is a unix socket
	// directory, do not set the Host field of the url because it won't be
	// percent encoded, use the query part instead
	if !strings.Contains(infos["host"], "/") {
		hosts := strings.Split(infos["host"], ",")
		ports := strings.Split(infos["port"], ",")

		// Ensure we have lists of the same size to build host:port in a loop
		if len(hosts) > len(ports) {
			if len(ports) == 1 {
				// same non default port for all hosts, duplicate it
				// for the next loop
				if ports[0] != "" {
					for i := 0; i < len(hosts); i++ {
						ports = append(ports, ports[0])
					}
				}
			} else {
				// fill with empty port to fix the list
				for i := 0; i < len(hosts); i++ {
					ports = append(ports, "")
				}
			}
		}

		hostnames := make([]string, 0, len(hosts))

		for i, host := range hosts {
			// Take care of IPv6 addresses
			if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
				host = "[" + host + "]"
			}

			if ports[i] != "" {
				hostnames = append(hostnames, host+":"+ports[i])
			} else {
				hostnames = append(hostnames, host)
			}
		}

		u.Host = strings.Join(hostnames, ",")
	}

	// dbname
	u.Path = "/" + infos["dbname"]
	u.RawPath = "/" + url.PathEscape(infos["dbname"])

	// compute query
	query := url.Values{}
	needPort := false

	// Sort keys so that host comes before port and we can add port to the
	// query when we are forced to add host to the query (unix socket
	// directory) in the next loop
	keys := make([]string, 0, len(infos))
	for k := range infos {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		if k == "host" && strings.Contains(infos[k], "/") || k == "port" && needPort {
			needPort = true
			query.Set(k, infos[k])
			continue
		}

		if k != "host" && k != "port" && k != "user" && k != "password" && k != "dbname" {
			query.Set(k, infos[k])
		}
	}
	u.RawQuery = query.Encode()

	return u.String()
}

// prepareConnInfo returns a connexion string computed from the input
// values. When the dbname is already a connection string or a postgresql://
// URI, it only add the application_name keyword if not set.
func prepareConnInfo(host string, port int, username string, dbname string) (*ConnInfo, error) {
	var (
		conninfo *ConnInfo
		err      error
	)

	// dbname may be a connstring or a URI. The database name option,
	// usually -d for PostgreSQL binaires accept a connection string and
	// URIs. We do a simple check for a = sign or the postgresql scheme. If
	// someone has a database name containing a space, one can still dump
	// it by giving us connstring.
	if strings.HasPrefix(dbname, "postgresql://") || strings.Contains(dbname, "=") {
		conninfo, err = parseConnInfo(dbname)
		if err != nil {
			return nil, err
		}

	} else {
		conninfo = &ConnInfo{
			Infos: make(map[string]string),
		}

		if host != "" {
			conninfo.Infos["host"] = host
		}

		if port != 0 {
			conninfo.Infos["port"] = fmt.Sprintf("%v", port)
		}

		if username != "" {
			conninfo.Infos["user"] = username
		}

		if dbname != "" {
			conninfo.Infos["dbname"] = dbname
		}
	}

	if _, ok := conninfo.Infos["application_name"]; !ok {
		l.Verboseln("using pg_back as application_name")
		conninfo.Infos["application_name"] = "pg_back"
	}

	return conninfo, nil
}
