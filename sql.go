// pg_goback
//
// Copyright 2020 Nicolas Thauvin. All rights reserved.
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
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"github.com/lib/pq"
	"os"
	"strings"
	"time"
)

type pg struct {
	conn      *sql.DB
	version   int
	xlogOrWal string
}

func pgGetVersionNum(db *sql.DB) (int, error) {
	var version int

	err := db.QueryRow("select setting from pg_settings where name = 'server_version_num'").Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("could not get PostgreSQL server version: %s", err)
	}

	return version, nil
}

func dbOpen(conninfo string) (*pg, error) {

	db, err := sql.Open("postgres", conninfo)
	if err != nil {
		return nil, fmt.Errorf("could not open database: %s", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("could not connect to database: %s", err)
	}

	newDB := new(pg)
	newDB.conn = db
	newDB.version, err = pgGetVersionNum(db)
	if err != nil {
		db.Close()
		return nil, err
	}

	// Keyword xlog has been replaced by wal as of PostgreSQL 10
	if newDB.version >= 100000 {
		newDB.xlogOrWal = "wal"
	} else {
		newDB.xlogOrWal = "xlog"
	}

	return newDB, nil
}

func (db *pg) Close() error {
	return db.conn.Close()
}

func prepareConnInfo(host string, port int, username string, dbname string) string {
	var conninfo string

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
	conninfo += "application_name=pg_goback"

	return conninfo
}

func sqlQuoteLiteral(s string) string {
	var o string
	// Make standard_conforming_strings happy if the input
	// contains some backslash
	if strings.ContainsAny(s, "\\") {
		o = "E"
	}
	o += "'"

	// double single quotes and backslahses
	o += strings.ReplaceAll(s, "'", "''")
	o = strings.ReplaceAll(o, "\\", "\\\\")

	o += "'"

	return o
}

func listAllDatabases(db *pg, withTemplates bool) ([]string, error) {
	var (
		query  string
		dbname string
	)

	if withTemplates {
		query = "select datname from pg_database where datallowconn;"
	} else {
		query = "select datname from pg_database where datallowconn and not datistemplate;"
	}

	dbs := make([]string, 0)
	rows, err := db.conn.Query(query)
	if err != nil {
		return dbs, fmt.Errorf("could not list databases: %s", err)
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&dbname)
		if err != nil {
			continue
		}
		dbs = append(dbs, dbname)
	}
	if err := rows.Err(); err != nil {
		return dbs, fmt.Errorf("could not retrieve rows: %s", err)
	}
	return dbs, nil
}

type pgVersionError struct {
	s string
}

func (e *pgVersionError) Error() string {
	return e.s
}

// pg_dumpacl stuff
func dumpCreateDBAndACL(db *pg, dbname string) (string, error) {
	var s string

	if dbname == "" {
		return "", fmt.Errorf("empty input dbname")
	}

	// this query only work from 9.0, where datcollate and datctype were added to pg_database
	if db.version < 90000 {
		return "", &pgVersionError{s: "cluster version is older than 9.0, not dumping ACL"}
	}

	// this is no longer necessary after 11
	if db.version >= 110000 {
		return "", nil
	}

	rows, err := db.conn.Query(
		"SELECT coalesce(rolname, (select rolname from pg_authid where oid=(select datdba from pg_database where datname='template0'))), "+
			"  pg_encoding_to_char(d.encoding), "+
			"  datcollate, datctype, "+
			"  datistemplate, datacl, datconnlimit, "+
			"  (SELECT spcname FROM pg_tablespace t WHERE t.oid = d.dattablespace) AS dattablespace "+
			"FROM pg_database d"+
			"  LEFT JOIN pg_authid u ON (datdba = u.oid) "+
			"WHERE datallowconn AND datname = $1",
		dbname)
	if err != nil {
		return "", fmt.Errorf("could not query database information for %s: %s", dbname, err)
	}
	defer rows.Close()

	out := new(bytes.Buffer)
	for rows.Next() {
		var (
			owner      string
			encoding   string
			collate    string
			ctype      string
			istemplate bool
			acl        []sql.NullString
			connlimit  int
			tablespace string
		)
		err := rows.Scan(&owner, &encoding, &collate, &ctype, &istemplate, pq.Array(&acl), &connlimit, &tablespace)
		if err != nil {
			return "", fmt.Errorf("could not get row: %s", err)
		}

		if dbname != "template1" && dbname != "postgres" {
			s += fmt.Sprintf("--\n-- Database creation\n--\n\n")
			s += fmt.Sprintf("CREATE DATABASE \"%s\" WITH TEMPLATE = template0 OWNER = \"%s\"", dbname, owner)
			s += fmt.Sprintf(" ENCODING = %s", sqlQuoteLiteral(encoding))
			s += fmt.Sprintf(" LC_COLLATE = %s", sqlQuoteLiteral(collate))

			s += fmt.Sprintf(" LC_CTYPE = %s", sqlQuoteLiteral(ctype))

			if tablespace != "pg_default" {
				s += fmt.Sprintf(" TABLESPACE = \"%s\"", tablespace)
			}
			if connlimit != -1 {
				s += fmt.Sprintf(" CONNECTION LIMIT = %d", connlimit)
			}
			s += fmt.Sprintf(";\n\n")

			if istemplate {
				s += fmt.Sprintf("UPDATE pg_catalog.pg_database SET datistemplate = 't' WHERE datname = %s;\n", sqlQuoteLiteral(dbname))
			}
		}
		for _, e := range acl {
			// skip NULL values
			if !e.Valid {
				continue
			}

			s += makeACLCommands(e.String, dbname, owner)
		}
		// do not append this newline if we could have an empty
		// file with only this newline
		if len(out.String()) > 0 {
			s += fmt.Sprintf("\n")
		}
	}
	err = rows.Err()
	if err != nil {
		return s, fmt.Errorf("could not retrive rows: %s", err)
	}

	return s, nil
}

func makeACLCommands(aclitem string, dbname string, owner string) string {
	var s string
	// the aclitem format is "grantee=privs/grantor" where privs
	// is a list of letters, one for each privilege followed by *
	// when grantee as WITH GRANT OPTION for it
	t := strings.Split(aclitem, "=")
	grantee := t[0]
	t = strings.Split(t[1], "/")
	privs := t[0]
	grantor := t[1]

	// public role: when the privs differ from the default, issue grants
	if grantee == "" {
		grantee = "PUBLIC"
		if privs != "Tc" {
			s += fmt.Sprintf("REVOKE ALL ON DATABASE \"%s\" FROM PUBLIC;\n", dbname)
		} else {
			return s
		}
	}
	// owner: when other roles have been given privileges, all
	// privileges are shown for the owner
	if grantee == owner {
		if privs != "CTc" {
			s += fmt.Sprintf("REVOKE ALL ON DATABASE \"%s\" FROM \"%s\";\n", dbname, grantee)
		} else {
			return s
		}
	}

	if grantor != owner {
		s += fmt.Sprintf("SET SESSION AUTHORIZATION \"%s\";\n", grantor)
	}
	for i, b := range privs {
		switch b {
		case 'C':
			s += fmt.Sprintf("GRANT CREATE ON DATABASE \"%s\" TO \"%s\"", dbname, grantee)
		case 'T':
			s += fmt.Sprintf("GRANT TEMPORARY ON DATABASE \"%s\" TO \"%s\"", dbname, grantee)
		case 'c':
			s += fmt.Sprintf("GRANT CONNECT ON DATABASE \"%s\" TO \"%s\"", dbname, grantee)
		}

		if i+1 < len(privs) {
			if privs[i+1] == '*' {
				s += fmt.Sprintf(" WITH GRANT OPTION;\n")
			} else {
				s += fmt.Sprintf(";\n")
			}
		} else {
			s += fmt.Sprintf(";\n")
		}
	}
	if grantor != owner {
		s += fmt.Sprintf("RESET SESSION AUTHORIZATION;\n")
	}
	return s
}

func dumpDBConfig(db *pg, dbname string) (string, error) {
	var s string
	// dump per database config
	rows, err := db.conn.Query("SELECT unnest(setconfig) FROM pg_db_role_setting WHERE setrole = 0 AND setdatabase = (SELECT oid FROM pg_database WHERE datname = $1)", dbname)
	if err != nil {
		return "", fmt.Errorf("could not query database configuration for %s: %s", dbname, err)
	}
	defer rows.Close()

	for rows.Next() {
		var keyVal string

		err := rows.Scan(&keyVal)
		if err != nil {
			return "", fmt.Errorf("could not get row: %s", err)
		}

		// split
		tokens := strings.Split(keyVal, "=")

		// do not quote the value for those two parameters
		if tokens[0] != "DateStyle" && tokens[0] != "search_path" {
			tokens[1] = fmt.Sprintf("'%s'", tokens[1])
		}
		s += fmt.Sprintf("ALTER DATABASE \"%s\" SET \"%s\" TO %s;\n", dbname, tokens[0], tokens[1])
	}
	err = rows.Err()
	if err != nil {
		return "", fmt.Errorf("could not retrive rows: %s", err)
	}

	return s, nil
}

func showSettings(db *pg) (string, error) {
	var s string

	if db.version < 90500 {
		return "", &pgVersionError{s: "cluster version is older than 9.5, not dumping configuration"}
	}

	// get the non default values set in the files and applied,
	// this avoid duplicates when multiple files define
	// parameters.
	rows, err := db.conn.Query("SELECT name, setting FROM pg_show_all_file_settings() WHERE applied ORDER BY name")
	if err != nil {
		return "", fmt.Errorf("could not query instance configuration: %s", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			name  string
			value string
		)

		err := rows.Scan(&name, &value)
		if err != nil {
			l.Errorln(err)
			continue
		}

		if name != "DateStyle" && name != "search_path" {
			value = fmt.Sprintf("'%s'", value)
		}

		s += fmt.Sprintf("%s = %s\n", name, value)
	}

	err = rows.Err()
	if err != nil {
		return "", fmt.Errorf("could not retrive rows: %s", err)
	}

	return s, nil
}

type pgReplicaHasLocks struct{}

func (*pgReplicaHasLocks) Error() string {
	return "replication not paused because of AccessExclusiveLock"
}

func pauseReplication(db *pg) error {
	// If an AccessExclusiveLock is granted when the replay is
	// paused, it will remain and pg_dump would be stuck forever
	rows, err := db.conn.Query(fmt.Sprintf("SELECT pg_%s_replay_pause() "+
		"WHERE NOT EXISTS (SELECT 1 FROM pg_locks WHERE mode = 'AccessExclusiveLock') "+
		"AND pg_is_in_recovery();", db.xlogOrWal))
	if err != nil {
		return fmt.Errorf("could not pause replication: %s", err)
	}
	defer rows.Close()

	// The query returns a single row with one column of type void,
	// which is and empty string, on success. It does not return
	// any row on failure
	void := "failed"
	for rows.Next() {
		err := rows.Scan(&void)
		if err != nil {
			return fmt.Errorf("could not get row: %s", err)
		}
	}
	if void == "failed" {
		return &pgReplicaHasLocks{}
	}
	return nil
}

func canPauseReplication(db *pg) (bool, error) {
	// hot standby exists from 9.0
	if db.version < 90000 {
		return false, nil
	}

	rows, err := db.conn.Query(fmt.Sprintf("SELECT 1 FROM pg_proc "+
		"WHERE proname='pg_%s_replay_pause' AND pg_is_in_recovery()", db.xlogOrWal))
	if err != nil {
		return false, fmt.Errorf("could not check if replication is pausable: %s", err)
	}
	defer rows.Close()

	// The query returns 1 on success, no row on failure
	var one int
	for rows.Next() {
		err := rows.Scan(&one)
		if err != nil {
			return false, fmt.Errorf("could not get row: %s", err)
		}
	}
	if one == 0 {
		return false, nil
	}

	return true, nil
}

func pauseReplicationWithTimeout(db *pg, timeOut int) error {

	if ok, err := canPauseReplication(db); !ok {
		return err
	}

	ticker := time.NewTicker(time.Duration(10) * time.Second)
	done := make(chan bool)
	stop := make(chan bool)
	fail := make(chan error)

	l.Infoln("pausing replication")

	// We want to retry pausing replication at a defined interval
	// but not forever. We cannot put the timeout in the same
	// select as the ticker since the ticker would always win
	go func() {
		var rerr *pgReplicaHasLocks
		defer ticker.Stop()

		for {
			if err := pauseReplication(db); err != nil {
				if errors.As(err, &rerr) {
					l.Warnln(err)
				} else {
					fail <- err
					return
				}
			} else {
				done <- true
				return
			}

			select {
			case <-stop:
				return
			case <-ticker.C:
				break
			}
		}
	}()

	// Return as soon as the replication is paused or stop the
	// goroutine if we hit the timeout
	select {
	case <-done:
		l.Infoln("replication paused")
	case <-time.After(time.Duration(timeOut) * time.Second):
		stop <- true
		return fmt.Errorf("replication not paused after %v", time.Duration(timeOut)*time.Second)
	case err := <-fail:
		return fmt.Errorf("%s", err)
	}

	return nil
}

func resumeReplication(db *pg) error {
	if ok, err := canPauseReplication(db); !ok {
		return err
	}

	l.Infoln("resuming replication")
	_, err := db.conn.Exec(fmt.Sprintf("SELECT pg_%s_replay_resume() WHERE pg_is_in_recovery();", db.xlogOrWal))
	if err != nil {
		return fmt.Errorf("could not resume replication: %s", err)
	}

	return nil
}
