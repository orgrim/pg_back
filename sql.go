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
	"database/sql"
	"fmt"
	"github.com/lib/pq"
	"io"
	"os"
	"strings"
	"time"
)

type DB struct {
	conn      *sql.DB
	version   int
	xlogOrWal string
}

func (db DB) String() string {
	return fmt.Sprintf("conn=%v, version=%v, walkeywork=%v", db.conn, db.version, db.xlogOrWal)
}

func DbGetVersionNum(db *sql.DB) (int, bool) {
	var version int

	err := db.QueryRow("select setting from pg_settings where name = 'server_version_num'").Scan(&version)
	if err != nil {
		l.Errorln(err)
		return 0, false
	}

	return version, true
}

func ListAllDatabases(db *DB, withTemplates bool) ([]string, bool) {
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
		l.Errorln(err)
		return dbs, false
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&dbname)
		if err != nil {
			l.Errorln(err)
			continue
		}
		dbs = append(dbs, dbname)
	}
	if err = rows.Err(); err != nil {
		l.Errorln(err)
		return dbs, false
	}
	return dbs, true
}

func PrepareConnInfo(host string, port int, username string, dbname string) string {
	var conninfo string

	if host != "" {
		conninfo += fmt.Sprintf("host=%v ", host)
	} else {
		// driver lib/pq defaults to localhost for the host, so
		// we have to check PGHOST and fallback to the unix
		// socket directory to avoid overriding PGHOST
		if _, ok := os.LookupEnv("PGHOST"); !ok {
			conninfo += "host=/var/run/postgresql "
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

func DbOpen(conninfo string) (*DB, bool) {
	var ok bool

	db, err := sql.Open("postgres", conninfo)
	if err != nil {
		l.Errorln(err)
		return nil, false
	}
	err = db.Ping()
	if err != nil {
		l.Errorln(err)
		db.Close()
		return nil, false
	}

	newDB := new(DB)
	newDB.conn = db
	newDB.version, ok = DbGetVersionNum(db)
	if !ok {
		db.Close()
		return nil, false
	}
	if newDB.version >= 100000 {
		newDB.xlogOrWal = "wal"
	} else {
		newDB.xlogOrWal = "xlog"
	}

	return newDB, true
}

func (db *DB) Close() error {
	return db.conn.Close()
}

func sqlEscapeString(i string) string {
	return strings.ReplaceAll(i, "'", "''")
}

// pg_dumpacl stuff
func DumpCreateDBAndACL(out io.Writer, db *DB, dbname string) (int, error) {
	var n, i int

	if dbname == "" {
		return 0, fmt.Errorf("empty input dbname")
	}

	// this query only work from 9.0, where datcollate and datctype were added to pg_database
	if db.version < 90000 {
		l.Warnln("Cluster version is older than 9.0, not dumping ACL")
		return 0, nil
	}

	// this is no longer necessary after 11
	if db.version >= 110000 {
		return 0, nil
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
		l.Errorln(err)
		return 0, err
	}
	defer rows.Close()

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
			l.Errorln(err)
			return 0, err
		}

		if dbname != "template1" && dbname != "postgres" {
			i, _ = fmt.Fprintf(out, "--\n-- Database creation\n--\n\n")
			n += i
			i, _ = fmt.Fprintf(out, "CREATE DATABASE \"%s\" WITH TEMPLATE = template0 OWNER = \"%s\"", dbname, owner)
			n += i
			i, _ = fmt.Fprintf(out, " ENCODING = '%s'", sqlEscapeString(encoding))
			n += i
			i, _ = fmt.Fprintf(out, " LC_COLLATE = '%s'", sqlEscapeString(collate))
			n += i
			i, _ = fmt.Fprintf(out, " LC_CTYPE = '%s'", sqlEscapeString(ctype))
			n += i

			if tablespace != "pg_default" {
				i, _ = fmt.Fprintf(out, " TABLESPACE = \"%s\"", tablespace)
				n += i
			}
			if connlimit != -1 {
				i, _ = fmt.Fprintf(out, " CONNECTION LIMIT = %d", connlimit)
				n += i
			}
			i, _ = fmt.Fprintf(out, ";\n\n")
			n += i

			if istemplate {
				i, _ = fmt.Fprintf(out, "UPDATE pg_catalog.pg_database SET datistemplate = 't' WHERE datname = '%s';\n", sqlEscapeString(dbname))
				n += i
			}
		}
		for _, e := range acl {
			if !e.Valid {
				continue
			}

			// the aclitem format is "grantee=privs/grantor" where privs
			// is a list of letters, one for each privilege followed by *
			// when grantee as WITH GRANT OPTION for it
			s := strings.Split(e.String, "=")
			grantee := s[0]
			s = strings.Split(s[1], "/")
			privs := s[0]
			grantor := s[1]

			// public role: when the privs differ from the default, issue grants
			if grantee == "" {
				grantee = "PUBLIC"
				if privs != "Tc" {
					i, _ = fmt.Fprintf(out, "REVOKE ALL ON DATABASE \"%s\" FROM PUBLIC;\n", dbname)
					n += i
				} else {
					continue
				}
			}
			// owner: when other roles have been given privileges, all
			// privileges are shown for the owner
			if grantee == owner {
				if privs != "CTc" {
					i, _ = fmt.Fprintf(out, "REVOKE ALL ON DATABASE \"%s\" FROM \"%s\";\n", dbname, grantee)
					n += i
				} else {
					continue
				}
			}

			if grantor != owner {
				i, _ = fmt.Fprintf(out, "SET SESSION AUTHORIZATION \"%s\";\n", grantor)
				n += i
			}
			for i, b := range privs {
				switch b {
				case 'C':
					i, _ = fmt.Fprintf(out, "GRANT CREATE ON DATABASE \"%s\" TO \"%s\"", dbname, grantee)
					n += i
				case 'T':
					i, _ = fmt.Fprintf(out, "GRANT TEMPORARY ON DATABASE \"%s\" TO \"%s\"", dbname, grantee)
					n += i
				case 'c':
					i, _ = fmt.Fprintf(out, "GRANT CONNECT ON DATABASE \"%s\" TO \"%s\"", dbname, grantee)
					n += i
				}

				if i+1 < len(privs) {
					if privs[i+1] == '*' {
						i, _ = fmt.Fprintf(out, " WITH GRANT OPTION;\n")
						n += i
					} else {
						i, _ = fmt.Fprintf(out, ";\n")
						n += i
					}
				} else {
					i, _ = fmt.Fprintf(out, ";\n")
					n += i
				}
			}
			if grantor != owner {
				i, _ = fmt.Fprintf(out, "RESET SESSION AUTHORIZATION;\n")
				n += i
			}

		}
		// do not count this newline, we could have an empty
		// file with only this newline otherwise
		fmt.Fprintf(out, "\n")
	}
	err = rows.Err()
	if err != nil {
		l.Errorln(err)
		return 0, err
	}
	rows.Close()

	// dump config
	rows, err = db.conn.Query("SELECT unnest(setconfig) FROM pg_db_role_setting WHERE setrole = 0 AND setdatabase = (SELECT oid FROM pg_database WHERE datname = $1)", dbname)
	if err != nil {
		l.Errorln(err)
		return 0, err
	}
	defer rows.Close()

	for rows.Next() {
		var keyVal string

		err := rows.Scan(&keyVal)
		if err != nil {
			l.Errorln(err)
			return 0, err
		}

		// split
		tokens := strings.Split(keyVal, "=")

		// do not quote the value for those two parameters
		if tokens[0] != "DateStyle" && tokens[0] != "search_path" {
			tokens[1] = fmt.Sprintf("'%s'", tokens[1])
		}
		i, _ = fmt.Fprintf(out, "ALTER DATABASE \"%s\" SET \"%s\" TO %s;\n", dbname, tokens[0], tokens[1])
		n += i
	}
	err = rows.Err()
	if err != nil {
		l.Errorln(err)
		return 0, err
	}

	return n, nil
}

func ShowSettings(out io.Writer, db *DB) (int, error) {
	var n, i int
	// get the non default values set in the files and applied,
	// this avoid duplicates when multiple files define
	// parameters.
	rows, err := db.conn.Query("SELECT name, setting FROM pg_show_all_file_settings() WHERE applied ORDER BY name")
	if err != nil {
		l.Errorln(err)
		return 0, err
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
			return 0, err
		}

		if name != "DateStyle" && name != "search_path" {
			value = fmt.Sprintf("'%s'", value)
		}

		i, _ = fmt.Fprintf(out, "%s = %s\n", name, value)
		n += i
	}

	err = rows.Err()
	if err != nil {
		l.Errorln(err)
		return 0, err
	}

	return n, nil
}

func PauseReplication(db *DB) error {
	// If an AccessExclusiveLock is granted when the replay is
	// paused, it will remain and pg_dump would be stuck forever
	rows, err := db.conn.Query(fmt.Sprintf("SELECT pg_%s_replay_pause() "+
		"WHERE NOT EXISTS (SELECT 1 FROM pg_locks WHERE mode = 'AccessExclusiveLock') "+
		"AND pg_is_in_recovery();", db.xlogOrWal))
	if err != nil {
		l.Errorln(err)
		return err
	}
	defer rows.Close()

	// The query returns a single row with one column of type void,
	// which is and empty string, on success. It does not return
	// any row on failure
	void := "failed"
	for rows.Next() {
		err := rows.Scan(&void)
		if err != nil {
			l.Errorln(err)
			return err
		}
	}
	if void == "failed" {
		return fmt.Errorf("Replication not paused because of AccessExclusiveLock")
	}
	return nil
}

func CanPauseReplication(db *DB) (bool, error) {

	rows, err := db.conn.Query(fmt.Sprintf("SELECT 1 FROM pg_proc "+
		"WHERE proname='pg_%s_replay_pause' AND pg_is_in_recovery()", db.xlogOrWal))
	if err != nil {
		l.Errorln(err)
		return false, err
	}
	defer rows.Close()

	// The query returns 1 on success, no row on failure
	var one int
	for rows.Next() {
		err := rows.Scan(&one)
		if err != nil {
			l.Errorln(err)
			return false, err
		}
	}
	if one == 0 {
		return false, nil
	}

	return true, nil
}

func PauseReplicationWithTimeout(db *DB, timeOut int) error {

	if ok, err := CanPauseReplication(db); !ok {
		return err
	}

	ticker := time.NewTicker(time.Duration(10) * time.Second)
	done := make(chan bool)
	stop := make(chan bool)

	l.Infoln("Pausing replication")

	// We want to retry pausing replication at a defined interval
	// but not forever. We cannot put the timeout in the same
	// select as the ticker since the ticker would always win
	go func() {
		defer ticker.Stop()

		for {
			if err := PauseReplication(db); err != nil {
				l.Warnln(err)
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
		l.Infoln("Replication paused")
	case <-time.After(time.Duration(timeOut) * time.Second):
		stop <- true
		return fmt.Errorf("Replication not paused after %v\n", time.Duration(timeOut)*time.Second)
	}

	return nil
}

func ResumeReplication(db *DB) error {
	if ok, err := CanPauseReplication(db); !ok {
		return err
	}

	l.Infoln("Resuming replication")
	_, err := db.conn.Exec(fmt.Sprintf("SELECT pg_%s_replay_resume() WHERE pg_is_in_recovery();", db.xlogOrWal))
	if err != nil {
		l.Errorln(err)
		return err
	}

	return nil
}
