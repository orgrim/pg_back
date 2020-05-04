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
func DumpCreateDB(out io.Writer, db *DB, dbname string) error {

	if dbname == "" {
		return fmt.Errorf("empty input dbname")
	}

	// this query only work from 9.0, where datcollate and datctype were added to pg_database
	if db.version < 90000 {
		l.Warnln("Cluster version is older than 9.0, not dumping ACL")
		return nil
	}

	// this is no longer necessary after 11
	if db.version >= 110000 {
		return nil
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
	defer rows.Close()
	if err != nil {
		l.Errorln(err)
		return err
	}
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
			return err
		}

		if dbname != "template1" && dbname != "postgres" {
			fmt.Fprintf(out, "CREATE DATABASE \"%s\" WITH TEMPLATE = template0 OWNER = \"%s\"", dbname, owner)
			fmt.Fprintf(out, " ENCODING = '%s'", sqlEscapeString(encoding))
			fmt.Fprintf(out, " LC_COLLATE = '%s'", sqlEscapeString(collate))
			fmt.Fprintf(out, " LC_CTYPE = '%s'", sqlEscapeString(ctype))

			if tablespace != "pg_default" {
				fmt.Fprintf(out, " TABLESPACE = \"%s\"", tablespace)
			}
			if connlimit != -1 {
				fmt.Fprintf(out, " CONNECTION LIMIT = %d", connlimit)
			}
			fmt.Fprintf(out, ";\n")

			if istemplate {
				fmt.Fprintf(out, "UPDATE pg_catalog.pg_database SET datistemplate = 't' WHERE datname = '%s';\n", sqlEscapeString(dbname))
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
						fmt.Fprintf(out, "REVOKE ALL ON DATABASE \"%s\" FROM PUBLIC;\n", dbname)
					} else {
						continue
					}
				}
				// owner: when other roles have been given privileges, all
				// privileges are shown for the owner
				if grantee == owner {
					if privs != "CTc" {
						fmt.Fprintf(out, "REVOKE ALL ON DATABASE \"%s\" FROM \"%s\";\n", dbname, grantee)
					} else {
						continue
					}
				}

				if grantor != owner {
					fmt.Fprintf(out, "SET SESSION AUTHORIZATION \"%s\";\n", grantor)
				}
				for i, b := range privs {
					switch b {
					case 'C':
						fmt.Fprintf(out, "GRANT CREATE ON DATABASE \"%s\" TO \"%s\"", dbname, grantee)
					case 'T':
						fmt.Fprintf(out, "GRANT TEMPORARY ON DATABASE \"%s\" TO \"%s\"", dbname, grantee)
					case 'c':
						fmt.Fprintf(out, "GRANT CONNECT ON DATABASE \"%s\" TO \"%s\"", dbname, grantee)
					}

					if i+1 < len(privs) {
						if privs[i+1] == '*' {
							fmt.Fprintf(out, " WITH GRANT OPTION;\n")
						} else {
							fmt.Fprintf(out, ";\n")
						}
					} else {
						fmt.Fprintf(out, ";\n")
					}
				}
				if grantor != owner {
					fmt.Fprintf(out, "RESET SESSION AUTHORIZATION;\n")
				}

			}

		}

	}
	err = rows.Err()
	if err != nil {
		l.Errorln(err)
		return err
	}
	return nil
}
