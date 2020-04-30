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
	"fmt"
	"os"
	"database/sql"
	_ "github.com/lib/pq"
)

type DB struct {
	conn *sql.DB
	version int
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
		query string
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
