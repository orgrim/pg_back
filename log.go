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
	"os"
	"log"
)

type LevelLog struct {
	logger *log.Logger
	verbose bool
}

var l = NewLevelLog()

func NewLevelLog() (* LevelLog) {
	return &LevelLog{
		logger: log.New(os.Stderr, "", log.LstdFlags|log.Lmsgprefix),
		verbose: false,
	}
}

// Verbose
func (l *LevelLog) SetVerbose(verbose bool) {
	l.verbose = verbose
}

func (l *LevelLog) Verbosef(format string, v ...interface{}) {
	if l.verbose {
		l.logger.SetPrefix("DEBUG: ")
		l.logger.Printf(format, v...)
	}
}

func (l *LevelLog) Verboseln(v ...interface{}) {
	if l.verbose {
		l.logger.SetPrefix("DEBUG: ")
		l.logger.Println(v...)
	}
}

// Info
func (l *LevelLog) Infof(format string, v ...interface{}) {
	l.logger.SetPrefix("INFO: ")
	l.logger.Printf(format, v...)
}

func (l *LevelLog) Infoln(v ...interface{}) {
	l.logger.SetPrefix("INFO: ")
	l.logger.Println(v...)
}

// Warn
func (l *LevelLog) Warnf(format string, v ...interface{}) {
	l.logger.SetPrefix("WARN: ")
	l.logger.Printf(format, v...)
}

func (l *LevelLog) Warnln(v ...interface{}) {
	l.logger.SetPrefix("WARN: ")
	l.logger.Println(v...)
}

// Error
func (l *LevelLog) Errorf(format string, v ...interface{}) {
	l.logger.SetPrefix("ERROR: ")
	l.logger.Printf(format, v...)
}

func (l *LevelLog) Errorln(v ...interface{}) {
	l.logger.SetPrefix("ERROR: ")
	l.logger.Println(v...)
}

// Fatal is the same as log.Fatalx with a prefix
func (l *LevelLog) Fatalf(format string, v ...interface{}) {
	l.logger.SetPrefix("FATAL: ")
	l.logger.Fatalf(format, v...)
}

func (l *LevelLog) Fatalln(v ...interface{}) {
	l.logger.SetPrefix("FATAL: ")
	l.logger.Fatalln(v...)
}
