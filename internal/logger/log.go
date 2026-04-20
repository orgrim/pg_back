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

package logger

import (
	"log"
	"os"
)

// LevelLog custom type to allow a verbose mode and handling of levels
// with a prefix
type LevelLog struct {
	Logger  *log.Logger
	verbose bool
	quiet   bool
}

// NewLevelLog setups a logger with the proper configuration for the underlying log
func NewLevelLog() *LevelLog {
	return &LevelLog{
		Logger:  log.New(os.Stderr, "", log.LstdFlags|log.Lmsgprefix),
		verbose: false,
		quiet:   false,
	}
}

// SetVerbose toggles verbose mode
func (l *LevelLog) SetVerbosity(verbose bool, quiet bool) {
	if quiet {
		l.quiet = quiet
		l.verbose = false

		// Quiet mode takes over verbose mode
		return
	}

	l.verbose = verbose
	if verbose {
		l.Logger.SetFlags(log.LstdFlags | log.Lmsgprefix | log.Lmicroseconds)
	}
}

// Verbosef prints with log.Printf a message with DEBUG: prefix using log.Printf, only when verbose mode is true
func (l *LevelLog) Verbosef(format string, v ...interface{}) {
	if l.verbose {
		l.Logger.SetPrefix("DEBUG: ")
		l.Logger.Printf(format, v...)
	}
}

// Verboseln prints a message with DEBUG: prefix using log.Println, only when verbose mode is true
func (l *LevelLog) Verboseln(v ...interface{}) {
	if l.verbose {
		l.Logger.SetPrefix("DEBUG: ")
		l.Logger.Println(v...)
	}
}

// Infof prints a message with INFO: prefix using log.Printf
func (l *LevelLog) Infof(format string, v ...interface{}) {
	if !l.quiet {
		l.Logger.SetPrefix("INFO: ")
		l.Logger.Printf(format, v...)
	}
}

// Infoln prints a message with INFO: prefix using log.Println
func (l *LevelLog) Infoln(v ...interface{}) {
	if !l.quiet {
		l.Logger.SetPrefix("INFO: ")
		l.Logger.Println(v...)
	}
}

// Warnf prints a message with WARN: prefix using log.Printf
func (l *LevelLog) Warnf(format string, v ...interface{}) {
	l.Logger.SetPrefix("WARN: ")
	l.Logger.Printf(format, v...)
}

// Warnln prints a message with WARN: prefix using log.Println
func (l *LevelLog) Warnln(v ...interface{}) {
	l.Logger.SetPrefix("WARN: ")
	l.Logger.Println(v...)
}

// Errorf prints a message with ERROR: prefix using log.Printf
func (l *LevelLog) Errorf(format string, v ...interface{}) {
	l.Logger.SetPrefix("ERROR: ")
	l.Logger.Printf(format, v...)
}

// Errorln prints a message with ERROR: prefix using log.Println
func (l *LevelLog) Errorln(v ...interface{}) {
	l.Logger.SetPrefix("ERROR: ")
	l.Logger.Println(v...)
}

// Fatalf prints a message with FATAL: prefix using log.Printf
func (l *LevelLog) Fatalf(format string, v ...interface{}) {
	l.Logger.SetPrefix("FATAL: ")
	l.Logger.Printf(format, v...)
}

// Fatalln prints a message with FATAL: prefix using log.Println
func (l *LevelLog) Fatalln(v ...interface{}) {
	l.Logger.SetPrefix("FATAL: ")
	l.Logger.Println(v...)
}
