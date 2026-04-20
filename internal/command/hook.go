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

package command

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/anmitsu/go-shlex"
	"github.com/orgrim/pg_back/internal/logger"
)

func hookCommand(logger *logger.LevelLog, cmd string, logPrefix string) error {
	if cmd == "" {
		return fmt.Errorf("unable to run an empty command")
	}

	logger.Verboseln("parsing hook command")
	words, err := shlex.Split(cmd, true)
	if err != nil {
		return fmt.Errorf("unable to parse hook command: %s", err)
	}

	prog := words[0]
	args := words[1:]

	logger.Verboseln("running:", prog, args)
	c := exec.Command(prog, args...)
	stdoutStderr, err := c.CombinedOutput()
	if err != nil {
		for line := range strings.SplitSeq(string(stdoutStderr), "\n") {
			if line != "" {
				logger.Errorln(logPrefix, line)
			}
		}
		return err
	}
	if len(stdoutStderr) > 0 {
		for line := range strings.SplitSeq(string(stdoutStderr), "\n") {
			if line != "" {
				logger.Infoln(logPrefix, line)
			}
		}
	}
	return nil
}

func PreBackupHook(logger *logger.LevelLog, cmd string) error {
	if cmd != "" {
		logger.Infoln("running pre-backup command:", cmd)
		if err := hookCommand(logger, cmd, "pre-backup:"); err != nil {
			logger.Fatalln("hook command failed:", err)
			return err
		}
	}
	return nil
}

func PostBackupHook(logger *logger.LevelLog, cmd string) {
	if cmd != "" {
		logger.Infoln("running post-backup command:", cmd)
		if err := hookCommand(logger, cmd, "post-backup:"); err != nil {
			logger.Fatalln("hook command failed:", err)
			os.Exit(1)
		}
	}
}
