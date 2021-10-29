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
	"errors"
	"filippo.io/age"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func ageEncrypt(src io.Reader, dst io.Writer, password string) error {
	// Age encrypt to a recipient, Scrypt allow to create a key from a passphrase
	recipient, err := age.NewScryptRecipient(password)
	if err != nil {
		return fmt.Errorf("failed to create recipient from password: %w", err)
	}

	w, err := age.Encrypt(dst, recipient)
	if err != nil {
		return fmt.Errorf("failed to create encrypted file: %w", err)
	}

	if _, err := io.Copy(w, src); err != nil {
		return fmt.Errorf("failed to write to encrypted file: %w", err)
	}

	// It is mandatory to Close the writer from age so that it flushes its data
	w.Close()

	return nil
}

func ageDecrypt(src io.Reader, dst io.Writer, password string) error {

	identity, err := age.NewScryptIdentity(password)
	if err != nil {
		return fmt.Errorf("failed to create identity from password: %w", err)
	}

	r, err := age.Decrypt(src, identity)
	if err != nil {
		var badpass *age.NoIdentityMatchError
		if errors.As(err, &badpass) {
			return fmt.Errorf("invalid passphrase")
		}
		return fmt.Errorf("failed to initiate decryption: %w", err)
	}

	if _, err := io.Copy(dst, r); err != nil {
		return fmt.Errorf("failed to read encrypted data: %w", err)
	}

	return nil
}

func encryptFile(path string, password string, keep bool) error {
	i, err := os.Stat(path)
	if err != nil {
		return err
	}

	if i.IsDir() {
		l.Verboseln("dump is a directory, encrypting all files inside")
		err = filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.Mode().IsRegular() {
				l.Verboseln("encrypting:", path)

				src, err := os.Open(path)
				if err != nil {
					l.Errorln(err)
					return err
				}
				defer src.Close()

				dstFile := fmt.Sprintf("%s.age", path)
				dst, err := os.Create(dstPath)
				if err != nil {
					l.Errorln(err)
					return err
				}
				defer dst.Close()

				if err := ageEncrypt(src, dst, password); err != nil {
					dst.Close()
					os.Remove(dstPath)
					return fmt.Errorf("could not encrypt %s: %s", path, err)
				}

				if !keep {
					src.Close()
					if err := os.Remove(path); err != nil {
						return fmt.Errorf("could not remove %s: %w", path, err)
					}
				}
			}
			return nil
		})

		if err != nil {
			return fmt.Errorf("error walking the path %q: %v", path, err)
		}
	} else {
		l.Verboseln("encrypting:", path)
		src, err := os.Open(path)
		if err != nil {
			l.Errorln(err)
			return err
		}

		defer src.Close()

		dstFile := fmt.Sprintf("%s.age", path)
		dst, err := os.Create(dstFile)
		if err != nil {
			l.Errorln(err)
			return err
		}

		defer dst.Close()

		if err := ageEncrypt(src, dst, password); err != nil {
			dst.Close()
			os.Remove(dstFile)
			return fmt.Errorf("could not encrypt %s: %s", path, err)
		}

		if !keep {
			src.Close()
			if err := os.Remove(path); err != nil {
				return fmt.Errorf("could not remove %s: %w", path, err)
			}
		}
	}

	return nil
}

func decryptFile(path string, password string) error {
	l.Infoln("decrypting", path)

	src, err := os.Open(path)
	if err != nil {
		return err
	}

	defer src.Close()

	dstFile := strings.TrimSuffix(path, ".age")
	dst, err := os.Create(dstFile)
	if err != nil {
		return err
	}

	defer dst.Close()

	if err := ageDecrypt(src, dst, password); err != nil {
		dst.Close()
		os.Remove(dstFile)
		return fmt.Errorf("could not decrypt %s: %s", path, err)
	}

	return nil
}
