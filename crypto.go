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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"filippo.io/age"
)

func ageEncrypt(src io.Reader, dst io.Writer, params encryptParams) error {
	if params.PublicKey != "" {
		return ageEncryptPublicKey(src, dst, params.PublicKey)
	}

	if params.Passphrase != "" {
		return ageEncryptPassphrase(src, dst, params.Passphrase)
	}

	return fmt.Errorf("Unexpected condition: no public key or passphrase")
}

func ageEncryptPassphrase(src io.Reader, dst io.Writer, passphrase string) error {
	// Age encrypt to a recipient, Scrypt allow to create a key from a passphrase
	recipient, err := age.NewScryptRecipient(passphrase)
	if err != nil {
		return fmt.Errorf("failed to create recipient from passphrase: %w", err)
	}

	return ageEncryptInternal(src, dst, recipient)
}

func ageEncryptPublicKey(src io.Reader, dst io.Writer, publicKey string) error {
	recipient, err := age.ParseX25519Recipient(publicKey)
	if err != nil {
		return fmt.Errorf("failed to create recipient from public key: %w", err)
	}

	return ageEncryptInternal(src, dst, recipient)
}

func ageEncryptInternal(src io.Reader, dst io.Writer, recipient age.Recipient) error {
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

func ageDecrypt(src io.Reader, dst io.Writer, params decryptParams) error {
	if params.PrivateKey != "" {
		return ageDecryptPrivateKey(src, dst, params.PrivateKey)
	}

	if params.Passphrase != "" {
		return ageDecryptPassphrase(src, dst, params.Passphrase)
	}

	return fmt.Errorf("No private key or passphrase specified")
}

func ageDecryptPrivateKey(src io.Reader, dst io.Writer, privateKey string) error {
	identity, err := age.ParseX25519Identity(privateKey)
	if err != nil {
		return fmt.Errorf("failed to parse AGE private key: %w", err)
	}

	return ageDecryptInternal(src, dst, identity)
}

func ageDecryptPassphrase(src io.Reader, dst io.Writer, passphrase string) error {
	identity, err := age.NewScryptIdentity(passphrase)
	if err != nil {
		return fmt.Errorf("failed to create identity from passphrase: %w", err)
	}

	return ageDecryptInternal(src, dst, identity)
}

func ageDecryptInternal(src io.Reader, dst io.Writer, identity age.Identity) error {
	r, err := age.Decrypt(src, identity)
	if err != nil {
		var badpass *age.NoIdentityMatchError
		if errors.As(err, &badpass) {
			return fmt.Errorf("invalid key or passphrase")
		}
		return fmt.Errorf("failed to initiate decryption: %w", err)
	}

	if _, err := io.Copy(dst, r); err != nil {
		return fmt.Errorf("failed to read encrypted data: %w", err)
	}

	return nil
}

func encryptFile(path string, params encryptParams, keep bool) ([]string, error) {
	encrypted := make([]string, 0)

	i, err := os.Stat(path)
	if err != nil {
		return encrypted, err
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
				dst, err := os.Create(dstFile)
				if err != nil {
					l.Errorln(err)
					return err
				}
				defer dst.Close()

				if err := ageEncrypt(src, dst, params); err != nil {
					dst.Close()
					os.Remove(dstFile)
					return fmt.Errorf("could not encrypt %s: %s", path, err)
				}

				encrypted = append(encrypted, dstFile)

				if !keep {
					l.Verboseln("removing source file:", path)
					src.Close()
					if err := os.Remove(path); err != nil {
						return fmt.Errorf("could not remove %s: %w", path, err)
					}
				}
			}
			return nil
		})

		if err != nil {
			return encrypted, fmt.Errorf("error walking the path %q: %v", path, err)
		}
	} else {
		l.Verboseln("encrypting:", path)
		src, err := os.Open(path)
		if err != nil {
			l.Errorln(err)
			return encrypted, err
		}

		defer src.Close()

		dstFile := fmt.Sprintf("%s.age", path)
		dst, err := os.Create(dstFile)
		if err != nil {
			l.Errorln(err)
			return encrypted, err
		}

		defer dst.Close()

		if err := ageEncrypt(src, dst, params); err != nil {
			dst.Close()
			os.Remove(dstFile)
			return encrypted, fmt.Errorf("could not encrypt %s: %s", path, err)
		}

		encrypted = append(encrypted, dstFile)

		if !keep {
			l.Verboseln("removing source file:", path)
			src.Close()
			if err := os.Remove(path); err != nil {
				return encrypted, fmt.Errorf("could not remove %s: %w", path, err)
			}
		}
	}

	return encrypted, nil
}

func decryptFile(path string, params decryptParams) error {
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

	if err := ageDecrypt(src, dst, params); err != nil {
		dst.Close()
		os.Remove(dstFile)
		return fmt.Errorf("could not decrypt %s: %s", path, err)
	}

	return nil
}
