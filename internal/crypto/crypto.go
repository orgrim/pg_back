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

package crypto

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"filippo.io/age"
	"github.com/orgrim/pg_back/internal/helpers"
	"github.com/orgrim/pg_back/internal/logger"
)

type EncryptParams struct {
	Logger *logger.LevelLog

	// Encrypt with a passphrase
	Passphrase string

	// Encrypt with an AGE public key encoded in Bech32
	PublicKey string
}

type DecryptParams struct {
	Logger *logger.LevelLog

	// A passphrase to use for decryption
	Passphrase string

	// An AGE private key encoded in Bech32
	PrivateKey string
}

func (params *EncryptParams) ageEncrypt(src io.Reader, dst io.Writer) error {
	if params.PublicKey != "" {
		return ageEncryptPublicKey(src, dst, params.PublicKey)
	}

	if params.Passphrase != "" {
		return ageEncryptPassphrase(src, dst, params.Passphrase)
	}

	return fmt.Errorf("unexpected condition: no public key or passphrase")
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
	if err := w.Close(); err != nil {
		return fmt.Errorf("failed to close encrypted file: %w", err)
	}

	return nil
}

func (params *DecryptParams) ageDecrypt(src io.Reader, dst io.Writer) error {
	if params.PrivateKey != "" {
		return ageDecryptPrivateKey(src, dst, params.PrivateKey)
	}

	if params.Passphrase != "" {
		return ageDecryptPassphrase(src, dst, params.Passphrase)
	}

	return fmt.Errorf("no private key or passphrase specified")
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

func (params *EncryptParams) EncryptFile(
	path string,
	mode int,
	keep bool,
) (_ []string, err error) {
	encrypted := make([]string, 0)

	i, err := os.Stat(path)
	if err != nil {
		return encrypted, err
	}

	if i.IsDir() {
		params.Logger.Verboseln("dump is a directory, encrypting all files inside")
		err = filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.Mode().IsRegular() {
				params.Logger.Verboseln("encrypting:", path)

				src, err := os.Open(path)
				if err != nil {
					params.Logger.Errorln(err)
					return err
				}
				defer helpers.WrappedClose(src, &err)

				dstFile := fmt.Sprintf("%s.age", path)
				dst, err := os.Create(dstFile)
				if err != nil {
					params.Logger.Errorln(err)
					return err
				}
				defer helpers.WrappedClose(dst, &err)

				if err := params.ageEncrypt(src, dst); err != nil {
					// explicitly ignore error on close and remove
					dst.Close()        //nolint:errcheck
					os.Remove(dstFile) //nolint:errcheck
					return fmt.Errorf("could not encrypt %s: %s", path, err)
				}

				encrypted = append(encrypted, dstFile)
				if mode > 0 {
					if err := os.Chmod(dstFile, os.FileMode(mode)); err != nil {
						return fmt.Errorf(
							"could not chmod to more secure permission for encrypted file: %w",
							err,
						)
					}
				}

				if !keep {
					params.Logger.Verboseln("removing source file:", path)
					if err := src.Close(); err != nil {
						return fmt.Errorf("could not close %s: %w", path, err)
					}
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
		params.Logger.Verboseln("encrypting:", path)
		src, err := os.Open(path)
		if err != nil {
			params.Logger.Errorln(err)
			return encrypted, err
		}

		defer helpers.WrappedClose(src, &err)

		dstFile := fmt.Sprintf("%s.age", path)
		dst, err := os.Create(dstFile)
		if err != nil {
			params.Logger.Errorln(err)
			return encrypted, err
		}

		defer helpers.WrappedClose(dst, &err)

		if err := params.ageEncrypt(src, dst); err != nil {
			// explicitly ignore error here, we already return an error
			dst.Close()        //nolint:errcheck
			os.Remove(dstFile) //nolint:errcheck
			return encrypted, fmt.Errorf("could not encrypt %s: %s", path, err)
		}

		encrypted = append(encrypted, dstFile)
		if mode > 0 {
			if err := os.Chmod(dstFile, os.FileMode(mode)); err != nil {
				return encrypted, fmt.Errorf("could not chmod to more secure permission for encrypted file: %w", err)
			}
		}
		if !keep {
			params.Logger.Verboseln("removing source file:", path)
			if err := src.Close(); err != nil {
				return encrypted, fmt.Errorf("could not close %s: %w", path, err)
			}
			if err := os.Remove(path); err != nil {
				return encrypted, fmt.Errorf("could not remove %s: %w", path, err)
			}
		}
	}

	return encrypted, err
}

func (params *DecryptParams) DecryptFile(path string) (err error) {
	params.Logger.Infoln("decrypting", path)

	src, err := os.Open(path)
	if err != nil {
		return err
	}

	defer helpers.WrappedClose(src, &err)

	dstFile := strings.TrimSuffix(path, ".age")
	dst, err := os.Create(dstFile)
	if err != nil {
		return err
	}

	defer helpers.WrappedClose(dst, &err)

	if err := params.ageDecrypt(src, dst); err != nil {
		// explicitly ignore error on close and remove
		dst.Close()        //nolint:errcheck
		os.Remove(dstFile) //nolint:errcheck
		return fmt.Errorf("could not decrypt %s: %s", path, err)
	}

	return err
}
