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
	"bytes"
	b64 "encoding/base64"
	"strings"
	"testing"

	"filippo.io/age"
)

const TEST_PRIVATE_KEY = "AGE-SECRET-KEY-1XLVVN6PHUZNFFFRA0AGLEJ22GWDGN6WG8KFDV56FH5DC9P2Y8F2SPH8W44"
const TEST_PUBLIC_KEY = "age1702xupy5u4d6a5z2dwcn9e2th4mqpth5kvl3nmhq063gf70d9awsl37jn6"
const TEST_ENCRYPTED_FILE_BASE64 = "YWdlLWVuY3J5cHRpb24ub3JnL3YxCi0+IFgyNTUxOSBzanBtS3hmUGQwekNER0hwWjNod1Z2Y1FCVEZGMkExdmRlcS9vUy8vTEZBClgvOXB3QjRLN2E3aERGUmFMSXdiM3h4R0JFTytwb0hsSEpJV0NTVk1mME0KLS0tIHpqSnRhc0F6NEZ6b0R6aEl5U0V3cnNmL2pKRWVwNHd3dU9wdExjeWx0Tk0KL9m6JZXXAeEZBA7w7nuyrl4ztjY2+Ypu1GNrL6bjv7aw+ACqVGZZwLDI6Q=="
const TEST_PLAINTEXT_FILE = "test string"

func TestAgeEncrypt_NilParams_Failure(t *testing.T) {
	content := "to be encrypted"
	reader := strings.NewReader(content)
	writer := &bytes.Buffer{}
	params := encryptParams{}

	err := ageEncrypt(reader, writer, params)
	if err == nil {
		t.Errorf("Expected empty encryption params to fail")
	}
}

func TestAgeDecrypt_NilParams_Failure(t *testing.T) {
	content := "to be encrypted"
	reader := strings.NewReader(content)
	writer := &bytes.Buffer{}
	params := decryptParams{}

	err := ageDecrypt(reader, writer, params)
	if err == nil {
		t.Errorf("Expected empty encryption params to fail")
	}
}

func TestAgeDecrypt_InvalidPrivateKey_Failure(t *testing.T) {
	encrypted, err := b64.StdEncoding.DecodeString(TEST_ENCRYPTED_FILE_BASE64)
	if err != nil {
		t.Fatalf("could not decode golden string")
	}
	reader := bytes.NewReader(encrypted)
	writer := &bytes.Buffer{}
	params := decryptParams{PrivateKey: TEST_PUBLIC_KEY}

	err = ageDecrypt(reader, writer, params)
	if err == nil {
		t.Errorf("Expected invalid private key to fail")
	}
}

func TestAgeDecrypt_InvalidPublicKey_Failure(t *testing.T) {
	content := "to be encrypted"
	reader := strings.NewReader(content)
	writer := &bytes.Buffer{}
	params := decryptParams{PrivateKey: TEST_PRIVATE_KEY}

	err := ageDecrypt(reader, writer, params)
	if err == nil {
		t.Errorf("Expected invalid public key to fail")
	}
}

func TestAgeEncryptPassphrase_EmptyPassphrase_Failure(t *testing.T) {
	content := "to be encrypted"
	reader := strings.NewReader(content)
	writer := &bytes.Buffer{}

	err := ageEncryptPassphrase(reader, writer, "")
	if err == nil {
		t.Errorf("Expected empty passphrase to fail")
	}
}

func TestAgeDecryptPassphrase_EmptyPassphrase_Failure(t *testing.T) {
	encrypted, err := b64.StdEncoding.DecodeString(TEST_ENCRYPTED_FILE_BASE64)
	if err != nil {
		t.Fatalf("could not decode golden string")
	}
	reader := bytes.NewReader(encrypted)
	writer := &bytes.Buffer{}

	err = ageDecryptPassphrase(reader, writer, "")
	if err == nil {
		t.Errorf("Expected empty passphrase to fail")
	}
}

func TestAgeDecrypt_Golden_Success(t *testing.T) {
	encrypted, err := b64.StdEncoding.DecodeString(TEST_ENCRYPTED_FILE_BASE64)
	if err != nil {
		t.Fatalf("could not decode golden string")
	}
	reader := bytes.NewReader(encrypted)
	writer := &bytes.Buffer{}
	params := decryptParams{
		PrivateKey: TEST_PRIVATE_KEY,
	}

	err = ageDecrypt(reader, writer, params)
	if err != nil {
		t.Fatalf("could not decrypt golden message: %v", err)
	}

	if writer.String() != TEST_PLAINTEXT_FILE {
		t.Errorf("got %v want %v", writer.String(), TEST_PLAINTEXT_FILE)
	}
}

func TestAgeEncrypt_PublicKey_Loopback_Success(t *testing.T) {
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}

	content := "to be encrypted"
	reader := strings.NewReader(content)
	writer := &bytes.Buffer{}
	params := encryptParams{PublicKey: identity.Recipient().String()}

	err = ageEncrypt(reader, writer, params)
	if err != nil {
		t.Errorf("Unexpected error when encrypting")
	}

	ciphertext := writer.String()
	if ciphertext == "" {
		t.Errorf("encrypted output is empty")
	}

	reader = strings.NewReader(ciphertext)
	writer = &bytes.Buffer{}
	decryptParams := decryptParams{PrivateKey: identity.String()}
	err = ageDecrypt(reader, writer, decryptParams)
	if err != nil {
		t.Errorf("Unexpected error when decrypting")
	}

	if writer.String() != content {
		t.Errorf("Did not decrypt to same plaintext")
	}
}

func TestAgeEncrypt_Passphrase_Loopback_Success(t *testing.T) {
	content := "to be encrypted"
	reader := strings.NewReader(content)
	writer := &bytes.Buffer{}
	params := encryptParams{Passphrase: "supersecret"}

	err := ageEncrypt(reader, writer, params)
	if err != nil {
		t.Errorf("Unexpected error when encrypting")
	}

	ciphertext := writer.String()
	if ciphertext == "" {
		t.Errorf("encrypted output is empty")
	}

	reader = strings.NewReader(ciphertext)
	writer = &bytes.Buffer{}
	decryptParams := decryptParams{Passphrase: "supersecret"}
	err = ageDecrypt(reader, writer, decryptParams)
	if err != nil {
		t.Errorf("Unexpected error when decrypting")
	}

	if writer.String() != content {
		t.Errorf("Did not decrypt to same plaintext")
	}
}

func TestAgeEncrypt_WrongPrivateKey_Loopback_Failure(t *testing.T) {
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}

	content := "to be encrypted"
	reader := strings.NewReader(content)
	writer := &bytes.Buffer{}
	params := encryptParams{PublicKey: identity.Recipient().String()}

	err = ageEncrypt(reader, writer, params)
	if err != nil {
		t.Errorf("Unexpected error when encrypting")
	}

	ciphertext := writer.String()
	if ciphertext == "" {
		t.Errorf("encrypted output is empty")
	}

	wrongIdentity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}

	reader = strings.NewReader(ciphertext)
	writer = &bytes.Buffer{}
	decryptParams := decryptParams{PrivateKey: wrongIdentity.String()}
	err = ageDecrypt(reader, writer, decryptParams)
	if err == nil {
		t.Errorf("Decryption should have failed")
	}
}

func TestAgeEncrypt_WrongPassphrase_Loopback_Failure(t *testing.T) {
	content := "to be encrypted"
	reader := strings.NewReader(content)
	writer := &bytes.Buffer{}
	params := encryptParams{Passphrase: "supersecret"}

	err := ageEncrypt(reader, writer, params)
	if err != nil {
		t.Errorf("Unexpected error when encrypting")
	}

	ciphertext := writer.String()
	if ciphertext == "" {
		t.Errorf("encrypted output is empty")
	}

	reader = strings.NewReader(ciphertext)
	writer = &bytes.Buffer{}
	decryptParams := decryptParams{Passphrase: "wrong"}
	err = ageDecrypt(reader, writer, decryptParams)
	if err == nil {
		t.Fatalf("Decryption should have failed")
	}
}
