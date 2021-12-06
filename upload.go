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
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"
	"io"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"
)

// A Repo is a remote service where we can upload files
type Repo interface {
	// Upload a path to the remote naming it target
	Upload(path string, target string) error

	// List remote files starting with a prefix. the prefix can be empty to
	// list all files
	List(prefix string) ([]Item, error)

	// Remove path from the remote
	Remove(path string) error

	// Close cleans up any open resource
	Close() error
}

type Item struct {
	key     string
	modtime time.Time
	isDir   bool
}

type s3repo struct {
	region     string
	bucket     string
	profile    string
	keyID      string
	secret     string
	endPoint   string
	forcePath  bool
	disableSSL bool
	session    *session.Session
}

func NewS3Repo(opts options) (*s3repo, error) {
	r := &s3repo{
		region:     opts.S3Region,
		bucket:     opts.S3Bucket,
		profile:    opts.S3Profile,
		keyID:      opts.S3KeyID,
		secret:     opts.S3Secret,
		endPoint:   opts.S3EndPoint,
		forcePath:  opts.S3ForcePath,
		disableSSL: opts.S3DisableTLS,
	}

	conf := aws.NewConfig()
	if r.region != "" {
		conf = conf.WithRegion(r.region)
	}

	if r.keyID != "" {
		conf = conf.WithCredentials(credentials.NewStaticCredentials(r.keyID, r.secret, ""))
	}

	if r.endPoint != "" {
		conf = conf.WithEndpoint(r.endPoint)
	}

	if r.forcePath {
		conf = conf.WithS3ForcePathStyle(true)
	}

	if r.disableSSL {
		conf = conf.WithDisableSSL(true)
	}

	sopts := session.Options{
		Config:            *conf,
		SharedConfigState: session.SharedConfigEnable,
	}

	if r.profile != "" {
		sopts.Profile = r.profile
	}

	session, err := session.NewSessionWithOptions(sopts)
	if err != nil {
		return nil, fmt.Errorf("could not create AWS session: %w", err)
	}

	r.session = session

	return r, nil
}

func (r *s3repo) Close() error {
	return nil
}

func (r *s3repo) Upload(path string, target string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("upload error: %w", err)
	}
	defer file.Close()

	uploader := s3manager.NewUploader(r.session)

	l.Infof("uploading %s to S3 bucket %s\n", path, r.bucket)
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(target),
		Body:   file,
	})

	if err != nil {
		return fmt.Errorf("unable to upload %q to %q: %w", path, r.bucket, err)
	}

	return nil
}

func (r *s3repo) List(prefix string) ([]Item, error) {
	svc := s3.New(r.session)

	files := make([]Item, 0)

	var contToken *string

	for {
		resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket:            aws.String(r.bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: contToken,
		})

		if err != nil {
			return files, fmt.Errorf("could not list items in S3 bucket %s: %w", r.bucket, err)
		}

		for _, item := range resp.Contents {
			file := Item{
				key:     *item.Key,
				modtime: *item.LastModified,
			}

			files = append(files, file)
		}

		if !*resp.IsTruncated {
			break
		}

		contToken = resp.NextContinuationToken
	}

	return files, nil
}

func (r *s3repo) Remove(path string) error {
	svc := s3.New(r.session)

	_, err := svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(path),
	})

	if err != nil {
		return fmt.Errorf("could not remove %s from S3 bucket %s: %w", path, r.bucket, err)
	}

	return nil
}

type sftpRepo struct {
	host             string
	port             string
	user             string
	password         string
	identityFile     string
	baseDir          string
	disableHostCheck bool
	conn             *ssh.Client
	client           *sftp.Client
}

func expandHomeDir(path string) (string, error) {
	expanded := filepath.Clean(path)

	if strings.HasPrefix(path, "~") {
		var (
			homeDir string
			err     error
		)

		parts := strings.SplitN(path, "/", 2)
		username := parts[0][1:]

		if username == "" {
			homeDir, err = os.UserHomeDir()
			if err != nil || homeDir == "" {
				u, err := user.Current()
				if err != nil {
					return expanded, fmt.Errorf("could not expand ~: %w", err)
				}

				homeDir = u.HomeDir
				if homeDir == "" {
					return expanded, fmt.Errorf("could not expand ~: empty home directory")
				}
			}

		} else {
			u, err := user.Lookup(username)
			if err != nil {
				return expanded, fmt.Errorf("could not expand ~%s: %w", username, err)
			}

			homeDir = u.HomeDir
			if homeDir == "" {
				return expanded, fmt.Errorf("could not expand ~%s: empty home directory", username)
			}
		}

		expanded = filepath.Clean(filepath.Join(homeDir, parts[1]))
	}

	return expanded, nil
}

func hostKeyCheck(ignore bool) ssh.HostKeyCallback {
	if ignore {
		return ssh.InsecureIgnoreHostKey()
	}

	knownHostsFiles := make([]string, 0)
	for _, p := range []string{"/etc/ssh/ssh_known_hosts", "~/.ssh/known_hosts"} {
		path, err := expandHomeDir(p)
		if err != nil {
			continue
		}

		// Check if the file is there to mitigate a complete failure
		// when loading all files in knownhosts.New()
		_, err = os.Stat(path)
		if err != nil {
			continue
		}

		knownHostsFiles = append(knownHostsFiles, path)
	}

	if len(knownHostsFiles) == 0 {
		// No host keys can be loaded for checking, return a callback
		// that fails all the time
		return func(hostname string, remote net.Addr, key ssh.PublicKey) error {
			return fmt.Errorf("ssh: no local keys to check host key")
		}
	}

	knownHostsKeyCb, err := knownhosts.New(knownHostsFiles...)
	if err != nil {
		return func(hostname string, remote net.Addr, key ssh.PublicKey) error {
			return fmt.Errorf("ssh: unable to load local keys to check host key")
		}
	}

	return knownHostsKeyCb
}

func pubKeyAuth(identity string, passphrase string) ([]ssh.Signer, error) {
	signers := make([]ssh.Signer, 0)

	if identity != "" {
		path, err := expandHomeDir(identity)
		if err != nil {
			return nil, fmt.Errorf("ssh: unable to load private key: %w", err)
		}

		key, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("ssh: could not read %s: %w", path, err)
		}

		signer, err := ssh.ParsePrivateKey(key)
		if err != nil {
			var passError *ssh.PassphraseMissingError
			if errors.As(err, &passError) {
				signer, err = ssh.ParsePrivateKeyWithPassphrase(key, []byte(passphrase))
				if err != nil {
					return nil, fmt.Errorf("ssh: could not decrypt %s: %w", path, err)
				}
			} else {
				return nil, fmt.Errorf("ssh: could not parse %s: %w", path, err)
			}
		}

		signers = append(signers, signer)
	}

	// ssh-agent(1) provides a UNIX socket at $SSH_AUTH_SOCK. We try to get
	// its keys but do not fail if it is not available
	socket := os.Getenv("SSH_AUTH_SOCK")
	conn, err := net.Dial("unix", socket)
	if err != nil {
		return signers, nil
	}

	agentClient := agent.NewClient(conn)
	agentSigners, err := agentClient.Signers()
	if err != nil {
		return signers, nil
	}

	signers = append(signers, agentSigners...)

	return signers, nil
}

func NewSFTPRepo(opts options) (*sftpRepo, error) {
	r := &sftpRepo{
		host:             opts.SFTPHost,
		port:             opts.SFTPPort,
		user:             opts.SFTPUsername,
		password:         opts.SFTPPassword,
		baseDir:          opts.SFTPDirectory,
		identityFile:     opts.SFTPIdentityFile,
		disableHostCheck: opts.SFTPIgnoreKnownHosts,
	}

	if r.port == "" {
		r.port = "22"
	}

	if r.user == "" {
		u, err := user.Current()
		if err != nil {
			return nil, fmt.Errorf("could not retrieve current username: %w", err)
		}

		r.user = u.Username
	}

	if r.password == "" {
		r.password = os.Getenv("PGBK_SSH_PASS")
	}

	// Prepare authentication methods for SSH. The password is used for
	// regular password authentication when a private key is not explicitly
	// provided. Otherwise use the password as a passphrase to decrypt the
	// private key. In all cases, we try to get private keys from a running
	// ssh agent
	methods := make([]ssh.AuthMethod, 0)
	if r.identityFile == "" && r.password != "" {
		methods = append(methods, ssh.Password(r.password))
	}

	signers, err := pubKeyAuth(r.identityFile, r.password)
	if err != nil {
		return nil, err
	}

	methods = append(methods, ssh.PublicKeys(signers...))

	config := &ssh.ClientConfig{
		User:            r.user,
		Auth:            methods,
		HostKeyCallback: hostKeyCheck(r.disableHostCheck),
	}

	// Connect to the remote server and perform the SSH handshake.
	hostport := fmt.Sprintf("%s:%s", r.host, r.port)
	conn, err := ssh.Dial("tcp", hostport, config)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to %s: %w", hostport, err)
	}

	r.conn = conn

	// Open a sftp client over the SSH connection, it is safe to use it
	// concurrently, so we keep it in the repo struct
	client, err := sftp.NewClient(r.conn)
	if err != nil {
		return nil, fmt.Errorf("could not open sftp session: %w", err)
	}

	r.client = client

	return r, nil
}

func (r *sftpRepo) Close() error {
	r.client.Close()
	return r.conn.Close()
}

func (r *sftpRepo) Upload(path string, target string) error {
	l.Infof("uploading %s to %s:%s using sftp\n", path, r.host, r.baseDir)

	src, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("sftp: could not open source %s: %w", path, err)
	}
	defer src.Close()

	rpath := filepath.Join(r.baseDir, target)

	// Target directory must be created first
	targetDir := filepath.Dir(rpath)
	if targetDir != "." && targetDir != "/" {
		if err := r.client.MkdirAll(targetDir); err != nil {
			return fmt.Errorf("sftp: could not create parent directory of %s: %w", rpath, err)
		}
	}

	dst, err := r.client.Create(rpath)
	if err != nil {
		return fmt.Errorf("sftp: could not open destination %s: %w", rpath, err)
	}
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		return fmt.Errorf("sftp: could not open send data with sftp: %s", err)
	}

	return nil
}

func (r *sftpRepo) List(prefix string) (items []Item, rerr error) {
	items = make([]Item, 0)
	w := r.client.Walk(r.baseDir)
	for w.Step() {
		if err := w.Err(); err != nil {
			l.Warnln("could not list remote file:", err)
			rerr = err
			continue
		}

		path := relPath(r.baseDir, w.Path())
		if !strings.HasPrefix(path, prefix) {
			continue
		}

		finfo := w.Stat()
		items = append(items, Item{
			key:     path,
			modtime: finfo.ModTime(),
			isDir:   finfo.IsDir(),
		})
	}

	return
}

func (r *sftpRepo) Remove(path string) error {
	if err := r.client.Remove(filepath.Join(r.baseDir, path)); err != nil {
		return err
	}

	return nil
}
