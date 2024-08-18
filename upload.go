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
	"cloud.google.com/go/storage"
	"context"
	"errors"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"io"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"
	"github.com/Backblaze/blazer/b2"
)

// A Repo is a remote service where we can upload files
type Repo interface {
	// Upload a path to the remote naming it target
	Upload(path string, target string) error

	// Download target from the remote and store it into path
	Download(target string, path string) error

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

// Replace any backslashes from windows to forward slashed
func forwardSlashes(target string) string {
	return strings.ReplaceAll(target, fmt.Sprintf("%c", os.PathSeparator), "/")
}

func NewRepo(kind string, opts options) (Repo, error) {
	var (
		repo Repo
		err  error
	)

	switch kind {
	case "s3":
		repo, err = NewS3Repo(opts)
		if err != nil {
			return nil, fmt.Errorf("failed to prepare S3 repo: %w", err)
		}
	case "b2":
		repo, err = NewB2Repo(opts)
		if err != nil {
			return nil, fmt.Errorf("failed to prepare B2 repo: %w", err)
		}
	case "sftp":
		repo, err = NewSFTPRepo(opts)
		if err != nil {
			return nil, fmt.Errorf("failed to prepare sftp repo: %w", err)
		}
	case "gcs":
		repo, err = NewGCSRepo(opts)
		if err != nil {
			return nil, fmt.Errorf("failed to prepare CGS repo: %w", err)
		}
	case "azure":
		repo, err = NewAzRepo(opts)
		if err != nil {
			return nil, fmt.Errorf("failed to prepare Azure repo: %w", err)
		}
	}

	return repo, nil
}

type b2repo struct {
	appKey            string
	b2Bucket          *b2.Bucket
	b2Client          *b2.Client
	bucket            string
	concurrentUploads int
	ctx               context.Context
	endpoint          string
	forcePath         bool
	keyID             string
	region            string
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

func NewB2Repo(opts options) (*b2repo, error) {
	r := &b2repo{
		appKey:            opts.B2AppKey,
		bucket:            opts.B2Bucket,
		concurrentUploads: opts.B2ConcurrentUploads,
		ctx:               context.Background(),
		endpoint:          opts.B2Endpoint,
		forcePath:         opts.B2ForcePath,
		keyID:             opts.B2KeyID,
		region:            opts.B2Region,
	}
	
	l.Verbosef("starting b2 client with %d connections to %s %s \n", r.concurrentUploads, r.endpoint, r.bucket)
	client, err := b2.NewClient(r.ctx, r.keyID, r.appKey)

	if err != nil {
		return nil, fmt.Errorf("could not create B2 session: %w", err)
	}

	r.b2Client = client

	bucket, err := r.b2Client.Bucket(r.ctx, r.bucket)

	if err != nil {
		return nil, fmt.Errorf("could not connect to B2 bucket: %w", err)
	}

	r.b2Bucket = bucket

	return r, nil
}

func (r *b2repo) Upload(path string, target string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := r.b2Bucket.Object(target).NewWriter(r.ctx)
	w.ConcurrentUploads = r.concurrentUploads

	l.Infof("uploading %s to B2 bucket %s\n", path, r.bucket)
	if _, err := io.Copy(w, f); err != nil {
		w.Close()
		return err
	}

	return w.Close()
}

func (r *b2repo) Download(target string, path string) error {

	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("download error: %w", err)
	}
	defer file.Close()

	bucket := r.b2Bucket

	remoteFile := bucket.Object(path).NewReader(r.ctx)
	defer remoteFile.Close()

	localFile, err := os.Create(target)
	if err != nil {
		return err
	}

	if _, err := io.Copy(file, remoteFile); err != nil {
		localFile.Close()
		return err
	}
	return localFile.Close()
}

func (r *b2repo) Close() error {
	return nil
}

func (r *b2repo) List(prefix string) ([]Item, error) {

	files := make([]Item, 0)

	i := r.b2Bucket.List(r.ctx, b2.ListPrefix(prefix))
	for i.Next() {
		obj := i.Object()

		attributes, err := obj.Attrs(r.ctx)

		if err != nil {
			return nil, err
		}

		files = append(files, Item{
			key:     obj.Name(),
			modtime: attributes.LastModified,
		},
		)
	}

	return files, i.Err()
}

func (r *b2repo) Remove(path string) error {
	ctx, cancel := context.WithCancel(r.ctx)

	defer cancel()

	return r.b2Bucket.Object(path).Delete(ctx)
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
		Key:    aws.String(forwardSlashes(target)),
		Body:   file,
	})

	if err != nil {
		return fmt.Errorf("unable to upload %q to %q: %w", path, r.bucket, err)
	}

	return nil
}

func (r *s3repo) Download(target string, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("download error: %w", err)
	}
	defer file.Close()

	downloader := s3manager.NewDownloader(r.session)

	l.Infof("downloading %s from S3 bucket %s to %s\n", target, r.bucket, path)
	_, err = downloader.Download(file, &s3.GetObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(forwardSlashes(target)),
	})

	if err != nil {
		return fmt.Errorf("unable to download %q from %q: %w", target, r.bucket, err)
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
			Prefix:            aws.String(forwardSlashes(prefix)),
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
		Key:    aws.String(forwardSlashes(path)),
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
	targetDir := filepath.Dir(rpath)

	// sftp requires slash as path separator
	if os.PathSeparator != '/' {
		rpath = strings.ReplaceAll(rpath, string(os.PathSeparator), "/")
		targetDir = strings.ReplaceAll(targetDir, string(os.PathSeparator), "/")
	}
	l.Verboseln("sftp remote path is:", rpath)

	// Target directory must be created first
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
		return fmt.Errorf("sftp: could not send data with sftp: %s", err)
	}

	return nil
}

func (r *sftpRepo) Download(target string, path string) error {
	l.Infof("downloading %s from %s:%s using sftp\n", target, r.host, r.baseDir)

	dst, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("sftp: could not open or create %s: %w", path, err)
	}
	defer dst.Close()

	rpath := filepath.Join(r.baseDir, target)

	// sftp requires slash as path separator
	if os.PathSeparator != '/' {
		rpath = strings.ReplaceAll(rpath, string(os.PathSeparator), "/")
	}
	l.Verboseln("sftp remote path is:", rpath)

	src, err := r.client.Open(rpath)
	if err != nil {
		return fmt.Errorf("sftp: could not open %s on %s: %w", rpath, r.host, err)
	}
	defer src.Close()

	if _, err := io.Copy(dst, src); err != nil {
		return fmt.Errorf("sftp: could not receive data with sftp: %s", err)
	}

	return nil
}

func (r *sftpRepo) List(prefix string) (items []Item, rerr error) {
	items = make([]Item, 0)

	// sftp requires slash as path separator
	baseDir := r.baseDir
	if os.PathSeparator != '/' {
		baseDir = strings.ReplaceAll(baseDir, string(os.PathSeparator), "/")
	}

	w := r.client.Walk(baseDir)
	for w.Step() {
		if err := w.Err(); err != nil {
			l.Warnln("could not list remote file:", err)
			rerr = err
			continue
		}

		// relPath() makes use of functions of the filepath std module
		// that take care of putting back the proper os.PathSeparator
		// if it find some slashes, so we can compare paths without
		// worrying about path separators
		path := relPath(baseDir, w.Path())

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
	rpath := filepath.Join(r.baseDir, path)

	// sftp requires slash as path separator
	if os.PathSeparator != '/' {
		rpath = strings.ReplaceAll(rpath, string(os.PathSeparator), "/")
	}

	if err := r.client.Remove(rpath); err != nil {
		return err
	}

	return nil
}

type gcsRepo struct {
	bucket  string
	url     string // Endpoint URL
	keyFile string
	client  *storage.Client
}

func NewGCSRepo(opts options) (*gcsRepo, error) {
	r := &gcsRepo{
		bucket:  opts.GCSBucket,
		url:     opts.GCSEndPoint,
		keyFile: opts.GCSCredentialsFile,
	}

	options := make([]option.ClientOption, 0)
	if r.url != "" {
		options = append(options, option.WithEndpoint(r.url))
	}

	if r.keyFile != "" {
		options = append(options, option.WithCredentialsFile(r.keyFile))
	}

	client, err := storage.NewClient(context.Background(), options...)
	if err != nil {
		return nil, fmt.Errorf("could not create GCS client: %w", err)
	}

	r.client = client

	return r, nil
}

func (r *gcsRepo) Close() error {
	return r.client.Close()
}

func (r *gcsRepo) Upload(path string, target string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("upload error: %w", err)
	}
	defer file.Close()

	obj := r.client.Bucket(r.bucket).Object(forwardSlashes(target)).NewWriter(context.Background())
	defer obj.Close()

	l.Infof("uploading %s to GCS bucket %s\n", path, r.bucket)
	if _, err := io.Copy(obj, file); err != nil {
		return fmt.Errorf("could not write data to GCS object: %w", err)
	}

	// The upload is done asynchronously, the error returned by Close()
	// says if it was successful
	return obj.Close()
}

func (r *gcsRepo) Download(target string, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("download error: %w", err)
	}
	defer file.Close()

	obj, err := r.client.Bucket(r.bucket).Object(forwardSlashes(target)).NewReader(context.Background())
	if err != nil {
		return fmt.Errorf("download error: %w", err)
	}
	defer obj.Close()

	l.Infof("downloading %s from GCS bucket %s to %s\n", target, r.bucket, path)
	if _, err := io.Copy(file, obj); err != nil {
		return fmt.Errorf("could not read data from GCS object: %w", err)
	}

	return obj.Close()
}

func (r *gcsRepo) List(prefix string) (items []Item, rerr error) {
	items = make([]Item, 0)

	it := r.client.Bucket(r.bucket).Objects(context.Background(), &storage.Query{Prefix: forwardSlashes(prefix)})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			l.Warnln("could not list remote file:", err)
			rerr = err
			break
		}

		items = append(items, Item{
			key:     attrs.Name,
			modtime: attrs.Updated,
		})
	}

	return
}

func (r *gcsRepo) Remove(path string) error {
	if err := r.client.Bucket(r.bucket).Object(forwardSlashes(path)).Delete(context.Background()); err != nil {
		return fmt.Errorf("could not remove %s from GCS bucket %s: %w", path, r.bucket, err)
	}

	return nil
}

type azRepo struct {
	container string
	account   string
	key       string
	endpoint  string
	client    *azblob.Client
}

func NewAzRepo(opts options) (*azRepo, error) {
	r := &azRepo{
		container: opts.AzureContainer,
		account:   opts.AzureAccount,
		key:       opts.AzureKey,
		endpoint:  opts.AzureEndpoint,
	}

	var (
		client *azblob.Client
		err    error
	)

	if r.account == "" {
		r.account = os.Getenv("AZURE_STORAGE_ACCOUNT")
	}

	if r.key == "" {
		r.key = os.Getenv("AZURE_STORAGE_KEY")
	}

	if r.account == "" {
		client, err = azblob.NewClientWithNoCredential(r.endpoint, nil)
		if err != nil {
			return nil, fmt.Errorf("could not create anonymous Azure client: %w", err)
		}
	} else {
		credential, err := azblob.NewSharedKeyCredential(r.account, r.key)
		if err != nil {
			return nil, fmt.Errorf("could not setup Azure credentials: %w", err)
		}

		url := fmt.Sprintf("https://%s.%s", r.account, r.endpoint)

		client, err = azblob.NewClientWithSharedKeyCredential(url, credential, nil)
		if err != nil {
			return nil, fmt.Errorf("could not create Azure client: %w", err)
		}
	}

	r.client = client

	return r, nil
}

func (r *azRepo) Upload(path string, target string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("upload error: %w", err)
	}
	defer file.Close()

	l.Infof("uploading %s to Azure container %s\n", path, r.container)
	_, err = r.client.UploadFile(context.Background(), r.container, path, file, nil)
	if err != nil {
		return fmt.Errorf("could not upload %s to Azure: %w", path, err)
	}

	return nil
}

func (r *azRepo) Download(target string, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("download error: %w", err)
	}
	defer file.Close()

	l.Infof("downloading %s from Azure container %s\n", target, r.container)
	_, err = r.client.DownloadFile(context.Background(), r.container, target, file, nil)
	if err != nil {
		return fmt.Errorf("could not download %s from Azure: %w", target, err)
	}

	return nil
}

func (r *azRepo) List(prefix string) ([]Item, error) {
	p := forwardSlashes(prefix)
	pager := r.client.NewListBlobsFlatPager(r.container, &azblob.ListBlobsFlatOptions{
		Prefix: &p,
	})

	files := make([]Item, 0)
	for pager.More() {
		resp, err := pager.NextPage(context.Background())
		if err != nil {
			return nil, fmt.Errorf("could not fully list Azure container %s: %w", r.container, err)
		}

		for _, v := range resp.Segment.BlobItems {
			file := Item{
				key:     *v.Name,
				modtime: *v.Properties.LastModified,
			}

			files = append(files, file)
		}
	}

	return files, nil
}

func (r *azRepo) Remove(path string) error {

	if _, err := r.client.DeleteBlob(context.Background(), r.container, forwardSlashes(path), nil); err != nil {
		return fmt.Errorf("could not remove blob from Azure container %s: %w", r.container, err)
	}

	return nil
}

func (r *azRepo) Close() error {
	return nil
}
