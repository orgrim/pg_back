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
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"os"
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

type Item struct {
	key     string
	modtime time.Time
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
