// Copyright 2021-present PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package s3dfs

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"github.com/ngaut/unistore/scheduler"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	s3config "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ngaut/unistore/config"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
)

const (
	DeletionFileName = "DELETION"
	deletionSize     = int(unsafe.Sizeof(deletion{}))
)

type S3Client struct {
	config.S3Options
	*scheduler.Scheduler
	instanceID        uint32
	cli               *s3.Client
	expirationSeconds uint64
	lock              sync.RWMutex
	deletions         []deletion
	lastDeleteTime    uint64
	simulateLatency   time.Duration
}

func NewS3Client(instanceID uint32, opts config.S3Options) (*S3Client, error) {
	if opts.Concurrency <= 0 {
		opts.Concurrency = 256
	}
	s3c := &S3Client{
		S3Options:  opts,
		Scheduler:  scheduler.NewScheduler(opts.Concurrency),
		instanceID: instanceID,
	}
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   256,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	client := &http.Client{Transport: tr}
	cred := credentials.NewStaticCredentialsProvider(opts.KeyID, opts.SecretKey, "")
	cfg, err := s3config.LoadDefaultConfig(
		context.TODO(),
		s3config.WithCredentialsProvider(cred),
		s3config.WithHTTPClient(client),
		s3config.WithRegion(opts.Region),
	)
	if err != nil {
		log.S().Errorf("load config error: %s", err.Error())
		return nil, err
	}
	if len(opts.Endpoint) > 0 {
		resolver := aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL: "http://" + opts.Endpoint,
			}, nil
		})
		cfg.EndpointResolver = resolver
	}
	s3c.cli = s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})
	if len(opts.SimulateLatency) > 0 {
		s3c.simulateLatency = config.ParseDuration(opts.SimulateLatency)
	}
	if len(opts.ExpirationDuration) > 0 {
		s3c.expirationSeconds = uint64(config.ParseDuration(opts.ExpirationDuration) / time.Second)
	}
	return s3c, nil
}

func (c *S3Client) Get(key string, offset, length int64) ([]byte, error) {
	input := &s3.GetObjectInput{}
	input.Bucket = &c.Bucket
	input.Key = &key
	if length > 0 {
		input.Range = aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	}
	out, err := c.cli.GetObject(context.TODO(), input)
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	var result []byte
	if length > 0 {
		result = make([]byte, length)
	} else {
		result = make([]byte, out.ContentLength)
	}
	_, err = io.ReadFull(out.Body, result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *S3Client) GetToFile(key string, filePath string) error {
	fid, ok := c.ParseFileID(key)
	if !ok {
		return errors.New("fail to parse file id:" + key)
	}
	log.S().Infof("get file from s3:%d", fid)
	if c.simulateLatency > 0 {
		time.Sleep(c.simulateLatency)
	}
	fd, err := y.OpenTruncFile(filePath, false)
	if err != nil {
		return err
	}
	defer fd.Close()
	input := &s3.GetObjectInput{}
	input.Bucket = &c.Bucket
	input.Key = &key
	out, err := c.cli.GetObject(context.TODO(), input)
	if err != nil {
		return err
	}
	defer out.Body.Close()
	_, err = io.Copy(fd, out.Body)
	return err
}

func (c *S3Client) Put(key string, data []byte) error {
	fid, ok := c.ParseFileID(key)
	if !ok {
		return errors.New("fail to parse file id:" + key)
	}
	log.S().Infof("put file to s3:%d", fid)
	if c.simulateLatency > 0 {
		time.Sleep(c.simulateLatency)
	}
	input := &s3.PutObjectInput{}
	input.ContentLength = int64(len(data))
	input.Bucket = &c.Bucket
	input.Key = &key
	input.Body = bytes.NewReader(data)
	_, err := c.cli.PutObject(context.TODO(), input, s3.WithAPIOptions(
		v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware,
	))
	return err
}

func (c *S3Client) Delete(key string) error {
	fid, ok := c.ParseFileID(key)
	if !ok {
		return errors.New("fail to parse file id:" + key)
	}
	log.S().Infof("delete file from s3:%d", fid)
	input := &s3.DeleteObjectInput{}
	input.Bucket = &c.Bucket
	input.Key = &key
	_, err := c.cli.DeleteObject(context.TODO(), input)
	return err
}

func (c *S3Client) ListFiles() (map[uint64]struct{}, error) {
	fileIDs := map[uint64]struct{}{}
	input := &s3.ListObjectsV2Input{}
	input.Bucket = &c.Bucket
	input.Prefix = aws.String(fmt.Sprintf("bg%08x", c.instanceID))
	p := s3.NewListObjectsV2Paginator(c.cli, input, func(o *s3.ListObjectsV2PaginatorOptions) {
		o.Limit = 1000
	})
	for p.HasMorePages() {
		page, err := p.NextPage(context.TODO())
		if err != nil {
			return nil, err
		}
		for _, objInfo := range page.Contents {
			var fid uint64
			_, err = fmt.Sscanf((*objInfo.Key)[10:26], "%016x", &fid)
			if err != nil {
				return nil, err
			}
			fileIDs[fid] = struct{}{}
		}
	}
	return fileIDs, nil
}

func (c *S3Client) BlockKey(fid uint64) string {
	return fmt.Sprintf("bg%08x%016x.sst", c.instanceID, fid)
}

func (c *S3Client) ParseFileID(key string) (uint64, bool) {
	if len(key) != 30 {
		return 0, false
	}
	if !strings.HasPrefix(key, "bg") {
		return 0, false
	}
	if !strings.HasSuffix(key, ".sst") {
		return 0, false
	}
	var fid uint64
	_, err := fmt.Sscanf(key[10:26], "%016x", &fid)
	if err != nil {
		return 0, false
	}
	return fid, true
}

func (c *S3Client) SetExpired(fid uint64) {
	if c.expirationSeconds > 0 {
		var toDelete []deletion
		var now = uint64(time.Now().Unix())
		c.lock.Lock()
		if now-c.lastDeleteTime > 10 {
			for i, d := range c.deletions {
				if d.expiredTime > now {
					toDelete = c.deletions[:i]
					break
				} else if i == len(c.deletions)-1 {
					toDelete = c.deletions
				}
			}
			c.deletions = c.deletions[len(toDelete):]
			c.lastDeleteTime = now
		}
		c.deletions = append(c.deletions, deletion{fid: fid, expiredTime: now + c.expirationSeconds})
		c.lock.Unlock()
		if len(toDelete) > 0 {
			go func() {
				for _, d := range toDelete {
					if err := c.Delete(c.BlockKey(d.fid)); err != nil {
						log.S().Errorf("cannot delete expired file: %s", err.Error())
					}
				}
			}()
		}
	}
}

type deletion struct {
	fid         uint64
	expiredTime uint64
}