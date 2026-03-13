// Package s3 provides an S3-compatible source implementation.
package s3

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/htb/htb-replicator/internal/source"
)

// s3Client implements source.Source backed by AWS SDK v2.
type s3Client struct {
	client *s3.Client
	bucket string
	prefix string
	region string
	endpoint string
}

// New creates a new S3 source client from opts.
// Required: bucket. Either region or endpoint must be set.
// Optional: access_key_id, secret_access_key, path_style, prefix.
func New(opts map[string]interface{}) (source.Source, error) {
	getString := func(key string) string {
		v, _ := opts[key]
		s, _ := v.(string)
		return s
	}
	getBool := func(key string) bool {
		v, _ := opts[key]
		b, _ := v.(bool)
		return b
	}

	bucket := getString("bucket")
	if bucket == "" {
		return nil, fmt.Errorf("s3 source: opts.bucket is required")
	}

	region := getString("region")
	endpoint := getString("endpoint")
	if region == "" && endpoint == "" {
		return nil, fmt.Errorf("s3 source: opts.region or opts.endpoint is required")
	}
	if region == "" {
		region = "us-east-1" // fallback for endpoint-only (e.g. MinIO)
	}

	accessKey := getString("access_key_id")
	secretKey := getString("secret_access_key")
	prefix := getString("prefix")
	pathStyle := getBool("path_style")

	awsOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(region),
	}

	if endpoint != "" {
		awsOpts = append(awsOpts,
			awsconfig.WithEndpointResolverWithOptions(
				aws.EndpointResolverWithOptionsFunc(
					func(service, reg string, options ...interface{}) (aws.Endpoint, error) {
						return aws.Endpoint{
							URL:               endpoint,
							SigningRegion:     region,
							HostnameImmutable: true,
						}, nil
					},
				),
			),
		)
	}

	if accessKey != "" && secretKey != "" {
		awsOpts = append(awsOpts,
			awsconfig.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
			),
		)
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), awsOpts...)
	if err != nil {
		return nil, fmt.Errorf("s3 source: load aws config: %w", err)
	}

	s3opts := []func(*s3.Options){}
	if pathStyle {
		s3opts = append(s3opts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	return &s3Client{
		client:   s3.NewFromConfig(awsCfg, s3opts...),
		bucket:   bucket,
		prefix:   prefix,
		region:   region,
		endpoint: endpoint,
	}, nil
}

// GetObject downloads a single object and returns its body stream and content length.
// The caller is responsible for closing the returned ReadCloser.
func (c *s3Client) GetObject(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	out, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, 0, fmt.Errorf("s3 get object %q: %w", key, err)
	}

	var size int64
	if out.ContentLength != nil {
		size = *out.ContentLength
	}
	return out.Body, size, nil
}

// HeadObject fetches metadata for a single object without downloading its body.
func (c *s3Client) HeadObject(ctx context.Context, key string) (source.SourceObject, error) {
	out, err := c.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return source.SourceObject{}, fmt.Errorf("s3 head object %q: %w", key, err)
	}

	obj := source.SourceObject{Key: key}
	if out.ETag != nil {
		obj.ETag = *out.ETag
	}
	if out.ContentLength != nil {
		obj.Size = *out.ContentLength
	}
	if out.LastModified != nil {
		obj.LastModified = *out.LastModified
	}
	return obj, nil
}

// DeleteObject permanently removes an object from the source bucket.
func (c *s3Client) DeleteObject(ctx context.Context, key string) error {
	_, err := c.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("s3 delete object %q: %w", key, err)
	}
	return nil
}

// Ping performs a lightweight connectivity check using a single ListObjectsV2 call.
func (c *s3Client) Ping(ctx context.Context) error {
	maxKeys := int32(1)
	_, err := c.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(c.bucket),
		MaxKeys: &maxKeys,
	})
	if err != nil {
		return fmt.Errorf("s3 ping bucket %q: %w", c.bucket, err)
	}
	return nil
}

// Close is a no-op for the S3 source (HTTP client pool is managed internally).
func (c *s3Client) Close() error { return nil }

// Endpoint returns the configured endpoint URL (or empty for native AWS).
func (c *s3Client) Endpoint() string { return c.endpoint }

// Region returns the configured AWS region.
func (c *s3Client) Region() string { return c.region }

// Bucket returns the configured bucket name.
func (c *s3Client) Bucket() string { return c.bucket }
