package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/htb/htb-replicator/config"
	"github.com/htb/htb-replicator/internal/destinations"
)

const multipartThreshold = 100 * 1024 * 1024 // 100 MiB
const multipartChunkSize = 10 * 1024 * 1024  // 10 MiB per part

// s3Dest writes replicated objects to an S3-compatible bucket.
type s3Dest struct {
	id     string
	client *s3.Client
	bucket string
}

// New creates an S3 destination from the given DestinationConfig.
// Required opts: bucket. Optional: endpoint, region, access_key_id,
// secret_access_key, path_style.
func New(cfg config.DestinationConfig) (destinations.Destination, error) {
	bucketVal, ok := cfg.Opts["bucket"]
	if !ok {
		return nil, fmt.Errorf("s3 destination %q: opts.bucket is required", cfg.ID)
	}
	bucket, ok := bucketVal.(string)
	if !ok || bucket == "" {
		return nil, fmt.Errorf("s3 destination %q: opts.bucket must be a non-empty string", cfg.ID)
	}

	region, _ := cfg.Opts["region"].(string)
	if region == "" {
		region = "us-east-1"
	}
	endpoint, _ := cfg.Opts["endpoint"].(string)
	accessKey, _ := cfg.Opts["access_key_id"].(string)
	secretKey, _ := cfg.Opts["secret_access_key"].(string)
	pathStyle, _ := cfg.Opts["path_style"].(bool)

	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(region),
	}

	if endpoint != "" {
		opts = append(opts,
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
		opts = append(opts,
			awsconfig.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
			),
		)
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("s3 destination %q: load aws config: %w", cfg.ID, err)
	}

	s3Opts := []func(*s3.Options){}
	if pathStyle {
		s3Opts = append(s3Opts, func(o *s3.Options) { o.UsePathStyle = true })
	}

	return &s3Dest{
		id:     cfg.ID,
		client: s3.NewFromConfig(awsCfg, s3Opts...),
		bucket: bucket,
	}, nil
}

func (d *s3Dest) ID() string   { return d.id }
func (d *s3Dest) Type() string { return "s3" }

// Write uploads an object to S3. For objects > 100 MiB it uses multipart upload;
// otherwise it uses PutObject.
func (d *s3Dest) Write(ctx context.Context, obj destinations.Object, r io.Reader) (destinations.WriteResult, error) {
	if obj.Size > multipartThreshold {
		return d.multipartUpload(ctx, obj, r)
	}
	return d.putObject(ctx, obj, r)
}

func (d *s3Dest) putObject(ctx context.Context, obj destinations.Object, r io.Reader) (destinations.WriteResult, error) {
	input := &s3.PutObjectInput{
		Bucket: aws.String(d.bucket),
		Key:    aws.String(obj.Key),
		Body:   r,
	}
	if obj.ContentType != "" {
		input.ContentType = aws.String(obj.ContentType)
	}
	if obj.Size > 0 {
		input.ContentLength = aws.Int64(obj.Size)
	}

	out, err := d.client.PutObject(ctx, input)
	if err != nil {
		return destinations.WriteResult{}, fmt.Errorf("put object %q: %w", obj.Key, err)
	}

	var etag string
	if out.ETag != nil {
		etag = strings.Trim(*out.ETag, `"`)
	}
	return destinations.WriteResult{ETag: etag, BytesWritten: obj.Size}, nil
}

func (d *s3Dest) multipartUpload(ctx context.Context, obj destinations.Object, r io.Reader) (destinations.WriteResult, error) {
	createOut, err := d.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(d.bucket),
		Key:    aws.String(obj.Key),
	})
	if err != nil {
		return destinations.WriteResult{}, fmt.Errorf("create multipart upload %q: %w", obj.Key, err)
	}
	uploadID := *createOut.UploadId

	var completedParts []types.CompletedPart
	var totalBytes int64
	buf := make([]byte, multipartChunkSize)
	partNum := int32(1)

	abortUpload := func() {
		_, _ = d.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(d.bucket),
			Key:      aws.String(obj.Key),
			UploadId: aws.String(uploadID),
		})
	}

	for {
		n, readErr := io.ReadFull(r, buf)
		if n > 0 {
			part, err := d.client.UploadPart(ctx, &s3.UploadPartInput{
				Bucket:     aws.String(d.bucket),
				Key:        aws.String(obj.Key),
				UploadId:   aws.String(uploadID),
				PartNumber: aws.Int32(partNum),
				Body:       bytes.NewReader(buf[:n]),
			})
			if err != nil {
				abortUpload()
				return destinations.WriteResult{}, fmt.Errorf("upload part %d for %q: %w", partNum, obj.Key, err)
			}
			completedParts = append(completedParts, types.CompletedPart{
				ETag:       part.ETag,
				PartNumber: aws.Int32(partNum),
			})
			totalBytes += int64(n)
			partNum++
		}
		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			break
		}
		if readErr != nil {
			abortUpload()
			return destinations.WriteResult{}, fmt.Errorf("read for multipart %q: %w", obj.Key, readErr)
		}
	}

	completeOut, err := d.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(d.bucket),
		Key:      aws.String(obj.Key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		abortUpload()
		return destinations.WriteResult{}, fmt.Errorf("complete multipart upload %q: %w", obj.Key, err)
	}

	var etag string
	if completeOut.ETag != nil {
		etag = strings.Trim(*completeOut.ETag, `"`)
	}
	return destinations.WriteResult{ETag: etag, BytesWritten: totalBytes}, nil
}

// Exists checks if the key exists in the S3 bucket.
func (d *s3Dest) Exists(ctx context.Context, key string) (string, bool, error) {
	out, err := d.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(d.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		// Check for 404-like not found errors.
		errStr := err.Error()
		if strings.Contains(errStr, "NoSuchKey") ||
			strings.Contains(errStr, "NotFound") ||
			strings.Contains(errStr, "404") {
			return "", false, nil
		}
		return "", false, fmt.Errorf("head object %q: %w", key, err)
	}

	var etag string
	if out.ETag != nil {
		etag = strings.Trim(*out.ETag, `"`)
	}
	return etag, true, nil
}

// Delete removes the object from S3.
func (d *s3Dest) Delete(ctx context.Context, key string) error {
	_, err := d.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(d.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("delete object %q: %w", key, err)
	}
	return nil
}

// ListKeys returns all object keys in the destination bucket.
func (d *s3Dest) ListKeys(ctx context.Context) ([]string, error) {
	var keys []string
	paginator := s3.NewListObjectsV2Paginator(d.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(d.bucket),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list objects: %w", err)
		}
		for _, obj := range page.Contents {
			if obj.Key != nil {
				keys = append(keys, *obj.Key)
			}
		}
	}
	return keys, nil
}

// Ping verifies connectivity by listing the bucket with max-keys=1.
func (d *s3Dest) Ping(ctx context.Context) error {
	_, err := d.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(d.bucket),
		MaxKeys: aws.Int32(1),
	})
	if err != nil {
		return fmt.Errorf("s3 ping %q: %w", d.bucket, err)
	}
	return nil
}

// Close is a no-op; the S3 client has no persistent connections to close.
func (d *s3Dest) Close() error { return nil }
