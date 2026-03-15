package s3

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/htb/htb-replicator/internal/source"
)

// ListAll retrieves all objects from the source bucket using paginated
// ListObjectsV2 calls. An optional prefix filters the listing.
func (c *s3Client) ListAll(ctx context.Context) ([]source.SourceObject, error) {
	var objects []source.SourceObject

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
	}
	if c.prefix != "" {
		input.Prefix = aws.String(c.prefix)
	}

	paginator := s3.NewListObjectsV2Paginator(c.client, input)

	for paginator.HasMorePages() {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("context cancelled during list: %w", err)
		}

		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list objects page: %w", err)
		}

		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			// Skip S3 directory markers — zero-byte objects whose key ends
			// with "/" that the S3 console creates to represent "folders".
			// They have no file content and must not be written to destinations.
			if strings.HasSuffix(*obj.Key, "/") {
				continue
			}
			so := source.SourceObject{Key: *obj.Key}
			if obj.ETag != nil {
				so.ETag = *obj.ETag
			}
			if obj.Size != nil {
				so.Size = *obj.Size
			}
			if obj.LastModified != nil {
				so.LastModified = *obj.LastModified
			}
			objects = append(objects, so)
		}
	}

	return objects, nil
}
