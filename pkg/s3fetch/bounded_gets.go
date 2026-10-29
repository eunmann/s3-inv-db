package s3fetch

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// boundedGets caps the GetObject calls in flight across every download
// that shares it. The transfer manager bounds parts per object only, so
// N concurrent downloads would otherwise hold N × Concurrency
// connections. A slot is held from the request until the body closes.
type boundedGets struct {
	transfermanager.S3APIClient

	slots chan struct{}
}

func newBoundedGets(client transfermanager.S3APIClient, maxInFlight int) *boundedGets {
	return &boundedGets{S3APIClient: client, slots: make(chan struct{}, maxInFlight)}
}

func (b *boundedGets) GetObject(ctx context.Context, in *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	select {
	case b.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, fmt.Errorf("wait for a GET slot: %w", ctx.Err())
	}
	out, err := b.S3APIClient.GetObject(ctx, in, optFns...)
	if err != nil {
		<-b.slots

		return nil, err //nolint:wrapcheck // the transfer manager wraps and classifies SDK errors itself
	}
	out.Body = &slotBody{ReadCloser: out.Body, release: func() { <-b.slots }}

	return out, nil
}

// tryAcquire takes a slot if one is free now, for a GET that is worth
// making only without waiting: a stream's read-ahead.
func (b *boundedGets) tryAcquire() bool {
	select {
	case b.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

// release frees a slot tryAcquire took.
func (b *boundedGets) release() { <-b.slots }

// slotBody releases its GET slot on the first Close.
type slotBody struct {
	io.ReadCloser

	release func()
	once    sync.Once
}

func (s *slotBody) Close() error {
	err := s.ReadCloser.Close()
	s.once.Do(s.release)

	return err //nolint:wrapcheck // passes the body's own close error through
}
