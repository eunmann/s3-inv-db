package s3fetch

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// fakeGets returns bodies that count how many are open at once.
type fakeGets struct {
	transfermanager.S3APIClient

	mu         sync.Mutex
	open, peak int
	fail       bool
}

var errFakeGet = errors.New("fake GetObject failure")

func (f *fakeGets) GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if f.fail {
		return nil, errFakeGet
	}
	f.mu.Lock()
	f.open++
	f.peak = max(f.peak, f.open)
	f.mu.Unlock()

	return &s3.GetObjectOutput{Body: &countedBody{Reader: strings.NewReader("x"), gets: f}}, nil
}

type countedBody struct {
	io.Reader

	gets *fakeGets
}

func (c *countedBody) Close() error {
	c.gets.mu.Lock()
	c.gets.open--
	c.gets.mu.Unlock()

	return nil
}

func TestBoundedGets_CapsOpenBodies(t *testing.T) {
	fake := &fakeGets{}
	b := newBoundedGets(fake, 3)
	var wg sync.WaitGroup
	for range 40 {
		wg.Go(func() {
			out, err := b.GetObject(t.Context(), &s3.GetObjectInput{})
			if err != nil {
				t.Error(err)

				return
			}
			time.Sleep(time.Millisecond)
			_, _ = io.Copy(io.Discard, out.Body)
			_ = out.Body.Close()
			_ = out.Body.Close() // a second Close must not free a second slot
		})
	}
	wg.Wait()
	if p := fake.peak; p > 3 || p < 1 {
		t.Fatalf("peak open bodies = %d, want 1..3", p)
	}
	if n := len(b.slots); n != 0 {
		t.Fatalf("%d slots still held after every body closed", n)
	}
}

func TestBoundedGets_FailedGetFreesItsSlot(t *testing.T) {
	b := newBoundedGets(&fakeGets{fail: true}, 1)
	for range 3 {
		if _, err := b.GetObject(t.Context(), &s3.GetObjectInput{}); !errors.Is(err, errFakeGet) {
			t.Fatalf("err = %v, want errFakeGet", err)
		}
	}
	if n := len(b.slots); n != 0 {
		t.Fatalf("%d slots held after failed GETs", n)
	}
}

func TestBoundedGets_WaitHonoursContext(t *testing.T) {
	b := newBoundedGets(&fakeGets{}, 1)
	held, err := b.GetObject(t.Context(), &s3.GetObjectInput{})
	if err != nil {
		t.Fatal(err)
	}
	defer held.Body.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err := b.GetObject(ctx, &s3.GetObjectInput{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded while every slot is held", err)
	}
}
