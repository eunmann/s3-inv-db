package events_test

import (
	"sync"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/extsort/events"
)

func TestBus_Publish_NoSubscribers_NoOp(t *testing.T) {
	bus := events.NewBus()
	defer bus.Close()
	// Should not panic, not block, not allocate.
	bus.Publish(events.Event{Stage: events.StageAggregator, Type: events.EvtBatchCommitted})
	if bus.HasSubscribers() {
		t.Fatal("HasSubscribers() should be false with no subscribers")
	}
}

func TestBus_Subscribe_ReceivesMatchingStage(t *testing.T) {
	bus := events.NewBus()
	defer bus.Close()
	sub := bus.Subscribe(8, events.StageAggregator)
	defer sub.Cancel()

	bus.Publish(events.Event{Stage: events.StageAggregator, Type: events.EvtBatchCommitted, Payload: 42})
	bus.Publish(events.Event{Stage: events.StageMerge, Type: events.EvtRoundStarted}) // filtered out

	select {
	case ev := <-sub.C:
		if ev.Stage != events.StageAggregator {
			t.Errorf("got stage %q, want aggregator", ev.Stage)
		}
		if ev.Payload != 42 {
			t.Errorf("payload = %v, want 42", ev.Payload)
		}
		if ev.Time.IsZero() {
			t.Error("Time not auto-stamped")
		}
	case <-time.After(time.Second):
		t.Fatal("did not receive aggregator event")
	}

	// Second event was for a different stage; channel should be empty.
	select {
	case ev := <-sub.C:
		t.Errorf("unexpected event for stage %q", ev.Stage)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestBus_Subscribe_NoStages_ReceivesAll(t *testing.T) {
	bus := events.NewBus()
	defer bus.Close()
	sub := bus.Subscribe(8)
	defer sub.Cancel()

	bus.Publish(events.Event{Stage: events.StageAggregator, Type: events.EvtBatchCommitted})
	bus.Publish(events.Event{Stage: events.StageMerge, Type: events.EvtRoundStarted})

	got := make(map[events.Stage]bool)
	for i := range 2 {
		select {
		case ev := <-sub.C:
			got[ev.Stage] = true
		case <-time.After(time.Second):
			t.Fatalf("only received %d/2 events", i)
		}
	}
	if !got[events.StageAggregator] || !got[events.StageMerge] {
		t.Errorf("expected both stages, got %v", got)
	}
}

func TestBus_Publish_SlowSubscriber_DropsEvents_NoBlock(t *testing.T) {
	bus := events.NewBus()
	defer bus.Close()
	sub := bus.Subscribe(2) // tiny buffer
	// Do not drain — let it fill.

	const n = 100
	done := make(chan struct{})
	go func() {
		for range n {
			bus.Publish(events.Event{Stage: events.StageAggregator, Type: events.EvtBatchCommitted})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Publish blocked on a full slow subscriber")
	}
	if got := sub.Dropped(); got < n-2 {
		t.Errorf("Dropped() = %d, want at least %d (buffer=2)", got, n-2)
	}
}

func TestBus_Cancel_ClosesChannel(t *testing.T) {
	bus := events.NewBus()
	defer bus.Close()
	sub := bus.Subscribe(4)
	sub.Cancel()
	// After Cancel the channel should be closed; ranging should
	// terminate promptly.
	done := make(chan struct{})
	go func() {
		for range sub.C { //nolint:revive // explicit drain
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("channel did not close after Cancel")
	}
}

func TestBus_Close_DisablesPublishAndClosesSubscribers(t *testing.T) {
	bus := events.NewBus()
	sub := bus.Subscribe(4)
	bus.Close()

	done := make(chan struct{})
	go func() {
		for range sub.C { //nolint:revive // explicit drain until close
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("subscriber channel not closed by Bus.Close")
	}

	// Subsequent Publish must not panic.
	bus.Publish(events.Event{Stage: events.StagePipeline, Type: events.EvtStageStart})
}

func TestBus_ConcurrentPublishersAndSubscribers(t *testing.T) {
	bus := events.NewBus()
	defer bus.Close()

	const (
		publishers     = 8
		subscribers    = 4
		eventsPerPub   = 1000
		expectedPerSub = publishers * eventsPerPub
	)

	subs := make([]*events.Subscription, subscribers)
	for i := range subs {
		subs[i] = bus.Subscribe(4096)
	}

	var wg sync.WaitGroup
	for range publishers {
		wg.Go(func() {
			for range eventsPerPub {
				bus.Publish(events.Event{
					Stage: events.StageAggregator,
					Type:  events.EvtBatchCommitted,
				})
			}
		})
	}
	wg.Wait()

	// Each subscriber should have received some events (might have
	// dropped some due to small buffer). The key invariant: no
	// publisher blocked and no panics.
	for i, sub := range subs {
		recv := 0
		drain := time.NewTimer(200 * time.Millisecond)
	loop:
		for {
			select {
			case <-sub.C:
				recv++
			case <-drain.C:
				break loop
			}
		}
		if uint64(recv)+sub.Dropped() < expectedPerSub-100 {
			t.Errorf("sub %d: recv=%d dropped=%d, expected sum ~%d", i, recv, sub.Dropped(), expectedPerSub)
		}
	}
}
