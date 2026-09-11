package saga

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	taskq "github.com/Nuvraxis/taskQ"
	"github.com/Nuvraxis/taskQ/membroker"
)

func TestHopKey(t *testing.T) {
	tests := []struct {
		name string
		hop  Hop
		want string
	}{
		{"forward", Hop{"s1", 0, Forward}, "s1:0:" + Forward.String()},
		{"compensating", Hop{"s1", 2, Compensating}, "s1:2:" + Compensating.String()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.hop.Key(); got != tc.want {
				t.Fatalf("Key() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestHopFromContext_Absent(t *testing.T) {
	if _, ok := HopFromContext(context.Background()); ok {
		t.Fatal("expected ok=false on a bare context")
	}
}

func TestHopFromContext_EndToEnd(t *testing.T) {
	broker := membroker.New()
	defer broker.Close()

	var (
		mu   sync.Mutex
		seen []Hop
	)
	record := func(ctx context.Context) {
		h, ok := HopFromContext(ctx)
		if !ok {
			t.Error("step ran without a Hop in ctx")
			return
		}
		mu.Lock()
		seen = append(seen, h)
		mu.Unlock()
	}

	type state struct{}
	steps := []Step[state]{
		{
			Name:       "a",
			Do:         func(ctx context.Context, _ *state) error { record(ctx); return nil },
			Compensate: func(ctx context.Context, _ *state) error { record(ctx); return nil },
		},
		{
			Name: "b",
			Do:   func(ctx context.Context, _ *state) error { record(ctx); return errors.New("boom") },
		},
	}

	done := make(chan struct{})
	sg := New(broker, "hops", steps,
		WithMaxRetry[state](0),
		WithOnFailed(func(context.Context, string, state, string, error) { close(done) }),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool := taskq.NewPool(broker, "hops", sg.Handler())
	go func() { _ = pool.Run(ctx) }()

	id, err := sg.Start(ctx, state{})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("saga did not finish in time")
	}

	want := []Hop{
		{id, 0, Forward},
		{id, 1, Forward},
		{id, 0, Compensating},
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != len(want) {
		t.Fatalf("saw %d hops, want %d: %+v", len(seen), len(want), seen)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("hop %d = %+v, want %+v", i, seen[i], want[i])
		}
	}
}
