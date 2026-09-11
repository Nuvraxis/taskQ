// Command compensate demonstrates what happens when a saga step fails
// permanently: earlier steps that already succeeded are rolled back, in
// reverse order, by their own Compensate — automatically, with no manual
// bookkeeping. It reuses the same order workflow as examples/saga/basic,
// except the final "ship-order" step always fails (simulating, say, a
// carrier outage), so watch the log for reserve/charge running forward,
// then unwinding in reverse.
//
// Run it with:
//
//	go run ./examples/saga/compensate
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	taskq "github.com/Nuvraxis/taskQ"
	"github.com/Nuvraxis/taskQ/membroker"
	"github.com/Nuvraxis/taskQ/saga"
)

// OrderState is the state threaded through every step of the saga.
type OrderState struct {
	OrderID       string
	ReservationID string
	ChargeID      string
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	broker := membroker.New()
	defer broker.Close()

	steps := []saga.Step[OrderState]{
		{
			Name: "reserve-inventory",
			Do: func(ctx context.Context, s *OrderState) error {
				s.ReservationID = "resv-" + s.OrderID
				log.Printf("[%s] reserved inventory (reservation %s)", s.OrderID, s.ReservationID)
				return nil
			},
			Compensate: func(ctx context.Context, s *OrderState) error {
				/* release s.ReservationID, keyed by saga.HopFromContext(ctx) */
				return nil
			},
		},
		{
			Name: "charge-payment",
			Do: func(ctx context.Context, s *OrderState) error {
				s.ChargeID = "charge-" + s.OrderID
				log.Printf("[%s] charged payment (charge %s)", s.OrderID, s.ChargeID)
				return nil
			},
			Compensate: func(ctx context.Context, s *OrderState) error {
				/* refund s.ChargeID, keyed by saga.HopFromContext(ctx) */
				return nil
			},
		},
		{
			Name: "ship-order",
			Do: func(ctx context.Context, s *OrderState) error {
				log.Printf("[%s] attempting to ship...", s.OrderID)
				return errors.New("carrier API unavailable")
			},
			// No Compensate: this step never succeeds in this demo, so
			// it never needs undoing — only steps that already
			// completed (reserve, charge) get rolled back.
		},
	}

	done := make(chan struct{})
	sg := saga.New[OrderState](broker, "orders", steps,
		// WithMaxRetry(0): give up on the very first attempt instead of
		// retrying ship-order a few times first — keeps this demo's
		// output short. A production saga would normally leave this at
		// its default (3) so a genuinely transient failure gets a few
		// chances before triggering a rollback.
		saga.WithMaxRetry[OrderState](0),
		saga.WithOnComplete(func(ctx context.Context, sagaID string, state OrderState) {
			log.Printf("saga %s complete", sagaID)
			close(done)
		}),
		saga.WithOnFailed(func(ctx context.Context, sagaID string, state OrderState, failedStep string, cause error) {
			log.Printf("saga %s failed at %q: %v — every prior step has been rolled back", sagaID, failedStep, cause)
			close(done)
		}),
		saga.WithOnCompensationFailed(func(ctx context.Context, sagaID string, state OrderState, step string, cause error) {
			// Not expected in this demo — every Compensate above always
			// succeeds — but a real system should page a human here:
			// this is the one outcome a saga can't resolve automatically.
			log.Printf("saga %s: compensating %q itself failed: %v — MANUAL INTERVENTION NEEDED", sagaID, step, cause)
			close(done)
		}),
	)

	pool := taskq.NewPool[saga.Envelope[OrderState]](broker, "orders", sg.Handler())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	poolDone := make(chan struct{})
	go func() {
		if err := pool.Run(ctx); err != nil {
			log.Printf("pool.Run: %v", err)
		}
		close(poolDone)
	}()

	sagaID, err := sg.Start(ctx, OrderState{OrderID: "order-2002"})
	if err != nil {
		return fmt.Errorf("start saga: %w", err)
	}
	log.Printf("started saga %s", sagaID)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		log.Println("timed out waiting for the saga to finish")
	}

	cancel()
	broker.Close()
	<-poolDone
	return nil
}
