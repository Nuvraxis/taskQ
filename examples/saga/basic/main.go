// Command basic demonstrates a straightforward, all-succeeds saga: an
// order workflow that reserves inventory, charges payment, and ships the
// order, each as its own durable step. It uses membroker for zero setup;
// the saga package itself has no broker-specific code at all — the same
// Saga[S] runs unmodified over redisbroker or pgbroker.
//
// Run it with:
//
//	go run ./examples/saga/basic
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	taskq "github.com/Nuvraxis/taskQ"
	"github.com/Nuvraxis/taskQ/membroker"
	"github.com/Nuvraxis/taskQ/saga"
)

// OrderState is the state threaded through every step of the saga. Each
// step mutates it to record what it did, so a later Compensate knows
// exactly what needs undoing.
type OrderState struct {
	OrderID       string
	ItemSKU       string
	AmountCents   int
	ReservationID string // set by "reserve-inventory", read by its Compensate
	ChargeID      string // set by "charge-payment", read by its Compensate
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
				// A real step would call an inventory service here.
				s.ReservationID = "resv-" + s.OrderID
				log.Printf("[%s] reserved %s (reservation %s)", s.OrderID, s.ItemSKU, s.ReservationID)
				return nil
			},
			Compensate: func(ctx context.Context, s *OrderState) error {
				log.Printf("[%s] releasing reservation %s", s.OrderID, s.ReservationID)
				return nil
			},
		},
		{
			Name: "charge-payment",
			Do: func(ctx context.Context, s *OrderState) error {
				// A real step would call a payment gateway here.
				s.ChargeID = "charge-" + s.OrderID
				log.Printf("[%s] charged $%.2f (charge %s)", s.OrderID, float64(s.AmountCents)/100, s.ChargeID)
				return nil
			},
			Compensate: func(ctx context.Context, s *OrderState) error {
				log.Printf("[%s] refunding charge %s", s.OrderID, s.ChargeID)
				return nil
			},
		},
		{
			Name: "ship-order",
			Do: func(ctx context.Context, s *OrderState) error {
				// The last step — nothing depends on undoing it, so it
				// has no Compensate. A nil Compensate is treated as an
				// automatic no-op if rollback ever reached this far.
				log.Printf("[%s] shipped", s.OrderID)
				return nil
			},
		},
	}

	done := make(chan struct{})
	sg := saga.New[OrderState](broker, "orders", steps,
		saga.WithOnComplete(func(ctx context.Context, sagaID string, state OrderState) {
			log.Printf("saga %s complete: order %s fully processed", sagaID, state.OrderID)
			close(done)
		}),
		saga.WithOnFailed(func(ctx context.Context, sagaID string, state OrderState, failedStep string, cause error) {
			log.Printf("saga %s failed at %q: %v", sagaID, failedStep, cause)
			close(done)
		}),
	)

	// The Pool is what actually drives the saga forward: every hop —
	// each step, each direction — passes through here, one dequeue at a
	// time. There's nothing saga-specific about it; it's the exact same
	// taskq.Pool used for any other Handler[T].
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

	sagaID, err := sg.Start(ctx, OrderState{
		OrderID:     "order-1001",
		ItemSKU:     "sku-42",
		AmountCents: 4999,
	})
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
