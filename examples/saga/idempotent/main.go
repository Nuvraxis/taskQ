// Command idempotent shows why Compensate must be safe to retry and how
// saga.HopFromContext gives you a stable key to make it so.
//
// The refund gateway here is deliberately flaky: the first refund call
// succeeds on the gateway side but returns an error, as if the network
// dropped before the response arrived. taskQ retries the hop; the second
// call presents the same idempotency key and the gateway reports it as a
// duplicate instead of refunding again.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	taskq "github.com/Nuvraxis/taskQ"
	"github.com/Nuvraxis/taskQ/membroker"
	"github.com/Nuvraxis/taskQ/saga"
)

type OrderState struct {
	OrderID  string
	ChargeID string // set by charge-payment, read by its Compensate
}

// gateway stands in for a payment provider that honors idempotency keys.
type gateway struct {
	mu       sync.Mutex
	refunded map[string]string // idempotency key -> charge ID
	flaky    bool              // fail once after the refund lands
}

// Refund issues a refund for chargeID under key. dup is true when key has
// been seen before, in which case nothing is refunded again.
func (g *gateway) Refund(key, chargeID string) (dup bool, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, seen := g.refunded[key]; seen {
		return true, nil
	}
	g.refunded[key] = chargeID
	if g.flaky {
		g.flaky = false
		return false, errors.New("connection dropped after the refund was issued")
	}
	return false, nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	broker := membroker.New()
	defer broker.Close()

	gw := &gateway{refunded: map[string]string{}, flaky: true}

	steps := []saga.Step[OrderState]{
		{
			Name: "charge-payment",
			Do: func(_ context.Context, s *OrderState) error {
				s.ChargeID = "charge-" + s.OrderID
				log.Printf("charged %s", s.ChargeID)
				return nil
			},
			Compensate: func(ctx context.Context, s *OrderState) error {
				hop, _ := saga.HopFromContext(ctx)
				dup, err := gw.Refund(hop.Key(), s.ChargeID)
				switch {
				case err != nil:
					log.Printf("refund %s failed: %v (taskQ will retry this hop)", s.ChargeID, err)
					return err
				case dup:
					log.Printf("refund %s already issued under %s, skipping", s.ChargeID, hop.Key())
				default:
					log.Printf("refunded %s", s.ChargeID)
				}
				return nil
			},
		},
		{
			Name: "ship-order",
			Do: func(_ context.Context, _ *OrderState) error {
				return errors.New("carrier rejected the parcel")
			},
		},
	}

	done := make(chan struct{})
	sg := saga.New(broker, "orders", steps,
		saga.WithMaxRetry[OrderState](2),
		saga.WithOnFailed(func(_ context.Context, id string, _ OrderState, step string, cause error) {
			log.Printf("saga %s rolled back; %s failed: %v", id, step, cause)
			close(done)
		}),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool := taskq.NewPool(broker, "orders", sg.Handler(),
		taskq.WithBackoffStrategy(taskq.ExponentialBackoff{
			Base: 100 * time.Millisecond, Max: time.Second, Factor: 2,
		}),
	)
	go func() { _ = pool.Run(ctx) }()

	if _, err := sg.Start(ctx, OrderState{OrderID: "order-1001"}); err != nil {
		return fmt.Errorf("start saga: %w", err)
	}

	select {
	case <-done:
	case <-ctx.Done():
		return errors.New("saga did not finish in time")
	}

	fmt.Printf("refunds issued: %d (one, across two Compensate attempts)\n", len(gw.refunded))
	return nil
}
