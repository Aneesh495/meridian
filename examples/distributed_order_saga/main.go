package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/Aneesh495/meridian/pkg/workflow"
)

type InventoryReservation struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

type PaymentAuth struct {
	OrderID string  `json:"order_id"`
	Amount  float64 `json:"amount"`
}

type ShippingDispatch struct {
	OrderID     string `json:"order_id"`
	Destination string `json:"destination"`
}

func main() {
	fmt.Println("=== Meridian Example: Distributed Order Fulfillment Saga ===")
	fmt.Println()

	var stateMu sync.Mutex
	inventoryStock := map[string]int{"SKU-9942": 100}
	paymentAuthorized := false
	shipmentCreated := false

	activities := map[string]workflow.ActivityFunc{
		"ReserveInventory": func(ctx context.Context, in []byte) ([]byte, error) {
			stateMu.Lock()
			defer stateMu.Unlock()

			var req InventoryReservation
			if err := json.Unmarshal(in, &req); err != nil {
				return nil, err
			}

			if inventoryStock[req.SKU] < req.Quantity {
				return nil, fmt.Errorf("insufficient inventory for %s", req.SKU)
			}
			inventoryStock[req.SKU] -= req.Quantity
			fmt.Printf("   [Inventory] Reserved %d units of %s (remaining: %d)\n", req.Quantity, req.SKU, inventoryStock[req.SKU])
			return []byte("inventory-reserved"), nil
		},

		"ReleaseInventory": func(ctx context.Context, in []byte) ([]byte, error) {
			stateMu.Lock()
			defer stateMu.Unlock()

			var req InventoryReservation
			_ = json.Unmarshal(in, &req)
			inventoryStock[req.SKU] += req.Quantity
			fmt.Printf("   [Compensation] Released %d units of %s back to inventory (total: %d)\n", req.Quantity, req.SKU, inventoryStock[req.SKU])
			return []byte("inventory-released"), nil
		},

		"AuthorizePayment": func(ctx context.Context, in []byte) ([]byte, error) {
			stateMu.Lock()
			defer stateMu.Unlock()

			var req PaymentAuth
			if err := json.Unmarshal(in, &req); err != nil {
				return nil, err
			}
			paymentAuthorized = true
			fmt.Printf("   [Payment] Authorized $%.2f for order %s\n", req.Amount, req.OrderID)
			return []byte("payment-authorized"), nil
		},

		"RefundPayment": func(ctx context.Context, in []byte) ([]byte, error) {
			stateMu.Lock()
			defer stateMu.Unlock()

			paymentAuthorized = false
			fmt.Println("   [Compensation] Voided payment authorization and issued full refund")
			return []byte("payment-refunded"), nil
		},

		"DispatchShipment": func(ctx context.Context, in []byte) ([]byte, error) {
			stateMu.Lock()
			defer stateMu.Unlock()

			var req ShippingDispatch
			_ = json.Unmarshal(in, &req)

			// Simulate downstream shipping failure for out-of-range destination
			if req.Destination == "Restricted-Zone-9" {
				return nil, errors.New("carrier delivery unavailable to restricted zone")
			}
			shipmentCreated = true
			return []byte("shipment-dispatched"), nil
		},
	}

	orderWorkflow := func(ctx workflow.WorkflowContext, in []byte) ([]byte, error) {
		fmt.Println("1. Executing Step 1: Inventory Reservation...")
		invPayload, _ := json.Marshal(InventoryReservation{SKU: "SKU-9942", Quantity: 5})
		f1 := ctx.ExecuteActivity("ReserveInventory", invPayload, workflow.ActivityTaskOptions{})
		if _, err := f1.Get(); err != nil {
			return nil, err
		}

		// Register reverse compensation
		ctx.Saga().AddStep("ReleaseInventory", func() error {
			f := ctx.ExecuteActivity("ReleaseInventory", invPayload, workflow.ActivityTaskOptions{})
			_, err := f.Get()
			return err
		})

		fmt.Println("2. Executing Step 2: Payment Authorization...")
		payPayload, _ := json.Marshal(PaymentAuth{OrderID: "ORD-8801", Amount: 450.00})
		f2 := ctx.ExecuteActivity("AuthorizePayment", payPayload, workflow.ActivityTaskOptions{})
		if _, err := f2.Get(); err != nil {
			_ = ctx.Saga().Compensate()
			return nil, err
		}

		// Register reverse compensation
		ctx.Saga().AddStep("RefundPayment", func() error {
			f := ctx.ExecuteActivity("RefundPayment", payPayload, workflow.ActivityTaskOptions{})
			_, err := f.Get()
			return err
		})

		fmt.Println("3. Executing Step 3: Shipping Carrier Dispatch (Destination: Restricted-Zone-9)...")
		shipPayload, _ := json.Marshal(ShippingDispatch{OrderID: "ORD-8801", Destination: "Restricted-Zone-9"})
		f3 := ctx.ExecuteActivity("DispatchShipment", shipPayload, workflow.ActivityTaskOptions{})
		if _, err := f3.Get(); err != nil {
			fmt.Printf("   ERROR: Shipping failed (%v). Unwinding Saga in LIFO order...\n", err)
			if compErr := ctx.Saga().Compensate(); compErr != nil {
				return nil, fmt.Errorf("saga compensation failure: %w", compErr)
			}
			return nil, fmt.Errorf("order failed but safely compensated: %w", err)
		}

		return []byte("order-fulfilled"), nil
	}

	history := workflow.NewHistory("order-fulfillment-workflow", "run-101")
	replayer := workflow.NewReplayEngine(history, activities)

	_, err := replayer.Execute(orderWorkflow, []byte("start"))
	if err == nil {
		fmt.Println("UNEXPECTED: Order workflow should have encountered simulated shipping error")
		os.Exit(1)
	}

	fmt.Println()
	fmt.Println("4. Verifying Post-Compensation Invariants:")
	stateMu.Lock()
	fmt.Printf("   Inventory Stock: %d (Expected: 100)\n", inventoryStock["SKU-9942"])
	fmt.Printf("   Payment Authorized: %v (Expected: false)\n", paymentAuthorized)
	fmt.Printf("   Shipment Created: %v (Expected: false)\n", shipmentCreated)

	if inventoryStock["SKU-9942"] == 100 && !paymentAuthorized && !shipmentCreated {
		fmt.Println()
		fmt.Println("SUCCESS: Saga safely rolled back all external mutations in reverse order!")
	} else {
		fmt.Println("FAILURE: Saga state invariant violated!")
		os.Exit(1)
	}
	stateMu.Unlock()
}
