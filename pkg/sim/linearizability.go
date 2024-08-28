package sim

import (
	"fmt"
)

// CheckResult details the outcome of linearizability verification.
type CheckResult struct {
	Linearizable     bool
	LinearizedOrder  []Operation
	ViolationReason  string
	StatesExplored   int
}

// CheckLinearizability executes the Wing & Gong (WGL) algorithm to verify
// whether the concurrent execution history is strictly linearizable against the model.
func CheckLinearizability(model Model, history *History) CheckResult {
	ops := history.Operations()
	n := len(ops)
	if n == 0 {
		return CheckResult{Linearizable: true}
	}

	// Compute precedence matrix:
	// precededBy[i] contains all indices j such that op[j] strictly preceded op[i] (End[j] < Start[i]).
	precededBy := make([][]int, n)
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j && ops[j].EndNano < ops[i].StartNano {
				precededBy[i] = append(precededBy[i], j)
			}
		}
	}

	linearized := make([]bool, n)
	currentOrder := make([]Operation, 0, n)
	visitedStates := make(map[string]bool)
	statesExplored := 0

	var search func(state any) bool
	search = func(state any) bool {
		statesExplored++

		if len(currentOrder) == n {
			return true // All operations successfully linearized!
		}

		// State memoization key: bitmask of linearized operations + string representation of state
		memoKey := fmt.Sprintf("%v:%v", linearized, state)
		if visitedStates[memoKey] {
			return false
		}
		visitedStates[memoKey] = true

		// Find candidate operations ready to be linearized:
		// An operation is ready if:
		// 1. It is not yet linearized.
		// 2. All operations that strictly preceded it in real-time have already been linearized.
		for i := 0; i < n; i++ {
			if linearized[i] {
				continue
			}

			// Check real-time precedence constraint
			canLinearize := true
			for _, predIdx := range precededBy[i] {
				if !linearized[predIdx] {
					canLinearize = false
					break
				}
			}

			if !canLinearize {
				continue
			}

			// Attempt step in model
			op := ops[i]
			nextState, expectedOut, ok := model.Step(state, op)
			if !ok {
				continue
			}

			// Verify if observed output matches expected output
			if !model.Equal(op.Output, expectedOut) {
				continue
			}

			// Forward transition in search
			linearized[i] = true
			currentOrder = append(currentOrder, op)

			if search(nextState) {
				return true
			}

			// Backtrack
			currentOrder = currentOrder[:len(currentOrder)-1]
			linearized[i] = false
		}

		return false
	}

	initialState := model.Init()
	isLinearizable := search(initialState)

	if !isLinearizable {
		return CheckResult{
			Linearizable:    false,
			ViolationReason: fmt.Sprintf("no valid linearization exists among %d concurrent operations", n),
			StatesExplored:  statesExplored,
		}
	}

	resultOrder := make([]Operation, len(currentOrder))
	copy(resultOrder, currentOrder)

	return CheckResult{
		Linearizable:    true,
		LinearizedOrder: resultOrder,
		StatesExplored:  statesExplored,
	}
}
