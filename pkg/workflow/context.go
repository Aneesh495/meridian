package workflow

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/Aneesh495/meridian/pkg/core"
)

// WorkflowContext provides the deterministic execution environment passed into workflows.
type WorkflowContext interface {
	WorkflowID() core.WorkflowID
	RunID() core.RunID
	Now() time.Time
	NewUUID() string
	ExecuteActivity(name string, input []byte, opts ActivityTaskOptions) Future
	Sleep(d time.Duration) Future
	Saga() *Saga
}

type workflowContextImpl struct {
	workflowID core.WorkflowID
	runID      core.RunID
	virtualNow time.Time
	seqCounter uint64
	saga       *Saga
	replayer   *ReplayEngine
}

func (ctx *workflowContextImpl) WorkflowID() core.WorkflowID {
	return ctx.workflowID
}

func (ctx *workflowContextImpl) RunID() core.RunID {
	return ctx.runID
}

func (ctx *workflowContextImpl) Now() time.Time {
	return ctx.virtualNow
}

// NewUUID generates a deterministic pseudorandom identifier derived from workflow ID and sequence counter.
func (ctx *workflowContextImpl) NewUUID() string {
	ctx.seqCounter++
	seed := fmt.Sprintf("%s:%s:%d", ctx.workflowID, ctx.runID, ctx.seqCounter)
	h := sha256.Sum256([]byte(seed))
	return hex.EncodeToString(h[:16])
}

func (ctx *workflowContextImpl) ExecuteActivity(name string, input []byte, opts ActivityTaskOptions) Future {
	return ctx.replayer.ScheduleActivity(name, input, opts)
}

func (ctx *workflowContextImpl) Sleep(d time.Duration) Future {
	return ctx.replayer.ScheduleTimer(d)
}

func (ctx *workflowContextImpl) Saga() *Saga {
	return ctx.saga
}
