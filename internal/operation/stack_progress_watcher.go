package operation

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/go-to-k/delstack/pkg/client"
)

const stackProgressPollInterval = 3 * time.Second

// StackProgressWatcher streams CloudFormation stack events to the writer while a
// stack is being deleted, rendering each event in CDK-style:
//
//	<StackName> | <N>/<Total> | HH:MM:SS | <ResourceStatus> | <ResourceType> | <LogicalId> (<PhysicalId>)
type StackProgressWatcher struct {
	stackName  string
	cfn        client.ICloudFormation
	totalCount int
	startTime  time.Time
	out        io.Writer

	mu           sync.Mutex
	printedCount int
	seen         map[string]struct{}
}

func NewStackProgressWatcher(stackName string, cfn client.ICloudFormation, totalCount int) *StackProgressWatcher {
	return &StackProgressWatcher{
		stackName:  stackName,
		cfn:        cfn,
		totalCount: totalCount,
		startTime:  time.Now(),
		out:        os.Stderr,
		seen:       map[string]struct{}{},
	}
}

// Start launches a background poller that prints new events until ctx is
// canceled. Returns a function to call after cancel() to wait for the final
// drain and ensure the last batch of events has been printed.
func (w *StackProgressWatcher) Start(ctx context.Context) (waitDone func()) {
	done := make(chan struct{})

	go func() {
		defer close(done)
		ticker := time.NewTicker(stackProgressPollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				// Final drain using a fresh context so we still print the last
				// events after the parent ctx was canceled.
				drainCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				w.pollOnce(drainCtx)
				cancel()
				return
			case <-ticker.C:
				w.pollOnce(ctx)
			}
		}
	}()

	return func() { <-done }
}

func (w *StackProgressWatcher) pollOnce(ctx context.Context) {
	events, err := w.cfn.DescribeStackEvents(ctx, aws.String(w.stackName))
	if err != nil {
		return
	}

	// DescribeStackEvents returns events newest-first; print oldest-first.
	sort.SliceStable(events, func(i, j int) bool {
		ti := events[i].Timestamp
		tj := events[j].Timestamp
		if ti == nil || tj == nil {
			return false
		}
		return ti.Before(*tj)
	})

	for _, ev := range events {
		if ev.Timestamp != nil && ev.Timestamp.Before(w.startTime) {
			continue
		}
		id := aws.ToString(ev.EventId)
		if id == "" {
			continue
		}
		w.mu.Lock()
		if _, ok := w.seen[id]; ok {
			w.mu.Unlock()
			continue
		}
		w.seen[id] = struct{}{}
		w.printedCount++
		n := w.printedCount
		w.mu.Unlock()

		w.printEvent(n, ev)
	}
}

func (w *StackProgressWatcher) printEvent(n int, ev types.StackEvent) {
	ts := "--:--:--"
	if ev.Timestamp != nil {
		ts = ev.Timestamp.Local().Format("15:04:05")
	}
	status := string(ev.ResourceStatus)
	resType := aws.ToString(ev.ResourceType)
	logical := aws.ToString(ev.LogicalResourceId)
	physical := aws.ToString(ev.PhysicalResourceId)

	line := fmt.Sprintf("%s | %d/%d | %s | %s | %s | %s",
		w.stackName, n, w.totalCount, ts, status, resType, logical)
	if physical != "" && physical != logical {
		line += fmt.Sprintf(" (%s)", physical)
	}
	if reason := aws.ToString(ev.ResourceStatusReason); reason != "" {
		line += " " + reason
	}
	fmt.Fprintln(w.out, line)
}
