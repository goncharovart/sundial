package sundial

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func TestMemoryLeader_SingleAcquire(t *testing.T) {
	l := NewMemoryLeader()
	ok, err := l.TryAcquire(context.Background(), "node-1")
	if err != nil || !ok {
		t.Fatalf("first TryAcquire returned (%v, %v), want (true, nil)", ok, err)
	}
	if !l.IsLeader() {
		t.Error("IsLeader = false after successful TryAcquire")
	}
}

func TestMemoryLeader_RejectsEmptyNodeID(t *testing.T) {
	l := NewMemoryLeader()
	_, err := l.TryAcquire(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty nodeID")
	}
}

func TestMemoryLeader_IdempotentReacquire(t *testing.T) {
	l := NewMemoryLeader()
	ctx := context.Background()
	_, _ = l.TryAcquire(ctx, "node-1")
	ok, err := l.TryAcquire(ctx, "node-1")
	if err != nil || !ok {
		t.Fatalf("re-acquire by same node returned (%v, %v); expected (true, nil)", ok, err)
	}
}

func TestMemoryLeader_TwoNodesCompete(t *testing.T) {
	// In the in-process case, two Schedulers share a single
	// MemoryLeader; that is how we simulate two cluster nodes
	// racing for leadership.
	shared := NewMemoryLeader()

	ok1, _ := shared.TryAcquire(context.Background(), "node-1")
	if !ok1 {
		t.Fatal("node-1 failed to acquire")
	}

	// node-2 has its own MemoryLeader view but in real life would
	// hit the same Postgres advisory lock. We emulate by sharing.
	ok2, _ := shared.TryAcquire(context.Background(), "node-2")
	if ok2 {
		t.Fatal("node-2 acquired while node-1 holds the lock")
	}
}

func TestMemoryLeader_ReleaseFreesLock(t *testing.T) {
	shared := NewMemoryLeader()
	ctx := context.Background()

	ok1, _ := shared.TryAcquire(ctx, "node-1")
	if !ok1 {
		t.Fatal("setup: node-1 acquire failed")
	}
	if err := shared.Release(ctx); err != nil {
		t.Fatalf("Release failed: %v", err)
	}

	ok2, _ := shared.TryAcquire(ctx, "node-2")
	if !ok2 {
		t.Fatal("node-2 should acquire after node-1 Release")
	}
}

func TestMemoryLeader_ReleaseIsIdempotent(t *testing.T) {
	l := NewMemoryLeader()
	ctx := context.Background()
	if err := l.Release(ctx); err != nil {
		t.Errorf("Release on cold leader returned %v; expected nil", err)
	}
	_, _ = l.TryAcquire(ctx, "n1")
	if err := l.Release(ctx); err != nil {
		t.Errorf("Release while held returned %v; expected nil", err)
	}
	if err := l.Release(ctx); err != nil {
		t.Errorf("Release after Release returned %v; expected nil", err)
	}
}

func TestMemoryLeader_ConcurrentTryAcquireRaces(t *testing.T) {
	shared := NewMemoryLeader()
	const N = 64
	var acquired int32
	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start
			ok, _ := shared.TryAcquire(context.Background(), fmt.Sprintf("node-%d", id))
			if ok {
				atomic.AddInt32(&acquired, 1)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if got := atomic.LoadInt32(&acquired); got != 1 {
		t.Fatalf("%d goroutines won the race; expected exactly 1", got)
	}
}
