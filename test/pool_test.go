package test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Gustrb/spinlock/pool"
)

func TestShouldBeAbleToSubmitTasks(t *testing.T) {
	tp := pool.NewThreadPool[int](10)

	future, _ := tp.Submit(func(_ context.Context) (int, error) {
		return 10, nil
	})

	result, err := future.Get(context.Background())
	if err != nil {
		t.Fatalf("Expected error to be nil, got: %s", err)
	}

	if result != 10 {
		t.Fatalf("Expected result to be 10, got: %d", result)
	}
	future, _ = tp.Submit(func(_ context.Context) (int, error) {
		return 0, errors.New("dummy err")
	})

	result, err = future.Get(context.Background())
	if err == nil {
		t.Fatalf("Expected error to not be nil, got: nil")
	}
}

func TestShouldBeAbleToHandleConcurrentFutures(t *testing.T) {
	var mu sync.Mutex
	futureMap := make(map[int]*pool.Future[int], 100)
	tp := pool.NewThreadPool[int](10)
	var wg sync.WaitGroup

	for i := range 100 {
		wg.Go(func() {
			mu.Lock()
			futureMap[i], _ = tp.Submit(func(_ context.Context) (int, error) {
				return i, nil
			})
			mu.Unlock()
		})
	}

	wg.Wait()

	for k, fut := range futureMap {
		result, err := fut.Get(context.Background())
		if err != nil {
			t.Fatalf("expected future to not fail: %s", err)
		}

		if result != k {
			t.Fatalf("unexpected value from future, expected: %d, got: %d", k, result)
		}
	}
}

func TestShouldBeAbleToHandleCancellation(t *testing.T) {
	tp := pool.NewThreadPool[int](10)

	future, _ := tp.Submit(func(ctx context.Context) (int, error) {
		time.Sleep(time.Second * 10)
		return 1, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	_, err := future.Get(ctx)
	if err == nil {
		t.Fatalf("expected context cancellation")
	}
}

func TestCancelPropagation(t *testing.T) {
	tp := pool.NewThreadPool[int](10)

	parentCtx, parentCancel := context.WithCancel(context.Background())
	future, _ := tp.SubmitWithContext(parentCtx, func(ctx context.Context) (int, error) {
		select {
		case <-time.After(10 * time.Second):
			return 1, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	})

	// cancel BEFORE calling Get
	parentCancel()

	_, err := future.Get(context.Background())
	if err == nil {
		t.Fatalf("expected task cancellation")
	}
}

func TestShutdownProperly(t *testing.T) {
	tp := pool.NewThreadPool[int](10)

	var counter atomic.Int32

	for range 100 {
		tp.Submit(func(ctx context.Context) (int, error) {
			time.Sleep(10 * time.Millisecond)
			counter.Add(1)
			return 0, nil
		})
	}

	tp.Shutdown()

	if counter.Load() != 100 {
		t.Fatalf("expected all tasks to finish, got %d", counter.Load())
	}

	_, err := tp.Submit(func(ctx context.Context) (int, error) {
		return 1, nil
	})

	if !errors.Is(err, pool.ErrPoolShutdown) {
		t.Fatalf("expected ErrShutdown, got %v", err)
	}
}

func TestTrySubmitShouldNotBlock_ChannelFull(t *testing.T) {
	tp := pool.NewThreadPool[int](0)
	defer tp.Shutdown()

	for i := range pool.TaskQueueSize {
		_, err := tp.Submit(func(ctx context.Context) (int, error) {
			return i, nil
		})
		if err != nil {
			t.Fatalf("Failed to submit task %d to fill buffer: %v", i, err)
		}
	}

	_, err := tp.TrySubmit(func(ctx context.Context) (int, error) {
		return 42, nil
	})

	if !errors.Is(err, pool.ErrQueueFull) {
		t.Fatalf("Expected ErrQueueFull, but got %v", err)
	}

	tp.Shutdown()
}

func TestSubmitWithTimeout_SubmissionTimeout(t *testing.T) {
	tp := pool.NewThreadPool[int](0)

	// Fill the queue with blocked tasks
	blockers := make([]chan struct{}, pool.TaskQueueSize)
	for i := range pool.TaskQueueSize {
		blockers[i] = make(chan struct{})
		_, err := tp.SubmitWithContext(context.Background(), func(ctx context.Context) (int, error) {
			<-blockers[i]
			return 0, nil
		})
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
	}

	// Now queue is full, this should timeout
	_, err := tp.SubmitWithTimeout(func(ctx context.Context) (int, error) {
		return 123, nil
	}, 10*time.Millisecond)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %#v", err)
	}

	for i := range blockers {
		close(blockers[i])
	}
	tp.Shutdown()
}

func TestSubmitWithTimeout_TaskExecutionTimeout(t *testing.T) {
	tp := pool.NewThreadPool[int](1)

	fut, err := tp.SubmitWithTimeout(func(ctx context.Context) (int, error) {
		select {
		case <-time.After(200 * time.Millisecond):
			return 1, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}, 50*time.Millisecond)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, taskErr := fut.Get(context.Background())

	if !errors.Is(taskErr, context.DeadlineExceeded) {
		t.Fatalf("expected task to timeout with DeadlineExceeded, got %#v", taskErr)
	}
}

func TestSubmitWithTimeout_Success(t *testing.T) {
	tp := pool.NewThreadPool[int](1)

	fut, err := tp.SubmitWithTimeout(func(ctx context.Context) (int, error) {
		return 99, nil
	}, 100*time.Millisecond)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val, futErr := fut.Get(context.Background())

	if futErr != nil {
		t.Fatalf("unexpected future error: %v", futErr)
	}
	if val != 99 {
		t.Fatalf("expected 99, got %d", val)
	}
}

func TestSubmitWithTimeout_ShutdownBeforeSubmit(t *testing.T) {
	tp := pool.NewThreadPool[int](1)
	tp.Shutdown()

	_, err := tp.SubmitWithTimeout(func(ctx context.Context) (int, error) {
		return 1, nil
	}, 50*time.Millisecond)

	if !errors.Is(err, pool.ErrPoolShutdown) {
		t.Fatalf("expected ErrPoolShutdown, got %#v", err)
	}
}

func TestSubmitWithTimeout_ShutdownDuringSubmit(t *testing.T) {
	tp := pool.NewThreadPool[int](0)

	// fill queue
	blockers := make([]chan struct{}, pool.TaskQueueSize)
	for i := range pool.TaskQueueSize {
		blockers[i] = make(chan struct{})
		tp.Submit(func(ctx context.Context) (int, error) {
			<-blockers[i]
			return 0, nil
		})
	}

	// race: shutdown shortly after starting submit
	done := make(chan error)

	go func() {
		_, err := tp.SubmitWithTimeout(func(ctx context.Context) (int, error) {
			return 1, nil
		}, 200*time.Millisecond)
		done <- err
	}()

	time.Sleep(10 * time.Millisecond)
	tp.Shutdown()

	err := <-done

	if err != pool.ErrPoolShutdown && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected ErrPoolShutdown or DeadlineExceeded, got %#v", err)
	}

	for i := range blockers {
		close(blockers[i])
	}
}
