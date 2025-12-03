package pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

const (
	TaskQueueSize = 100
)

var (
	ErrPoolShutdown = errors.New("pool is already closed")
	ErrQueueFull    = errors.New("the queue is full")
	ErrPanic        = errors.New("the queue panicked")
)

type Task[T any] func(context.Context) (T, error)

type Future[T any] struct {
	ch chan outcome[T]
}

type outcome[T any] struct {
	result T
	err    error
}

type ThreadPool[T any] struct {
	tasks    chan func()
	mu       sync.RWMutex
	shutdown int32
	wg       sync.WaitGroup
}

func NewThreadPool[T any](workers int) *ThreadPool[T] {
	p := &ThreadPool[T]{
		tasks: make(chan func(), TaskQueueSize),
	}

	for range workers {
		p.wg.Go(func() {
			for run := range p.tasks {
				func() {
					defer func() {
						_ = recover()
					}()
					run()
				}()
			}
		})
	}

	return p
}

// safe blocking send that recovers if channel was closed concurrently.
// returns true if send succeeded; false if a panic happened (channel closed).
func (tp *ThreadPool[T]) safeSend(ctx context.Context, fn func()) (err error) {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	defer func() {
		if r := recover(); r != nil {
			err = ErrPanic
		}
	}()

	select {
	case tp.tasks <- fn:
		return nil
	case <-ctx.Done():
		return context.DeadlineExceeded
	}
}

// safe non-blocking send (select) that recovers if channel closed.
// returns (sent, queueFull). If sent==true => success.
// If sent==false && queueFull==true => queue was full (no send).
// If sent==false && queueFull==false => send failed due to closed channel.
func (tp *ThreadPool[T]) safeTrySend(fn func()) (sent bool, queueFull bool) {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	defer func() {
		if r := recover(); r != nil {
			// panic => channel closed
			sent = false
			queueFull = false
		}
	}()

	select {
	case tp.tasks <- fn:
		return true, false
	default:
		return false, true
	}
}

func (tp *ThreadPool[T]) SubmitWithContext(ctx context.Context, task Task[T]) (*Future[T], error) {
	if atomic.LoadInt32(&tp.shutdown) == 1 {
		return nil, ErrPoolShutdown
	}

	fut := &Future[T]{
		ch: make(chan outcome[T], 1),
	}

	fn := func() {
		res, err := task(ctx)
		fut.ch <- outcome[T]{result: res, err: err}
	}

	if err := tp.safeSend(ctx, fn); err != nil {
		return nil, err
	}

	return fut, nil
}

// Submit the submitted task is non-cancellable and we create a new context per task. If this is not the desirable
// behavior, please use SubmitWithContext
func (tp *ThreadPool[T]) Submit(task Task[T]) (*Future[T], error) {
	return tp.SubmitWithContext(context.Background(), task)
}

func (tp *ThreadPool[T]) TrySubmit(task Task[T]) (*Future[T], error) {
	return tp.TrySubmitWithContext(context.Background(), task)
}

func (tp *ThreadPool[T]) TrySubmitWithContext(ctx context.Context, task Task[T]) (*Future[T], error) {
	if atomic.LoadInt32(&tp.shutdown) == 1 {
		return nil, ErrPoolShutdown
	}

	fut := &Future[T]{
		ch: make(chan outcome[T], 1),
	}

	fn := func() {
		res, err := task(ctx)
		fut.ch <- outcome[T]{result: res, err: err}
	}

	sent, queueFull := tp.safeTrySend(fn)
	if sent {
		return fut, nil
	}

	if queueFull {
		return nil, ErrQueueFull
	}

	return nil, ErrPoolShutdown
}

func (tp *ThreadPool[T]) SubmitWithTimeout(task Task[T], timeout time.Duration) (*Future[T], error) {
	if atomic.LoadInt32(&tp.shutdown) == 1 {
		return nil, ErrPoolShutdown
	}

	fut := &Future[T]{
		ch: make(chan outcome[T], 1),
	}

	// execution-time timeout
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	fn := func() {
		defer cancel()
		res, err := task(ctx)
		fut.ch <- outcome[T]{result: res, err: err}
	}

	// submission-time timeout
	submitCtx, submitCancel := context.WithTimeout(context.Background(), timeout)
	defer submitCancel()

	if err := tp.safeSend(submitCtx, fn); err != nil {
		return nil, err
	}

	return fut, nil
}

func (tp *ThreadPool[T]) Shutdown() {
	if !atomic.CompareAndSwapInt32(&tp.shutdown, 0, 1) {
		return
	}

	tp.mu.Lock()
	close(tp.tasks)
	tp.mu.Unlock()
	tp.wg.Wait()
}

func (f *Future[T]) Get(ctx context.Context) (T, error) {
	var zero T

	select {
	case out := <-f.ch:
		return out.result, out.err
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}
