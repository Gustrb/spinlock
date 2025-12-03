package pool

import (
	"context"
	"errors"
	"sync"
)

type Task[T any] func(context.Context) (T, error)

type Future[T any] struct {
	result chan T
	err    chan error
}

type ThreadPool[T any] struct {
	tasks chan func()
	mu    sync.RWMutex
	// TODO: Atomic int?
	shutdown bool
	wg       sync.WaitGroup
}

func NewThreadPool[T any](workers int) *ThreadPool[T] {
	p := &ThreadPool[T]{
		tasks:    make(chan func(), 100),
		shutdown: false,
	}

	for range workers {
		p.wg.Go(func() {
			for run := range p.tasks {
				run()
			}
		})
	}

	return p
}

func (tp *ThreadPool[T]) SubmitWithContext(ctx context.Context, task Task[T]) (*Future[T], error) {
	fut := &Future[T]{
		result: make(chan T, 1),
		err:    make(chan error, 1),
	}

	tp.mu.RLock()
	if tp.shutdown {
		tp.mu.RUnlock()
		return nil, ErrPoolShutdown
	}
	tp.mu.RUnlock()

	tp.tasks <- func() {
		res, err := task(ctx)
		fut.result <- res
		fut.err <- err
	}

	return fut, nil
}

var (
	ErrPoolShutdown = errors.New("pool is already closed")
)

// Submit the submitted task is non-cancellable and we create a new context per task. If this is not the desirable
// behavior, please use SubmitWithContext
func (tp *ThreadPool[T]) Submit(task Task[T]) (*Future[T], error) {
	fut := &Future[T]{
		result: make(chan T, 1),
		err:    make(chan error, 1),
	}

	tp.mu.RLock()
	if tp.shutdown {
		tp.mu.RUnlock()
		return nil, ErrPoolShutdown
	}
	tp.mu.RUnlock()

	tp.tasks <- func() {
		ctx := context.Background()
		res, err := task(ctx)
		fut.result <- res
		fut.err <- err
	}

	return fut, nil
}

func (tp *ThreadPool[T]) Shutdown() {
	tp.mu.Lock()
	tp.shutdown = true
	close(tp.tasks)
	tp.mu.Unlock()
	tp.wg.Wait()
}

func (f *Future[T]) Get(ctx context.Context) (T, error) {
	var zero T

	select {
	case res := <-f.result:
		err := <-f.err
		return res, err
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}
