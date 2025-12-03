package pool

import (
	"context"
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
}

func NewThreadPool[T any](workers int) *ThreadPool[T] {
	p := &ThreadPool[T]{
		tasks: make(chan func(), 100),
	}

	for range workers {
		go func() {
			for run := range p.tasks {
				run()
			}
		}()
	}

	return p
}

func (tp *ThreadPool[T]) SubmitWithContext(ctx context.Context, task Task[T]) *Future[T] {
	fut := &Future[T]{
		result: make(chan T, 1),
		err:    make(chan error, 1),
	}

	tp.tasks <- func() {
		res, err := task(ctx)
		fut.result <- res
		fut.err <- err
	}

	return fut
}

// Submit the submitted task is non-cancellable and we create a new context per task. If this is not the desirable
// behavior, please use SubmitWithContext
func (tp *ThreadPool[T]) Submit(task Task[T]) *Future[T] {
	fut := &Future[T]{
		result: make(chan T, 1),
		err:    make(chan error, 1),
	}

	tp.tasks <- func() {
		ctx := context.Background()
		res, err := task(ctx)
		fut.result <- res
		fut.err <- err
	}

	return fut
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
