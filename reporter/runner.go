package reporter

import (
	"context"
)

// Task  has a run method with context parameter
type Task interface {
	Run(ctx context.Context)
}

// Runner task group management
type Runner struct {
	tasks  []Task             // list of task
	cancel context.CancelFunc // notify runner to stop task
}

// Add task to runner
func (r *Runner) Add(tasks ...Task) {
	r.tasks = append(r.tasks, tasks...)
}

// Start a runner
func (r *Runner) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	for _, task := range r.tasks {
		go task.Run(ctx)
	}
	r.cancel = cancel
}

// Stop a runner
func (r *Runner) Stop() {
	r.cancel()
}
