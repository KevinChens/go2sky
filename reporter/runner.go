package reporter

import (
	"context"
	"google.golang.org/grpc/connectivity"
	configuration "skywalking.apache.org/repo/goapi/collect/agent/configuration/v3"
	"time"
)

// Task defines a task
type Task struct {
	Name  func(r *gRPCReporter) // name of the task
	Param *gRPCReporter         // parameter of the task
}

// Runner task group management
type Runner struct {
	tasks    []Task     // list of task
	complete chan error // receiving task completed signal
}

// New a runner
func New() *Runner {
	return &Runner{
		complete: make(chan error),
	}
}

// Add task to runner
func (r *Runner) Add(tasks ...Task) {
	r.tasks = append(r.tasks, tasks...)
}

// Start a runner
func (r *Runner) Start() {
	go func() {
		r.complete <- r.run()
	}()
}

// Stop a runner
func (r *Runner) Stop() error {
	select {
	case err := <-r.complete:
		return err
	}
}

// run a task
func (r *Runner) run() error {
	for _, task := range r.tasks {
		task.Name(task.Param)
	}
	return nil
}

// FetchConfiguration fetch config
func FetchConfiguration() func(r *gRPCReporter) {
	return func(r *gRPCReporter) {
		for {
			if r.conn.GetState() == connectivity.Shutdown {
				break
			}

			configurations, err := r.cdsClient.FetchConfigurations(context.Background(), &configuration.ConfigurationSyncRequest{
				Service: r.service,
				Uuid:    r.cdsService.UUID,
			})

			if err != nil {
				r.logger.Errorf("fetch dynamic configuration error %v", err)
				time.Sleep(r.cdsInterval)
				continue
			}

			if len(configurations.GetCommands()) > 0 && configurations.GetCommands()[0].Command == "ConfigurationDiscoveryCommand" {
				command := configurations.GetCommands()[0]
				r.cdsService.HandleCommand(command)
			}

			time.Sleep(r.cdsInterval)
		}
	}
}
