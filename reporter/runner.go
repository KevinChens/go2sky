package reporter

import (
	"context"
	"google.golang.org/grpc/connectivity"
	configuration "skywalking.apache.org/repo/goapi/collect/agent/configuration/v3"
	"time"
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

// New a runner
func New() *Runner {
	return &Runner{}
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

// Run fetch config
func (r *gRPCReporter) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
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
}
