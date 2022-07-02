package reporter

import (
	"context"
	"github.com/SkyAPM/go2sky"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	configuration "skywalking.apache.org/repo/goapi/collect/agent/configuration/v3"
	"time"
)

type cdsTask struct {
	conn        *grpc.ClientConn
	cdsClient   configuration.ConfigurationDiscoveryServiceClient
	cdsWatchers []go2sky.AgentConfigChangeWatcher
}

// Run fetch config
// need parameter: r.conn, r.cdsClient, ctk.cdsWatchers
func (ctk *cdsTask) Run(ctx context.Context) {

	r := &gRPCReporter{
		conn:      ctk.conn,
		cdsClient: ctk.cdsClient,
	}

	if r.cdsClient == nil {
		return
	}

	// bind watchers
	r.cdsService.BindWatchers(ctk.cdsWatchers)

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
