package reporter

import (
	"context"
	"github.com/SkyAPM/go2sky"
	"github.com/SkyAPM/go2sky/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	configuration "skywalking.apache.org/repo/goapi/collect/agent/configuration/v3"
	"time"
)

type cdsTask struct {
	service     string
	conn        *grpc.ClientConn
	cdsClient   configuration.ConfigurationDiscoveryServiceClient
	cdsService  *go2sky.ConfigDiscoveryService
	cdsWatchers []go2sky.AgentConfigChangeWatcher
	cdsInterval time.Duration
	logger      logger.Log
}

// Run fetch config
// need parameter: r.conn, r.cdsClient, ctk.cdsWatchers
func (ctk *cdsTask) Run(ctx context.Context) {
	if ctk.cdsClient == nil {
		return
	}

	// bind watchers
	ctk.cdsService.BindWatchers(ctk.cdsWatchers)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			for {
				if ctk.conn.GetState() == connectivity.Shutdown {
					break
				}

				configurations, err := ctk.cdsClient.FetchConfigurations(context.Background(), &configuration.ConfigurationSyncRequest{
					Service: ctk.service,
					Uuid:    ctk.cdsService.UUID,
				})

				if err != nil {
					ctk.logger.Errorf("fetch dynamic configuration error %v", err)
					time.Sleep(ctk.cdsInterval)
					continue
				}

				if len(configurations.GetCommands()) > 0 && configurations.GetCommands()[0].Command == "ConfigurationDiscoveryCommand" {
					command := configurations.GetCommands()[0]
					ctk.cdsService.HandleCommand(command)
				}

				time.Sleep(ctk.cdsInterval)
			}
		}
	}
}
