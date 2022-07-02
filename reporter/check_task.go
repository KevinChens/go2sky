package reporter

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/metadata"
	managementv3 "skywalking.apache.org/repo/goapi/collect/management/v3"
	"time"
)

type checkTasks struct {
	conn             *grpc.ClientConn
	checkInterval    time.Duration
	managementClient managementv3.ManagementServiceClient
}

// Run check
func (ctk *checkTasks) Run(ctx context.Context) {

	r := &gRPCReporter{
		conn:             ctk.conn,
		checkInterval:    ctk.checkInterval,
		managementClient: ctk.managementClient,
	}

	if r.checkInterval < 0 || r.conn == nil || r.managementClient == nil {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			instancePropertiesSubmitted := false
			for {
				if r.conn.GetState() == connectivity.Shutdown {
					break
				}

				// report the process status
				if r.processStatusHookEnable {
					reportProcess(r)
				}

				if !instancePropertiesSubmitted {
					err := r.reportInstanceProperties()
					if err != nil {
						r.logger.Errorf("report serviceInstance properties error %v", err)
						time.Sleep(r.checkInterval)
						continue
					}
					instancePropertiesSubmitted = true
				}

				_, err := r.managementClient.KeepAlive(metadata.NewOutgoingContext(context.Background(), r.md), &managementv3.InstancePingPkg{
					Service:         r.service,
					ServiceInstance: r.serviceInstance,
					Layer:           r.layer,
				})

				if err != nil {
					r.logger.Errorf("send keep alive signal error %v", err)
				}
				time.Sleep(r.checkInterval)
			}
		}
	}
}
