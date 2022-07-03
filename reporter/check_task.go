package reporter

import (
	"context"
	"github.com/SkyAPM/go2sky/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/metadata"
	managementv3 "skywalking.apache.org/repo/goapi/collect/management/v3"
	"time"
)

type checkTasks struct {
	service          string
	serviceInstance  string
	conn             *grpc.ClientConn
	checkInterval    time.Duration
	managementClient managementv3.ManagementServiceClient
	logger           logger.Log
	// Instance belong layer name which define in the backend
	layer                   string
	processStatusHookEnable bool
}

// Run check
func (ctk *checkTasks) Run(ctx context.Context) {

	if ctk.checkInterval < 0 || ctk.conn == nil || ctk.managementClient == nil {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			instancePropertiesSubmitted := false
			for {
				if ctk.conn.GetState() == connectivity.Shutdown {
					break
				}

				// report the process status
				if ctk.processStatusHookEnable {
					// TODO: reportProcess()是谁的方法
					reportProcess(r)
				}

				if !instancePropertiesSubmitted {
					// TODO: reportInstanceProperties是谁的方法
					err := r.reportInstanceProperties()
					if err != nil {
						ctk.logger.Errorf("report serviceInstance properties error %v", err)
						time.Sleep(ctk.checkInterval)
						continue
					}
					instancePropertiesSubmitted = true
				}

				_, err := ctk.managementClient.KeepAlive(metadata.NewOutgoingContext(context.Background(), r.md), &managementv3.InstancePingPkg{
					Service:         ctk.service,
					ServiceInstance: ctk.serviceInstance,
					Layer:           ctk.layer,
				})

				if err != nil {
					ctk.logger.Errorf("send keep alive signal error %v", err)
				}
				time.Sleep(ctk.checkInterval)
			}
		}
	}
}
