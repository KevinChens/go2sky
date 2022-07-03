package reporter

import (
	"context"
	"github.com/SkyAPM/go2sky/logger"
	"google.golang.org/grpc/metadata"
	agentv3 "skywalking.apache.org/repo/goapi/collect/language/agent/v3"
	"time"
)

type pplTasks struct {
	traceClient agentv3.TraceSegmentReportServiceClient
	sendCh      chan *agentv3.SegmentObject
	logger      logger.Log
}

// Run send pipeline
func (ptk *pplTasks) Run(ctx context.Context) {

	if ptk.traceClient == nil {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		StreamLoop:
			for {
				stream, err := ptk.traceClient.Collect(metadata.NewOutgoingContext(context.Background(), r.md))
				if err != nil {
					ptk.logger.Errorf("open stream error %v", err)
					time.Sleep(5 * time.Second)
					continue StreamLoop
				}
				for s := range ptk.sendCh {
					err = stream.Send(s)
					if err != nil {
						ptk.logger.Errorf("send segment error %v", err)
						// TODO: closeStream()是谁的方法
						ptk.closeStream(stream)
						continue StreamLoop
					}
				}
				// TODO: closeStream()是谁的方法
				ptk.closeStream(stream)
				// TODO: closeGRPCConn()是谁的方法
				ptk.closeGRPCConn()
				break
			}
		}
	}
}
