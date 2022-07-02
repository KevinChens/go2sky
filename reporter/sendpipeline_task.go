package reporter

import (
	"context"
	"google.golang.org/grpc/metadata"
	agentv3 "skywalking.apache.org/repo/goapi/collect/language/agent/v3"
	"time"
)

type pplTasks struct {
	traceClient agentv3.TraceSegmentReportServiceClient
	sendCh      chan *agentv3.SegmentObject
}

// Run send pipeline
func (ptk *pplTasks) Run(ctx context.Context) {

	r := &gRPCReporter{
		traceClient: ptk.traceClient,
		sendCh:      ptk.sendCh,
	}

	if r.traceClient == nil {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		StreamLoop:
			for {
				stream, err := r.traceClient.Collect(metadata.NewOutgoingContext(context.Background(), r.md))
				if err != nil {
					r.logger.Errorf("open stream error %v", err)
					time.Sleep(5 * time.Second)
					continue StreamLoop
				}
				for s := range r.sendCh {
					err = stream.Send(s)
					if err != nil {
						r.logger.Errorf("send segment error %v", err)
						r.closeStream(stream)
						continue StreamLoop
					}
				}
				r.closeStream(stream)
				r.closeGRPCConn()
				break
			}
		}
	}
}
