package streamNotif

import (
	"fmt"
	"testing"
	"time"
)

type TestBatchPublisher struct {
	Batches   int
	Count     int
	Completed bool
}

func (b *TestBatchPublisher) PublishBatch(evnts []TracedEvent) {
	for _, evnt := range evnts {
		fmt.Printf("%d: %v\n", b.Batches, evnt)
		b.Count++
	}
	b.Batches++
}

func (b *TestBatchPublisher) Close() {
	b.Completed = true
}

var _ IBatchPublisher = (*TestBatchPublisher)(nil)

func TestNotificationDispatch(t *testing.T) {
	dispatch := NewNotifDispatch()
	batchPub := TestBatchPublisher{}
	_ = NewPublishObserverAndSubscribe(
		&batchPub,
		dispatch,
	)
	n := 2000
	for i := 0; i < n; i++ {
		dispatch.DispatchEventNotification(
			"test",
			"test",
			"test",
			-1,
			nil,
			time.Now(),
			"test",
			"test",
		)
		if i % 5 == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	dispatch.Close()
	if batchPub.Count != n {
		fmt.Printf("count miss match")
		t.Fail()
	}
	if batchPub.Completed == false {
		fmt.Printf("not completed")
		t.Fail()
	}
}
