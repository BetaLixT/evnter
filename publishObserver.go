package streamNotif

import (
	"fmt"
	"sync"
	"time"
)

type PublishObserver struct {
	publisher  IBatchPublisher
  key        string
	eventQueue chan TracedEvent
	dispatch   *NotificationDispatch
	processingWg sync.WaitGroup
}

var _ INotificationObserver = (*PublishObserver)(nil)

func (obs *PublishObserver) Subscribe(
  dispatch *NotificationDispatch,
) (err error) {
  if (dispatch != nil) {
    obs.dispatch = dispatch
    obs.eventQueue = make(chan TracedEvent)
    go func() {
      obs.processingWg.Add(1)
      obs.processQueue()
      obs.processingWg.Done()
    }()
	  obs.dispatch.Subscribe(obs.key, obs)
  } else {
    err = fmt.Errorf("already subscribed")
  }
  return
}

func (obs *PublishObserver) Unsubscribe() {
  obs.dispatch.Unsubscribe(obs.key)
}

func (obs *PublishObserver) OnNext(
	evnt EventEntity,
	traceparent string,
	tracepart string,
) {
	obs.eventQueue <- TracedEvent{
		Event:       evnt,
		Traceparent: traceparent,
		Tracepart:   tracepart,
	}
}

func (obs *PublishObserver) OnCompleted() {
	close(obs.eventQueue)
	obs.processingWg.Wait()
	obs.publisher.Close()
}

func (obs *PublishObserver) processQueue() {
	ticker := time.NewTicker(200 * time.Millisecond)
	ticker.Stop()
	defer ticker.Stop()
	obs.publisher.Close()

	active := true
	batch := make([]TracedEvent, 0)
	evnt := TracedEvent{}

	for active {
		select {
		case <-ticker.C:	
			if len(batch) != 0 {
				ticker.Stop()
				obs.publisher.PublishBatch(batch)
				batch = make([]TracedEvent, 0)
				// ticker.Reset(200 * time.Millisecond)
			}

		case evnt, active = <-obs.eventQueue:
			if active {
				if len(batch) == 0 {
					ticker.Reset(200 * time.Millisecond)
				}
				batch = append(batch, evnt)
			}
		}
	}
	// publish remaining messages
	if len(batch) != 0 {
		obs.publisher.PublishBatch(batch)
	}
}

func NewPublishObserverAndSubscribe(
	publisher IBatchPublisher,
	dispatch *NotificationDispatch,
) *PublishObserver {
	obs := PublishObserver{
		publisher: publisher,
		key: "PublishObserver",
	}
	obs.Subscribe(dispatch)
	return &obs
}
