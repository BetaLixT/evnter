package streamNotif

type INotificationObserver interface {
  OnNext(
    evnt EventEntity,
    traceparent string,
    tracepartition string,
  )
  OnCompleted()
}

type IBatchPublisher interface {
  PublishBatch([]TracedEvent)
  Close()
}
