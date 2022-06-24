package streamNotif

import (
	"encoding/json"
	"fmt"

	"github.com/streadway/amqp"
)

type RabbitMQBatchPublisher struct {
	ch   *amqp.Channel
	optn *RabbitMQBatchPublisherOptions
}

func (b *RabbitMQBatchPublisher) PublishBatch(evnts []TracedEvent) {
	for _, evnt := range evnts {
	  json, err := json.Marshal(evnt.Event)
	  if err != nil {
	    // TODO log
	    continue
	  }
	  b.ch.Publish(
	    b.optn.ExchangeName,
	    fmt.Sprintf(
	      "%s.%s.%s",
	      b.optn.ServiceName,
	      evnt.Event.Stream,
	      evnt.Event.Event,
	    ),
	    true,
	    false,
	    amqp.Publishing{
	      ContentType: "application/json",
	      Body: []byte(json),
	    },
	  )
	}
}

func (b *RabbitMQBatchPublisher) Close() {
	b.ch.Close()
}

var _ IBatchPublisher = (*RabbitMQBatchPublisher)(nil)

func NewRabbitMQBatchPublisher(
	conn *amqp.Connection,
	optn *RabbitMQBatchPublisherOptions,
) *RabbitMQBatchPublisher {
	ch, err := conn.Channel()
	if err != nil {
		panic(fmt.Errorf("error creating channel: %w", err))
	}

	err = ch.ExchangeDeclare(
		optn.ExchangeName,
		optn.ExchangeType,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(fmt.Errorf("error declaring exchange: %w", err))
	}
	return &RabbitMQBatchPublisher{
		ch:   ch,
		optn: optn,
	}
}
