package message

import (
	"github.com/quarks-tech/protoevent-amqp-go/pkg/rabbitmq/message/contentmode/binary"
	"github.com/quarks-tech/protoevent-amqp-go/pkg/rabbitmq/message/contentmode/structured"
	"github.com/quarks-tech/protoevent-go/pkg/event"
	"github.com/streadway/amqp"
)

const structuredContentType = "application/cloudevents+json"

type Marshaler struct {
	structured structured.Marshaler
	binary     binary.Marshaler
}

func (m Marshaler) Marshal(md *event.Metadata, data []byte) (amqp.Publishing, error) {
	if md.DataContentType == structuredContentType {
		return m.structured.Marshal(md, data)
	}

	return m.binary.Marshal(md, data)
}

func (m Marshaler) Unmarshal(d *amqp.Delivery) (*event.Metadata, []byte, error) {
	if d.ContentType == structuredContentType {
		return m.structured.Unmarshal(d)
	}

	return m.binary.Unmarshal(d)
}
