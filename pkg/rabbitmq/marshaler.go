package rabbitmq

import (
	"github.com/quarks-tech/protoevent-go/pkg/event"
	stdamqp "github.com/streadway/amqp"
)

type Marshaler interface {
	Unmarshal(d *stdamqp.Delivery) (*event.Metadata, []byte, error)
	Marshal(md *event.Metadata, data []byte) stdamqp.Publishing
}
