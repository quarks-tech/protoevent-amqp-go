package structured

import (
	"github.com/quarks-tech/protoevent-go/pkg/event"
	"github.com/streadway/amqp"
)

type Marshaler struct{}

func (m Marshaler) Marshal(md *event.Metadata, data []byte) amqp.Publishing {
	panic("structured marshal not implemented")
}

func (m Marshaler) Unmarshal(d *amqp.Delivery) (*event.Metadata, []byte, error) {
	panic("structured unmarshal not implemented")
}
