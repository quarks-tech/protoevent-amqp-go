package rabbitmq

import (
	"context"
	"fmt"
	"runtime"

	"github.com/google/uuid"
	"github.com/quarks-tech/amqp"
	"github.com/quarks-tech/protoevent-amqp-go/pkg/rabbitmq/message"
	stdamqp "github.com/streadway/amqp"
	"golang.org/x/sync/errgroup"

	"github.com/quarks-tech/amqp/connpool"
	"github.com/quarks-tech/protoevent-go/pkg/eventbus"
)

const dlxSuffix = ".dlx"

type receiverOptions struct {
	marshaler     Marshaler
	queue         string
	workerCount   int
	prefetchCount int
	consumerTag   string
	setupTopology bool
	enableDLX     bool
}

func defaultReceiverOptions() receiverOptions {
	maxProcs := runtime.GOMAXPROCS(0)

	return receiverOptions{
		marshaler:     message.Marshaler{},
		workerCount:   maxProcs,
		prefetchCount: maxProcs * 3,
	}
}

type ReceiverOption func(o *receiverOptions)

func WithWorkerNum(c int) ReceiverOption {
	return func(o *receiverOptions) {
		o.workerCount = c
	}
}

func WithTopologySetup() ReceiverOption {
	return func(o *receiverOptions) {
		o.setupTopology = true
	}
}

func WithDLX() ReceiverOption {
	return func(o *receiverOptions) {
		o.enableDLX = true
	}
}

func WithPrefetchCount(c int) ReceiverOption {
	return func(o *receiverOptions) {
		o.prefetchCount = c
	}
}

func WithMessageParser(m Marshaler) ReceiverOption {
	return func(opts *receiverOptions) {
		opts.marshaler = m
	}
}

type Receiver struct {
	client       *amqp.Client
	options      receiverOptions
	consumerName string
}

func NewReceiver(client *amqp.Client, opts ...ReceiverOption) *Receiver {
	options := defaultReceiverOptions()

	for _, opt := range opts {
		opt(&options)
	}

	s := &Receiver{
		client:  client,
		options: options,
	}

	return s
}

func (r *Receiver) Setup(ctx context.Context, consumerName string, infos ...eventbus.ServiceInfo) error {
	r.consumerName = consumerName

	if r.options.queue == "" {
		r.options.queue = consumerName
	}

	if r.options.consumerTag == "" {
		r.options.consumerTag = fmt.Sprintf("%s-%s", consumerName, uuid.New().String())
	}

	if !r.options.setupTopology {
		return nil
	}

	return r.client.Process(ctx, func(ctx context.Context, conn *connpool.Conn) error {
		return r.setupTopology(conn, infos)
	})
}

func (r *Receiver) setupTopology(conn *connpool.Conn, infos []eventbus.ServiceInfo) error {
	var queueDeclareArgs stdamqp.Table

	if r.options.enableDLX {
		dlxExchange := r.options.queue + dlxSuffix
		dlxQueue := r.options.queue + dlxSuffix

		queueDeclareArgs = stdamqp.Table{
			"x-dead-letter-exchange": dlxExchange,
		}

		err := conn.Channel().ExchangeDeclare(dlxExchange, stdamqp.ExchangeFanout, true, false, false, false, nil)
		if err != nil {
			return err
		}

		_, err = conn.Channel().QueueDeclare(dlxQueue, true, false, false, false, nil)
		if err != nil {
			return err
		}

		if err = conn.Channel().QueueBind(dlxQueue, "", dlxExchange, false, nil); err != nil {
			return err
		}
	}

	_, err := conn.Channel().QueueDeclare(r.options.queue, true, false, false, false, queueDeclareArgs)
	if err != nil {
		return err
	}

	for _, info := range infos {
		for _, eventName := range info.Events {
			if err = conn.Channel().QueueBind(r.options.queue, eventName, info.ServiceName, false, nil); err != nil {
				return err
			}
		}
	}

	return nil
}

func (r *Receiver) Receive(ctx context.Context, processor eventbus.Processor) error {
	return r.client.Process(ctx, func(ctx context.Context, conn *connpool.Conn) error {
		return r.receive(conn, ctx, processor)
	})
}

func (r *Receiver) receive(conn *connpool.Conn, ctx context.Context, processor eventbus.Processor) error {
	if err := conn.Channel().Qos(r.options.prefetchCount, 0, false); err != nil {
		return err
	}

	deliveries, err := conn.Channel().Consume(r.options.queue, r.options.consumerTag, false, false, false, false, nil)
	if err != nil {
		return err
	}

	eg, egCtx := errgroup.WithContext(context.Background())

	eg.Go(func() error {
		select {
		case <-ctx.Done():
			return conn.Channel().Cancel(r.options.consumerTag, false)
		case <-egCtx.Done():
			return conn.Close()
		case connErr := <-conn.NotifyClose(make(chan *stdamqp.Error)):
			return connErr
		}
	})

	for i := 0; i < r.options.workerCount; i++ {
		eg.Go(func() error {
			for delivery := range deliveries {
				select {
				case <-ctx.Done():
					return nil
				default:
					md, data, err := r.options.marshaler.Unmarshal(&delivery)
					if err == nil {
						err = processor(md, data)
					} else {
						err = eventbus.NewUnprocessableEventError(err)
					}

					if ackErr := doAcknowledge(&delivery, err); ackErr != nil {
						return ackErr
					}
				}
			}

			return nil
		})
	}

	return eg.Wait()
}

func doAcknowledge(m *stdamqp.Delivery, err error) error {
	switch {
	case err == nil:
		return m.Ack(false)
	case eventbus.IsUnprocessableEventError(err):
		return m.Reject(false)
	default:
		return m.Reject(true)
	}
}
