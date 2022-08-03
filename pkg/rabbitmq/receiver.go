package rabbitmq

import (
	"context"
	"fmt"

	"github.com/quarks-tech/amqp"
	"github.com/quarks-tech/protoevent-amqp-go/pkg/rabbitmq/message"
	"github.com/rs/xid"
	stdamqp "github.com/streadway/amqp"
	"golang.org/x/sync/errgroup"

	"github.com/quarks-tech/amqp/connpool"
	"github.com/quarks-tech/protoevent-go/pkg/eventbus"
)

const dlxSuffix = ".dlx"

type receiverOptions struct {
	marshaler      Marshaler
	queue          string
	prefetchCount  int
	consumerTag    string
	setupTopology  bool
	enableDLX      bool
	requeueOnError bool
}

func defaultReceiverOptions() receiverOptions {
	return receiverOptions{
		marshaler:     message.Marshaler{},
		prefetchCount: 3,
	}
}

type ReceiverOption func(o *receiverOptions)

func WithTopologySetup() ReceiverOption {
	return func(o *receiverOptions) {
		o.setupTopology = true
	}
}

func WithRequeue() ReceiverOption {
	return func(o *receiverOptions) {
		o.requeueOnError = true
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

func WithMarshaler(m Marshaler) ReceiverOption {
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

	r.options.consumerTag = fmt.Sprintf("%s-%s", consumerName, xid.New())

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
		return r.receive(ctx, conn, processor)
	})
}

func (r *Receiver) receive(ctx context.Context, conn *connpool.Conn, processor eventbus.Processor) error {
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

	eg.Go(func() error {
		for delivery := range deliveries {
			select {
			case <-egCtx.Done():
				return nil
			default:
				md, data, err := r.options.marshaler.Unmarshal(&delivery)
				if err == nil {
					err = processor(md, data)
				} else {
					err = eventbus.NewUnprocessableEventError(err)
				}

				if ackErr := doAcknowledge(&delivery, err, r.options.requeueOnError); ackErr != nil {
					return ackErr
				}
			}
		}

		return nil
	})

	return eg.Wait()
}

func doAcknowledge(m *stdamqp.Delivery, err error, requeueOnError bool) error {
	switch {
	case err == nil:
		return m.Ack(false)
	case eventbus.IsUnprocessableEventError(err):
		return m.Reject(false)
	default:
		return m.Reject(requeueOnError)
	}
}
