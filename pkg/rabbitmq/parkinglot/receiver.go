package parkinglot

import (
	"context"
	"fmt"
	"time"

	"github.com/quarks-tech/amqp"
	"github.com/quarks-tech/protoevent-amqp-go/pkg/rabbitmq"
	"github.com/quarks-tech/protoevent-amqp-go/pkg/rabbitmq/message"
	"github.com/rs/xid"
	stdamqp "github.com/streadway/amqp"
	"golang.org/x/sync/errgroup"

	"github.com/quarks-tech/amqp/connpool"
	"github.com/quarks-tech/protoevent-go/pkg/eventbus"
)

const (
	dlxSuffix        = ".dlx"
	waitSuffix       = ".wait"
	parkingLotSuffix = ".pl"
)

type receiverOptions struct {
	incomingQueue   string
	waitQueue       string
	parkingLotQueue string
	dlxExchange     string
	prefetchCount   int
	consumerTag     string
	setupTopology   bool
	setupBindings   bool
	maxRetries      int64
	minRetryBackoff time.Duration
	marshaler       rabbitmq.Marshaler
}

func defaultReceiverOptions() receiverOptions {
	return receiverOptions{
		marshaler:       message.Marshaler{},
		prefetchCount:   3,
		maxRetries:      3,
		minRetryBackoff: time.Second * 15,
	}
}

type ReceiverOption func(o *receiverOptions)

func WithMaxRetries(n int) ReceiverOption {
	return func(o *receiverOptions) {
		o.maxRetries = int64(n)
	}
}

func WithMinRetryBackoff(d time.Duration) ReceiverOption {
	return func(o *receiverOptions) {
		o.minRetryBackoff = d
	}
}

func WithTopologySetup() ReceiverOption {
	return func(o *receiverOptions) {
		o.setupTopology = true
	}
}

func WithBindingsSetup() ReceiverOption {
	return func(o *receiverOptions) {
		o.setupBindings = true
	}
}

func WithPrefetchCount(c int) ReceiverOption {
	return func(o *receiverOptions) {
		o.prefetchCount = c
	}
}

func WithMarshaler(m rabbitmq.Marshaler) ReceiverOption {
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

	return &Receiver{
		client:  client,
		options: options,
	}
}

func (r *Receiver) Setup(ctx context.Context, consumerName string, infos ...eventbus.ServiceInfo) error {
	r.consumerName = consumerName

	if r.options.incomingQueue == "" {
		r.options.incomingQueue = consumerName
	}

	r.options.dlxExchange = r.options.incomingQueue + dlxSuffix
	r.options.waitQueue = r.options.incomingQueue + waitSuffix
	r.options.parkingLotQueue = r.options.incomingQueue + parkingLotSuffix

	r.options.consumerTag = fmt.Sprintf("%s-%s", consumerName, xid.New())

	if !r.options.setupTopology && !r.options.setupBindings {
		return nil
	}

	return r.client.Process(ctx, func(ctx context.Context, conn *connpool.Conn) error {
		if r.options.setupTopology {
			if err := r.setupTopology(conn); err != nil {
				return err
			}
		}

		if r.options.setupBindings {
			if err := r.setupBindings(conn, infos); err != nil {
				return err
			}
		}

		return nil
	})
}

const (
	wait       = "wait"
	retry      = "retry"
	parkingLot = "parkingLot"
)

func (r *Receiver) setupTopology(conn *connpool.Conn) error {
	incomingQueueArgs := stdamqp.Table{
		"x-dead-letter-exchange":    r.options.dlxExchange,
		"x-dead-letter-routing-key": wait,
	}

	waitQueueArgs := stdamqp.Table{
		"x-dead-letter-exchange":    r.options.dlxExchange,
		"x-dead-letter-routing-key": retry,
		"x-message-ttl":             r.options.minRetryBackoff.Milliseconds(),
	}

	err := conn.Channel().ExchangeDeclare(r.options.dlxExchange, stdamqp.ExchangeTopic, true, false, false, false, nil)
	if err != nil {
		return err
	}

	_, err = conn.Channel().QueueDeclare(r.options.waitQueue, true, false, false, false, waitQueueArgs)
	if err != nil {
		return err
	}

	_, err = conn.Channel().QueueDeclare(r.options.parkingLotQueue, true, false, false, false, nil)
	if err != nil {
		return err
	}

	_, err = conn.Channel().QueueDeclare(r.options.incomingQueue, true, false, false, false, incomingQueueArgs)
	if err != nil {
		return err
	}

	if err = conn.Channel().QueueBind(r.options.waitQueue, wait, r.options.dlxExchange, false, nil); err != nil {
		return err
	}

	if err = conn.Channel().QueueBind(r.options.incomingQueue, retry, r.options.dlxExchange, false, nil); err != nil {
		return err
	}

	if err = conn.Channel().QueueBind(r.options.parkingLotQueue, parkingLot, r.options.dlxExchange, false, nil); err != nil {
		return err
	}

	return nil
}

func (r *Receiver) setupBindings(conn *connpool.Conn, infos []eventbus.ServiceInfo) error {
	for _, info := range infos {
		for _, eventName := range info.Events {
			if err := conn.Channel().QueueBind(r.options.incomingQueue, eventName, info.ServiceName, false, nil); err != nil {
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

	deliveries, err := conn.Channel().Consume(r.options.incomingQueue, r.options.consumerTag, false, false, false, false, nil)
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

				if ackErr := r.doAcknowledge(conn, &delivery, err); ackErr != nil {
					return fmt.Errorf("do acknowledge: %w", ackErr)
				}
			}
		}

		return nil
	})

	return eg.Wait()
}

func (r *Receiver) doAcknowledge(conn *connpool.Conn, d *stdamqp.Delivery, err error) error {
	switch {
	case err == nil:
		return d.Ack(false)
	case eventbus.IsUnprocessableEventError(err), hasExceededRetryCount(d, r.options.maxRetries):
		return r.putIntoParkingLot(conn, d)
	default:
		return d.Reject(false)
	}
}

func (r *Receiver) putIntoParkingLot(conn *connpool.Conn, d *stdamqp.Delivery) error {
	msg := stdamqp.Publishing{
		Headers:         d.Headers,
		Type:            d.Type,
		ContentType:     d.ContentType,
		ContentEncoding: d.ContentEncoding,
		DeliveryMode:    d.DeliveryMode,
		Body:            d.Body,
	}

	err := conn.Channel().Publish(r.options.dlxExchange, parkingLot, false, false, msg)
	if err != nil {
		return fmt.Errorf("put into parking lot: %w", err)
	}

	return d.Ack(false)
}

func hasExceededRetryCount(d *stdamqp.Delivery, max int64) bool {
	death, ok := d.Headers["x-death"].([]interface{})
	if !ok {
		return false
	}

	for _, i := range death {
		t := i.(stdamqp.Table)

		if t["queue"] == d.Headers["x-first-death-queue"] {
			return t["count"].(int64) >= max
		}
	}

	return false
}
