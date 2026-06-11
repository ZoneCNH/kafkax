package kafkax

type Option func(*options)

type ConsumerFactory func(Subscription) (Consumer, error)

type options struct {
	metrics         Metrics
	producer        Producer
	admin           Admin
	consumerFactory ConsumerFactory
}

func defaultOptions() options {
	return options{
		metrics: NoopMetrics{},
	}
}

func WithMetrics(metrics Metrics) Option {
	return func(o *options) {
		if metrics != nil {
			o.metrics = metrics
		}
	}
}

func WithProducer(producer Producer) Option {
	return func(o *options) {
		o.producer = producer
	}
}

func WithConsumer(consumer Consumer) Option {
	return func(o *options) {
		if consumer != nil {
			o.consumerFactory = func(Subscription) (Consumer, error) {
				return consumer, nil
			}
		}
	}
}

func WithConsumerFactory(factory ConsumerFactory) Option {
	return func(o *options) {
		o.consumerFactory = factory
	}
}

func WithAdmin(admin Admin) Option {
	return func(o *options) {
		o.admin = admin
	}
}
