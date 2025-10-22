using Confluent.Kafka;

namespace OrdersKafkaWebApp
{
    public class Consumer : IConsumer, IDisposable
    {
        private readonly IConfiguration _config;
        private IConsumer<string, string>? _consumer;
        private readonly object _lock = new object();

        public Consumer(IConfiguration config)
        {
            _config = config;
        }

        public async Task<OrderMessage?> ConsumeAsync(string topic, CancellationToken cancellationToken = default)
        {
            var consumer = GetOrCreateConsumer();

            try
            {
                var consumeResult = consumer.Consume(cancellationToken);

                if (consumeResult?.Message != null)
                {
                    Console.WriteLine($"Consumed event from topic {topic}: key = {consumeResult.Message.Key,-10} value = {consumeResult.Message.Value}");

                    return new OrderMessage
                    {
                        Key = consumeResult.Message.Key,
                        Value = consumeResult.Message.Value
                    };
                }

                return null;
            }
            catch (ConsumeException e)
            {
                Console.WriteLine($"Failed to consume message: {e.Error.Reason}");
                throw;
            }
        }

        public async Task ConsumeAsync(string topic, Func<OrderMessage, Task> messageHandler, CancellationToken cancellationToken = default)
        {
            var consumer = GetOrCreateConsumer();
            consumer.Subscribe(topic);

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(100));

                    if (consumeResult?.Message != null)
                    {
                        Console.WriteLine($"Consumed event from topic {topic}: key = {consumeResult.Message.Key,-10} value = {consumeResult.Message.Value}");

                        var orderMessage = new OrderMessage
                        {
                            Key = consumeResult.Message.Key,
                            Value = consumeResult.Message.Value
                        };

                        await messageHandler(orderMessage);
                    }
                }
            }
            catch (ConsumeException e)
            {
                Console.WriteLine($"Failed to consume message: {e.Error.Reason}");
                throw;
            }
        }

        private IConsumer<string, string> GetOrCreateConsumer()
        {
            lock (_lock)
            {
                if (_consumer == null)
                {
                    var kafkaConfig = new ConsumerConfig();
                    _config.GetSection("Kafka").Bind(kafkaConfig);

                    // Set required consumer properties
                    if (string.IsNullOrEmpty(kafkaConfig.GroupId))
                    {
                        kafkaConfig.GroupId = $"orders-consumer-group-{Guid.NewGuid()}";
                    }
                    if (kafkaConfig.AutoOffsetReset == null)
                    {
                        kafkaConfig.AutoOffsetReset = AutoOffsetReset.Latest;
                    }

                    _consumer = new ConsumerBuilder<string, string>(kafkaConfig).Build();
                }

                return _consumer;
            }
        }

        public void Dispose()
        {
            _consumer?.Close();
            _consumer?.Dispose();
        }
    }
}