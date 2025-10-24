using Confluent.Kafka;

namespace OrdersKafkaWebApp
{
    public class Consumer : IConsumer, IDisposable
    {
        private readonly IConfiguration _config;
        private readonly ILogger<Consumer> _logger;
        private IConsumer<string, string>? _consumer;
        private readonly object _lock = new object();

        public Consumer(IConfiguration config, ILogger<Consumer> logger)
        {
            _config = config;
            _logger = logger;
        }

        public async Task<OrderMessage?> ConsumeAsync(string topic, CancellationToken cancellationToken = default)
        {
            var consumer = GetOrCreateConsumer();
            consumer.Subscribe(topic);

            try
            {
                var consumeResult = consumer.Consume(cancellationToken);

                if (consumeResult?.Message != null)
                {
                    _logger.LogInformation($"Consumed event from topic {topic}: key = {consumeResult.Message.Key}, value = {consumeResult.Message.Value}");

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
                _logger.LogError($"Failed to consume message: {e.Error.Reason}");
                throw;
            }
        }

        public async Task ConsumeAsync(string topic, Func<OrderMessage, Task> messageHandler, CancellationToken cancellationToken = default)
        {
            var consumer = GetOrCreateConsumer();

            _logger.LogInformation($"Subscribing to topic: {topic}");
            consumer.Subscribe(topic);

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(1000)); // Increased timeout

                        if (consumeResult?.Message != null)
                        {
                            _logger.LogInformation($"Consumed event from topic {topic}: key = {consumeResult.Message.Key}, value = {consumeResult.Message.Value}");

                            var orderMessage = new OrderMessage
                            {
                                Key = consumeResult.Message.Key,
                                Value = consumeResult.Message.Value
                            };

                            await messageHandler(orderMessage);
                        }
                        else
                        {
                            // Log periodically when no messages
                            if (DateTime.Now.Second % 10 == 0) // Every 10 seconds
                            {
                                _logger.LogDebug($"No messages received from topic {topic}");
                            }
                        }
                    }
                    catch (ConsumeException e)
                    {
                        _logger.LogError($"Consume error: {e.Error.Reason}");
                        // Continue consuming even on errors
                    }
                }
                consumer.Close();
            }
            catch (Exception e)
            {
                _logger.LogError(e, $"Fatal error in consumer for topic {topic}");
                throw;
            }
        }

        private IConsumer<string, string> GetOrCreateConsumer()
        {            
                if (_consumer == null)
                {
                    var kafkaConfig = new ConsumerConfig();
                    _config.GetSection("Kafka").Bind(kafkaConfig);

                    // Set required consumer properties
                    if (string.IsNullOrEmpty(kafkaConfig.GroupId))
                    {
                        kafkaConfig.GroupId = $"orders-consumer-group-{Environment.MachineName}-{Guid.NewGuid().ToString("N")[..8]}";
                    }

                    // Change to Earliest to see existing messages
                    kafkaConfig.AutoOffsetReset = AutoOffsetReset.Earliest;

                    _logger.LogInformation($"Creating Kafka consumer with GroupId: {kafkaConfig.GroupId}");
                    _logger.LogInformation($"Bootstrap servers: {kafkaConfig.BootstrapServers}");

                    _consumer = new ConsumerBuilder<string, string>(kafkaConfig)
                        .SetErrorHandler((_, e) => _logger.LogError($"Kafka error: {e.Reason}"))
                        .SetStatisticsHandler((_, json) => _logger.LogDebug($"Kafka stats: {json}"))
                        .Build();

                    _logger.LogInformation("Kafka consumer created successfully");
                }

                return _consumer;            
        }

        public void Dispose()
        {
            _logger.LogInformation("Disposing Kafka consumer");
            _consumer?.Close();
            _consumer?.Dispose();
        }
    }
}