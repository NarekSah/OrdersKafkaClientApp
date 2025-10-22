using Confluent.Kafka;

namespace OrdersKafkaWebApp
{
    public class Consumer : IConsumer
    {
        private readonly IConfiguration _config;

        public Consumer(IConfiguration config)
        {
            _config = config;
        }

        public async Task<OrderMessage?> ConsumeAsync(string topic, CancellationToken cancellationToken = default)
        {
            // Get Kafka configuration section
            var kafkaConfig = new ConsumerConfig();
            _config.GetSection("Kafka").Bind(kafkaConfig);
            
            // Set required consumer properties if not already configured
            if (string.IsNullOrEmpty(kafkaConfig.GroupId))
            {
                kafkaConfig.GroupId = "orders-consumer-group";
            }
            if (kafkaConfig.AutoOffsetReset == null)
            {
                kafkaConfig.AutoOffsetReset = AutoOffsetReset.Earliest;
            }

            // creates a new consumer instance
            using (var consumer = new ConsumerBuilder<string, string>(kafkaConfig).Build())
            {
                try
                {
                    consumer.Subscribe(topic);
                    
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
                finally
                {
                    consumer.Close();
                }
            }
        }

        public async Task ConsumeAsync(string topic, Func<OrderMessage, Task> messageHandler, CancellationToken cancellationToken = default)
        {
            // Get Kafka configuration section
            var kafkaConfig = new ConsumerConfig();
            _config.GetSection("Kafka").Bind(kafkaConfig);
            
            // Set required consumer properties if not already configured
            if (string.IsNullOrEmpty(kafkaConfig.GroupId))
            {
                kafkaConfig.GroupId = "orders-consumer-group";
            }
            if (kafkaConfig.AutoOffsetReset == null)
            {
                kafkaConfig.AutoOffsetReset = AutoOffsetReset.Earliest;
            }

            // creates a new consumer instance
            using (var consumer = new ConsumerBuilder<string, string>(kafkaConfig).Build())
            {
                try
                {
                    consumer.Subscribe(topic);
                    
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        var consumeResult = consumer.Consume(cancellationToken);
                        
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
                finally
                {
                    consumer.Close();
                }
            }
        }
    }
}