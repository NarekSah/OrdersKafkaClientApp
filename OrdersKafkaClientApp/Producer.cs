using Confluent.Kafka;

namespace OrdersKafkaClientApp
{
    public class Producer : IProducer
    {
        private readonly IConfiguration _config;

        public Producer(IConfiguration config)
        {
            _config = config;
        }

        public async Task ProduceAsync(string topic, OrderMessage message)
        {
            // Get Kafka configuration section
            var kafkaConfig = new ProducerConfig();
            _config.GetSection("Producer").Bind(kafkaConfig);
            
            // creates a new producer instance
            using (var producer = new ProducerBuilder<string, string>(kafkaConfig).Build())
            {     
                try
                {
                    // produces a sample message to the user-created topic and waits for the result
                    var deliveryReport = await producer.ProduceAsync(topic, 
                        new Message<string, string> { Key = message.Key, Value = message.Value });

                    Console.WriteLine($"Produced event to topic {topic}: key = {deliveryReport.Message.Key,-10} value = {deliveryReport.Message.Value}");
                }
                catch (ProduceException<string, string> e)
                {
                    Console.WriteLine($"Failed to deliver message: {e.Error.Reason}");
                    throw;
                }
                finally
                {
                    // send any outstanding or buffered messages to the Kafka broker
                    producer.Flush(TimeSpan.FromSeconds(10));
                }
            }
        }
    }
}
