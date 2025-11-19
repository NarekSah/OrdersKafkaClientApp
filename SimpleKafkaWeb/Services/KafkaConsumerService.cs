using Confluent.Kafka;

using Microsoft.Extensions.Options;

namespace SimpleKafkaWeb.Services
{
    public class KafkaConsumerService : BackgroundService
    {
        private readonly ILogger<KafkaConsumerService> _logger;
        private readonly KafkaSettings _settings;
        private readonly MessageStore _store;

        public KafkaConsumerService(
            ILogger<KafkaConsumerService> logger,
            IOptions<KafkaSettings> options,
            MessageStore store)
        {
            _logger = logger;
            _settings = options.Value;
            _store = store;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = _settings.BootstrapServers,
                GroupId = _settings.GroupId,
                AutoOffsetReset = AutoOffsetReset.Latest,
                EnableAutoCommit = true,
                SecurityProtocol = Enum.Parse<SecurityProtocol>(_settings.SecurityProtocol, ignoreCase: true),
                SaslMechanism = Enum.Parse<SaslMechanism>(_settings.SaslMechanism, ignoreCase: true),
                SaslUsername = _settings.SaslUsername,
                SaslPassword = _settings.SaslPassword,
                ClientId = _settings.ClientId
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();

            consumer.Subscribe(_settings.Topic);

            _logger.LogInformation("Kafka consumer started for topic {Topic}", _settings.Topic);

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        var result = consumer.Consume(stoppingToken);
                        if (result?.Message != null)
                        {
                            var text = $"[{result.TopicPartitionOffset}] {result.Message.Value}";
                            _store.AddMessage(text);
                            _logger.LogInformation("Consumed: {Message}", text);
                        }
                    }
                    catch (ConsumeException ex)
                    {
                        _logger.LogError(ex, "Error while consuming from Kafka");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Kafka consumer stopping...");
            }
            finally
            {
                consumer.Close();
            }
        }
    }

    public class KafkaSettings
    {
        public string BootstrapServers { get; set; } = "";
        public string SaslUsername { get; set; } = "";
        public string SaslPassword { get; set; } = "";
        public string SecurityProtocol { get; set; } = "SaslSsl";
        public string SaslMechanism { get; set; } = "Plain";
        public string GroupId { get; set; } = "simple-web-consumer";
        public string Topic { get; set; } = "";
        public string ClientId { get; set; } = "OrdersKafkaWebApp";
    }
}
