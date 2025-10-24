using Confluent.Kafka;

using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Options;

using OrdersKafkaWebApp;

public class KafkaConsumerService : BackgroundService
{
    private readonly KafkaSettings _settings;
    private readonly IHubContext<MessageHub> _hub;
    private readonly ILogger<KafkaConsumerService> _logger;

    public KafkaConsumerService(
        IOptions<KafkaSettings> settings,
        IHubContext<MessageHub> hub,
        ILogger<KafkaConsumerService> logger)
    {
        _settings = settings.Value;
        _hub = hub;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {

        if(string.IsNullOrEmpty(_settings.SaslUsername) || string.IsNullOrEmpty(_settings.SaslPassword))
        {
            _logger.LogError("Kafka SASL username or password is not set. Please check your configuration.");
            return;
        }

        var conf = new ConsumerConfig
        {
            BootstrapServers = _settings?.BootstrapServers,
            GroupId = _settings?.GroupId,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = _settings?.SaslUsername,
            SaslPassword = _settings?.SaslPassword,
            EnableAutoCommit = true,
            AutoOffsetReset = ParseOffsetReset(_settings?.AutoOffsetReset)
        };

        using var consumer = new ConsumerBuilder<Ignore, string>(conf)
            .SetErrorHandler((_, e) => { 
                _logger.LogError("Kafka error: {Reason}", e.Reason);                
            })
            .Build();

        consumer.Subscribe(_settings.Topic);

        _logger.LogInformation("Kafka consumer started. Subscribed to topic: {Topic}", _settings.Topic);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var cr = consumer.Consume(stoppingToken);
                    if (cr is null) continue;

                    // Push just the message value to all connected clients
                    await _hub.Clients.All.SendAsync("message", cr.Message.Value, stoppingToken);

                    // Optional: log meta
                    _logger.LogDebug("Consumed {TopicPartitionOffset}", cr.TopicPartitionOffset);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "ConsumeException: {Reason}", ex.Error.Reason);
                }
                catch (OperationCanceledException)
                {
                    // shutting down
                }
            }
        }
        finally
        {
            try
            {
                consumer.Close();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error during consumer.Close()");
            }
        }
    }

    private static AutoOffsetReset ParseOffsetReset(string? s) =>
        string.Equals(s, "Earliest", StringComparison.OrdinalIgnoreCase)
            ? AutoOffsetReset.Earliest
            : AutoOffsetReset.Latest;
}
