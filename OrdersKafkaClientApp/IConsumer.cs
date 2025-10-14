namespace OrdersKafkaClientApp
{
    public interface IConsumer
    {
        Task<OrderMessage?> ConsumeAsync(string topic, CancellationToken cancellationToken = default);
        Task ConsumeAsync(string topic, Func<OrderMessage, Task> messageHandler, CancellationToken cancellationToken = default);
    }
}