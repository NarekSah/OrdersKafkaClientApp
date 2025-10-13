namespace OrdersKafkaClientApp
{
    public interface IProducer
    {
        Task ProduceAsync(string topic, OrderMessage message);
    }
}