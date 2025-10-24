namespace OrdersKafkaWebApp
{
    public sealed class KafkaSettings
    {
        public string BootstrapServers { get; set; } = "";
        public string SaslUsername { get; set; } = "";
        public string SaslPassword { get; set; } = "";
        public string Topic { get; set; } = "";
        public string GroupId { get; set; } = "kafka-viewer-group";
        public string AutoOffsetReset { get; set; } = "Latest"; // "Earliest" or "Latest"
    }
}
