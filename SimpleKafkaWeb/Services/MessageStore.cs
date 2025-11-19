using System.Collections.Concurrent;

namespace SimpleKafkaWeb.Services
{
    public class MessageStore
    {
        private readonly ConcurrentQueue<string> _messages = new();
        private readonly int _maxMessages;

        public MessageStore(int maxMessages = 50)
        {
            _maxMessages = maxMessages;
        }

        public void AddMessage(string message)
        {
            _messages.Enqueue(message);

            while (_messages.Count > _maxMessages && _messages.TryDequeue(out _)) { }
        }

        public IReadOnlyCollection<string> GetMessages()
        {
            return _messages.ToArray();
        }
    }
}
