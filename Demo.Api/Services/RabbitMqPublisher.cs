using Demo.Shared;
using RabbitMQ.Client;
using System.Text;
using System.Text.Json;

namespace Demo.Api.Services
{
    public class RabbitMqPublisher : IAsyncDisposable
    {
        private readonly IConnection _connection;
        private readonly IChannel _channel;
        private readonly string _queueName;
        public RabbitMqPublisher(IConfiguration config)
        {
            var factory = new ConnectionFactory { HostName = config["RabbitMq:Host"]! };
            _queueName = config["RabbitMq:QueueName"]!;
            _connection = factory.CreateConnectionAsync().GetAwaiter().GetResult();
            _channel = _connection.CreateChannelAsync().GetAwaiter().GetResult();
            // Declare a DURABLE queue (survives broker restart) [citation:1]
            _channel.QueueDeclareAsync(queue: _queueName, durable: true, exclusive: false, autoDelete: false);
        }
        public async Task PublishOrderAsync(OrderMessage message)
        {
            var json = JsonSerializer.Serialize(message);
            var body = Encoding.UTF8.GetBytes(json);
            // Publish as PERSISTENT message (survives broker restart) [citation:1]
            var props = new BasicProperties { Persistent = true };
            await _channel.BasicPublishAsync(
                exchange: string.Empty,
                routingKey: _queueName,
                mandatory: false,
                basicProperties: props,
                body: body);
        }
        public async ValueTask DisposeAsync()
        {
            await _channel.CloseAsync();
            await _connection.CloseAsync();
        }
    }
}
