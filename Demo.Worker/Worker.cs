using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;
using Demo.Shared;

namespace Demo.Worker
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IConfiguration _config;
        private IConnection? _connection;
        private IChannel? _channel;
        public Worker(ILogger<Worker> logger, IConfiguration config)
        {
            _logger = logger;
            _config = config;
        }
        public override async Task StartAsync(CancellationToken cancellationToken)
        {
            var factory = new ConnectionFactory { HostName = _config["RabbitMq:Host"]! };
            _connection = await factory.CreateConnectionAsync();
            _channel = await _connection.CreateChannelAsync();
            var queueName = _config["RabbitMq:QueueName"]!;
            // Must match the API's declaration (durable = true)
            await _channel.QueueDeclareAsync(queue: queueName, durable: true, exclusive: false, autoDelete: false);
            // Set prefetch to avoid flooding the worker [citation:12]
            await _channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);
            var consumer = new AsyncEventingBasicConsumer(_channel);
            consumer.ReceivedAsync += async (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                try
                {
                    var order = JsonSerializer.Deserialize<OrderMessage>(message);
                    _logger.LogInformation("Processing order {OrderId} for {Email}", order?.OrderId, order?.CustomerEmail);
                    // Simulate work (e.g., send email)
                    await Task.Delay(2000);
                    // Manual Ack: Only remove from queue after successful processing [citation:1]
                    await _channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
                    _logger.LogInformation("Order processed successfully.");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to process message");
                    // Nack: Requeue=false sends to dead letter if configured, or drops it
                    await _channel.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false);
                }
            };
            // autoAck: false is CRITICAL for reliability [citation:1]
            await _channel.BasicConsumeAsync(queue: queueName, autoAck: false, consumer: consumer);
            await base.StartAsync(cancellationToken);
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            // The consumer is already started in StartAsync. Just keep the service alive.
            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(1000, stoppingToken);
            }
        }
        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            if (_channel != null) await _channel.CloseAsync();
            if (_connection != null) await _connection.CloseAsync();
            await base.StopAsync(cancellationToken);
        }
    }
}
