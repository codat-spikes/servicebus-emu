using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

var config = new ConfigurationBuilder()
    .AddJsonFile("appsettings.json", optional: true, reloadOnChange: false)
    .Build();

var queues = config.GetSection("Queues").Get<Dictionary<string, QueueOptions>>()
    ?? new Dictionary<string, QueueOptions>();

await using var services = new ServiceCollection()
    .AddLogging(builder =>
    {
        builder.AddConfiguration(config.GetSection("Logging"));
        builder.AddSimpleConsole(o =>
        {
            o.SingleLine = true;
            o.TimestampFormat = "HH:mm:ss.fff ";
        });
    })
    .BuildServiceProvider();

var loggerFactory = services.GetRequiredService<ILoggerFactory>();
var log = loggerFactory.CreateLogger("asbe");

var server = new AmqpServer(queues, loggerFactory);
log.LogInformation("Starting server");
server.Start();

await using var client = new ServiceBusClient(AmqpServer.LocalConnectionString);

log.LogInformation("Sending message to test-queue");
await using var sender = client.CreateSender("test-queue");
await sender.SendMessageAsync(new ServiceBusMessage("Hello world"));

log.LogInformation("Receiving from test-queue");
await using var receiver = client.CreateReceiver("test-queue", new ServiceBusReceiverOptions
{
    ReceiveMode = ServiceBusReceiveMode.PeekLock,
});
var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
if (msg is null)
{
    log.LogWarning("Received: <none>");
}
else
{
    log.LogInformation("Received: {Body}", msg.Body);
    await receiver.CompleteMessageAsync(msg);
}
