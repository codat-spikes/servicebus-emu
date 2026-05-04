using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;

var config = new ConfigurationBuilder()
    .AddJsonFile("appsettings.json", optional: true, reloadOnChange: false)
    .Build();

var queues = config.GetSection("Queues").Get<Dictionary<string, QueueOptions>>()
    ?? new Dictionary<string, QueueOptions>();

var server = new AmqpServer(queues);
Console.WriteLine("Starting server");
server.Start();

await using var client = new ServiceBusClient(AmqpServer.LocalConnectionString);

Console.WriteLine("Sending message to test-queue");
await using var sender = client.CreateSender("test-queue");
await sender.SendMessageAsync(new ServiceBusMessage("Hello world"));

Console.WriteLine("Receiving from test-queue");
await using var receiver = client.CreateReceiver("test-queue", new ServiceBusReceiverOptions
{
    ReceiveMode = ServiceBusReceiveMode.PeekLock,
});
var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
if (msg is null)
{
    Console.WriteLine("Received: <none>");
}
else
{
    Console.WriteLine($"Received: {msg.Body}");
    await receiver.CompleteMessageAsync(msg);
}
