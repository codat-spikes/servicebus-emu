using Azure.Messaging.ServiceBus;

var server = new AmqpServer();
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
