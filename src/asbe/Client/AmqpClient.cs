using Azure.Messaging.ServiceBus;

sealed class AmqpClient
{
    private readonly ServiceBusClient _client;
    public const string LocalConnectionString = "Endpoint=sb://localhost;SharedAccessKeyName=dev;SharedAccessKey=dev;UseDevelopmentEmulator=true;";

    public AmqpClient(string? connectionString = null)
    {
        _client = new ServiceBusClient(connectionString ?? LocalConnectionString);
    }

    public async Task Send(string queue, ServiceBusMessage message)
    {
        var sender = _client.CreateSender(queue);
        var batch = await sender.CreateMessageBatchAsync();
        batch.TryAddMessage(message);
        await sender.SendMessagesAsync(batch);
        await sender.DisposeAsync();
    }

    public async Task<string?> Receive(string queue, TimeSpan timeout)
    {
        var receiver = _client.CreateReceiver(queue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete,
        });
        var msg = await receiver.ReceiveMessageAsync(timeout);
        await receiver.DisposeAsync();
        return msg?.Body.ToString();
    }

    public ServiceBusReceiver CreateReceiver(string queue) =>
        _client.CreateReceiver(queue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
        });

    public async Task Drain(string queue)
    {
        var receiver = _client.CreateReceiver(queue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete,
        });
        while (await receiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(200)) is not null) { }
        await receiver.DisposeAsync();
    }
}
