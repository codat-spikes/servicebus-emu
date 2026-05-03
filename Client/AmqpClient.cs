using Azure.Messaging.ServiceBus;

sealed class AmqpClient
{
    private readonly ServiceBusClient _client;
    private const string _connString = "Endpoint=sb://localhost;SharedAccessKeyName=dev;SharedAccessKey=dev;UseDevelopmentEmulator=true;";

    public AmqpClient()
    {
        _client = new ServiceBusClient(_connString);
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
}
