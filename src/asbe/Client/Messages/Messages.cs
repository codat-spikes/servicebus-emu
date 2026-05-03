using Azure.Messaging.ServiceBus;

sealed class TestMessage(string content) : ServiceBusMessage(content);
