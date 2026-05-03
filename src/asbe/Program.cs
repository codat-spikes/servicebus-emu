var server = new AmqpServer();
var client = new AmqpClient();

Console.WriteLine("Starting server");
server.Start();

Console.WriteLine("Sending message to test-queue");
await client.Send("test-queue", new TestMessage("Hello world"));

Console.WriteLine("Receiving from test-queue");
var body = await client.Receive("test-queue", TimeSpan.FromSeconds(5));
Console.WriteLine($"Received: {body ?? "<none>"}");
