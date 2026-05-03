using System.Collections.Concurrent;

static class QueueStore
{
    private static readonly ConcurrentDictionary<string, InMemoryQueue> _queues = new();

    public static InMemoryQueue Get(string address) => _queues.GetOrAdd(address, _ => new InMemoryQueue());
}
