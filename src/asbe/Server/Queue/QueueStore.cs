using System.Collections.Concurrent;

sealed class QueueStore
{
    private readonly ConcurrentDictionary<string, InMemoryQueue> _queues = new();

    public InMemoryQueue Get(string address) => _queues.GetOrAdd(address, _ => new InMemoryQueue());
}
