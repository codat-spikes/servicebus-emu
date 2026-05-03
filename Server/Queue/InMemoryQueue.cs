using System.Collections.Concurrent;
using Amqp;

sealed class InMemoryQueue
{
    private readonly ConcurrentQueue<Message> _messages = new();
    private readonly ConcurrentQueue<TaskCompletionSource<Message>> _waiters = new();

    public void Enqueue(Message message)
    {
        while (_waiters.TryDequeue(out var waiter))
        {
            if (waiter.TrySetResult(message)) return;
        }
        _messages.Enqueue(message);
    }

    public Task<Message> DequeueAsync()
    {
        if (_messages.TryDequeue(out var msg)) return Task.FromResult(msg);
        var tcs = new TaskCompletionSource<Message>(TaskCreationOptions.RunContinuationsAsynchronously);
        _waiters.Enqueue(tcs);
        return tcs.Task;
    }
}
