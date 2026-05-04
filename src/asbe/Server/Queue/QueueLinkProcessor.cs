using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

sealed class QueueLinkProcessor : ILinkProcessor
{
    private const uint MaxMessageSize = 256 * 1024;

    private readonly QueueStore _queues;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<QueueLinkProcessor> _logger;

    public QueueLinkProcessor(QueueStore queues, ILoggerFactory? loggerFactory = null)
    {
        _queues = queues;
        _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
        _logger = _loggerFactory.CreateLogger<QueueLinkProcessor>();
    }

    public void Process(AttachContext attachContext)
    {
        var attach = attachContext.Attach;
        var address = attach.Role
            ? (attach.Source as Source)?.Address
            : (attach.Target as Target)?.Address;

        _logger.LogInformation("Attach link={Link} role={Role} address={Address}",
            attach.LinkName, attach.Role ? "receiver" : "sender", address);

        if (string.IsNullOrEmpty(address))
        {
            _logger.LogWarning("Rejecting attach link={Link}: address is required", attach.LinkName);
            attachContext.Complete(new Error(ErrorCode.InvalidField) { Description = "Address is required." });
            return;
        }

        attach.MaxMessageSize = MaxMessageSize;
        var endpoint = _queues.Get(address);

        if (!attach.Role)
        {
            attachContext.Complete(new TargetLinkEndpoint(new QueueMessageSink(endpoint, _loggerFactory.CreateLogger<QueueMessageSink>()), attachContext.Link), 100);
        }
        else
        {
            attachContext.Complete(new DrainAwareSourceLinkEndpoint(new QueueMessageSource(endpoint, _loggerFactory.CreateLogger<QueueMessageSource>()), attachContext.Link), 0);
        }
    }
}
