using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using Amqp.Types;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

sealed class QueueLinkProcessor : ILinkProcessor
{
    private const uint MaxMessageSize = 256 * 1024;
    private static readonly Symbol SessionFilterName = "com.microsoft:session-filter";
    private static readonly Symbol LockedUntilUtc = "com.microsoft:locked-until-utc";

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
            return;
        }

        if (TryReadSessionFilter(attach, out var hasFilter, out var requestedSessionId) && hasFilter)
        {
            AttachSessionReceiver(attachContext, address, endpoint, requestedSessionId);
            return;
        }

        attachContext.Complete(new DrainAwareSourceLinkEndpoint(new QueueMessageSource(endpoint, _loggerFactory.CreateLogger<QueueMessageSource>()), attachContext.Link), 0);
    }

    private void AttachSessionReceiver(AttachContext attachContext, string address, IQueueEndpoint endpoint, string? requestedSessionId)
    {
        var attach = attachContext.Attach;
        if (endpoint is not InMemoryQueue queue)
        {
            attachContext.Complete(new Error(ErrorCode.NotAllowed) { Description = $"Session filter not supported on '{address}'." });
            return;
        }

        Session session;
        if (requestedSessionId is null)
        {
            // Next-available-session: pick any session that has messages and isn't locked.
            // If none are available, reject the attach so the SDK retries / surfaces a timeout.
            var candidate = queue.Sessions.TryFindUnlockedWithMessages();
            if (candidate is null)
            {
                attachContext.Complete(new Error(ErrorCode.NotFound) { Description = "No unlocked session with pending messages." });
                return;
            }
            session = candidate;
        }
        else
        {
            session = queue.Sessions.GetOrCreate(requestedSessionId);
        }

        if (!session.TryAcquireLock(out var lockToken, out var lockedUntil))
        {
            attachContext.Complete(new Error(ErrorCode.ResourceLocked) { Description = $"Session '{session.Id}' is already locked." });
            return;
        }

        // Echo the session-id back in the source filter set and stamp the lock expiry into
        // the link properties — the SB SDK reads both of these from the response attach.
        if (attach.Source is Source source)
        {
            source.FilterSet ??= new Map();
            source.FilterSet[SessionFilterName] = session.Id;
        }
        attach.Properties ??= new Fields();
        attach.Properties[LockedUntilUtc] = lockedUntil.Ticks;

        var sessionEndpoint = new SessionEndpoint(queue, session);
        var src = new QueueMessageSource(sessionEndpoint, _loggerFactory.CreateLogger<QueueMessageSource>());
        var listenerLink = attachContext.Link;
        listenerLink.AddClosedCallback((_, _) =>
        {
            session.Release(lockToken);
            _logger.LogInformation("Session receiver detached id={SessionId}", session.Id);
        });
        _logger.LogInformation("Session receiver attached id={SessionId} lockedUntil={LockedUntil:O}", session.Id, lockedUntil);
        attachContext.Complete(new DrainAwareSourceLinkEndpoint(src, listenerLink), 0);
    }

    private static bool TryReadSessionFilter(Attach attach, out bool hasFilter, out string? sessionId)
    {
        hasFilter = false;
        sessionId = null;
        if (attach.Source is not Source source || source.FilterSet is null) return true;
        if (!source.FilterSet.ContainsKey(SessionFilterName)) return true;
        hasFilter = true;
        var value = source.FilterSet[SessionFilterName];
        sessionId = value switch
        {
            null => null,
            string s => s,
            DescribedValue d => d.Value as string,
            _ => null,
        };
        return true;
    }
}
