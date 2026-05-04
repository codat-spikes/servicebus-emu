using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using Amqp.Transactions;
using Amqp.Types;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

sealed class QueueLinkProcessor : ILinkProcessor
{
    private const uint MaxMessageSize = 256 * 1024;
    private static readonly Symbol SessionFilterName = "com.microsoft:session-filter";
    private static readonly Symbol LockedUntilUtc = "com.microsoft:locked-until-utc";
    // SB-specific error condition the Azure SDK maps to ServiceBusFailureReason.SessionCannotBeLocked.
    // Without this, the SDK falls back to GeneralError.
    private static readonly Symbol SessionCannotBeLockedError = "com.microsoft:session-cannot-be-locked";
    // SB sends the operation timeout as a uint (ms) on attach.Properties; we use it as
    // the bound for "next available session" waits. Real Service Bus blocks for the full
    // duration before signalling timeout to the SDK.
    private static readonly Symbol OperationTimeout = "com.microsoft:timeout";
    private static readonly TimeSpan DefaultNextSessionTimeout = TimeSpan.FromSeconds(60);

    private readonly QueueStore _queues;
    private readonly TxnManager _txnManager;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<QueueLinkProcessor> _logger;

    public QueueLinkProcessor(QueueStore queues, TxnManager txnManager, ILoggerFactory? loggerFactory = null)
    {
        _queues = queues;
        _txnManager = txnManager;
        _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
        _logger = _loggerFactory.CreateLogger<QueueLinkProcessor>();
    }

    public void Process(AttachContext attachContext)
    {
        var attach = attachContext.Attach;

        // Transaction coordinator attach: client wants to declare/discharge txns.
        // Hand it a sink over the shared TxnManager.
        if (!attach.Role && attach.Target is Coordinator)
        {
            _logger.LogInformation("Attach link={Link} role=sender target=coordinator", attach.LinkName);
            attach.MaxMessageSize = MaxMessageSize;
            var scope = _txnManager.CreateScope();
            var coordinatorSink = new CoordinatorMessageSink(scope, _loggerFactory.CreateLogger<CoordinatorMessageSink>());
            attachContext.Link.AddClosedCallback((_, _) => scope.Rollback());
            attachContext.Complete(new TargetLinkEndpoint(coordinatorSink, attachContext.Link), 100);
            return;
        }

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
        var resolution = _queues.Get(address);

        if (resolution.Error is { } error)
        {
            _logger.LogWarning("Rejecting attach link={Link} address={Address}: {Description}", attach.LinkName, address, error.Description);
            attachContext.Complete(error);
            return;
        }

        if (resolution.Topic is { } topic)
        {
            if (attach.Role)
            {
                _logger.LogWarning("Rejecting receiver attach link={Link} on topic '{Topic}': must attach to a subscription", attach.LinkName, topic.Name);
                attachContext.Complete(new Error(ErrorCode.NotAllowed) { Description = $"Cannot receive from topic '{topic.Name}'; attach to '{topic.Name}/Subscriptions/<name>' instead." });
                return;
            }
            attachContext.Complete(new TargetLinkEndpoint(new TopicMessageSink(topic, _txnManager, _loggerFactory.CreateLogger<TopicMessageSink>()), attachContext.Link), 100);
            return;
        }

        var endpoint = resolution.Queue!;

        if (!attach.Role)
        {
            attachContext.Complete(new TargetLinkEndpoint(new QueueMessageSink(endpoint, _txnManager, _loggerFactory.CreateLogger<QueueMessageSink>()), attachContext.Link), 100);
            return;
        }

        if (TryReadSessionFilter(attach, out var hasFilter, out var requestedSessionId) && hasFilter)
        {
            AttachSessionReceiver(attachContext, address, endpoint, requestedSessionId);
            return;
        }

        attachContext.Complete(new DrainAwareSourceLinkEndpoint(new QueueMessageSource(endpoint, _txnManager, _loggerFactory.CreateLogger<QueueMessageSource>()), attachContext.Link), 0);
    }

    private void AttachSessionReceiver(AttachContext attachContext, string address, IQueueEndpoint endpoint, string? requestedSessionId)
    {
        var attach = attachContext.Attach;
        if (endpoint is not InMemoryQueue queue)
        {
            attachContext.Complete(new Error(ErrorCode.NotAllowed) { Description = $"Session filter not supported on '{address}'." });
            return;
        }

        if (requestedSessionId is null)
        {
            // Defer the attach: park it until a session becomes available or the SDK's
            // operation timeout elapses. Service Bus's broker holds the attach open for
            // the full duration; firing back NotFound immediately is the parity gap we're
            // closing here.
            var timeout = ReadOperationTimeout(attach) ?? DefaultNextSessionTimeout;
            _ = Task.Run(() => WaitAndAttachNextSessionAsync(attachContext, queue, timeout));
            return;
        }

        var session = queue.Sessions.GetOrCreate(requestedSessionId);
        if (!session.TryAcquireLock(out var lockToken, out var lockedUntil))
        {
            attachContext.Complete(new Error(SessionCannotBeLockedError) { Description = $"Session '{session.Id}' is already locked." });
            return;
        }
        CompleteSessionAttach(attachContext, queue, session, lockToken, lockedUntil);
    }

    private async Task WaitAndAttachNextSessionAsync(AttachContext attachContext, InMemoryQueue queue, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            var remaining = deadline - DateTime.UtcNow;
            if (remaining <= TimeSpan.Zero)
            {
                attachContext.Complete(new Error(ErrorCode.NotFound) { Description = "No unlocked session with pending messages." });
                return;
            }
            var candidate = await queue.Sessions.WaitForUnlockedWithMessagesAsync(remaining, CancellationToken.None).ConfigureAwait(false);
            if (candidate is null)
            {
                attachContext.Complete(new Error(ErrorCode.NotFound) { Description = "No unlocked session with pending messages." });
                return;
            }
            // Race: another waiter on the same signal may grab the lock first. Loop and
            // wait again rather than rejecting the attach.
            if (candidate.TryAcquireLock(out var lockToken, out var lockedUntil))
            {
                CompleteSessionAttach(attachContext, queue, candidate, lockToken, lockedUntil);
                return;
            }
        }
    }

    private void CompleteSessionAttach(AttachContext attachContext, InMemoryQueue queue, Session session, Guid lockToken, DateTime lockedUntil)
    {
        var attach = attachContext.Attach;
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
        var src = new QueueMessageSource(sessionEndpoint, _txnManager, _loggerFactory.CreateLogger<QueueMessageSource>());
        var listenerLink = attachContext.Link;
        listenerLink.AddClosedCallback((_, _) =>
        {
            queue.Sessions.ReleaseLock(session, lockToken);
            _logger.LogInformation("Session receiver detached id={SessionId}", session.Id);
        });
        _logger.LogInformation("Session receiver attached id={SessionId} lockedUntil={LockedUntil:O}", session.Id, lockedUntil);
        attachContext.Complete(new DrainAwareSourceLinkEndpoint(src, listenerLink), 0);
    }

    private static TimeSpan? ReadOperationTimeout(Attach attach)
    {
        if (attach.Properties is null || !attach.Properties.ContainsKey(OperationTimeout)) return null;
        return attach.Properties[OperationTimeout] switch
        {
            uint ms => TimeSpan.FromMilliseconds(ms),
            int ms when ms >= 0 => TimeSpan.FromMilliseconds(ms),
            long ms when ms >= 0 => TimeSpan.FromMilliseconds(ms),
            _ => null,
        };
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
