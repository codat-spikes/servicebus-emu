using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using Amqp.Types;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Encoder = Amqp.Types.Encoder;

sealed class ManagementRequestProcessor : IRequestProcessor
{
    // Peek and renew-lock work on any IQueueEndpoint (incl. the DLQ buffer). Schedule
    // and session ops only make sense on the main queue, so they require the richer
    // InMemoryQueue surface; on a DLQ-bound processor those operations return 501.
    private readonly IQueueEndpoint _endpoint;
    private readonly InMemoryQueue? _queue;
    private readonly SubscriptionRules? _rules;
    private readonly ILogger<ManagementRequestProcessor> _logger;

    public ManagementRequestProcessor(InMemoryQueue queue, ILogger<ManagementRequestProcessor>? logger = null)
        : this(queue, queue, rules: null, logger) { }

    public ManagementRequestProcessor(IQueueEndpoint endpoint, ILogger<ManagementRequestProcessor>? logger = null)
        : this(endpoint, queue: null, rules: null, logger) { }

    public ManagementRequestProcessor(InMemoryQueue subscription, SubscriptionRules rules, ILogger<ManagementRequestProcessor>? logger = null)
        : this(subscription, subscription, rules, logger) { }

    private ManagementRequestProcessor(IQueueEndpoint endpoint, InMemoryQueue? queue, SubscriptionRules? rules, ILogger<ManagementRequestProcessor>? logger)
    {
        _endpoint = endpoint;
        _queue = queue;
        _rules = rules;
        _logger = logger ?? NullLogger<ManagementRequestProcessor>.Instance;
    }

    private const string OperationKey = "operation";
    private const string StatusCodeKey = "statusCode";
    private const string StatusDescriptionKey = "statusDescription";
    private const string RenewLockOperation = "com.microsoft:renew-lock";
    private const string PeekMessageOperation = "com.microsoft:peek-message";
    private const string ScheduleMessageOperation = "com.microsoft:schedule-message";
    private const string CancelScheduledMessageOperation = "com.microsoft:cancel-scheduled-message";
    private const string RenewSessionLockOperation = "com.microsoft:renew-session-lock";
    private const string GetSessionStateOperation = "com.microsoft:get-session-state";
    private const string SetSessionStateOperation = "com.microsoft:set-session-state";
    private const string AddRuleOperation = "com.microsoft:add-rule";
    private const string RemoveRuleOperation = "com.microsoft:remove-rule";
    private const string EnumerateRulesOperation = "com.microsoft:enumerate-rules";
    private const string ReceiveBySequenceNumberOperation = "com.microsoft:receive-by-sequence-number";
    private const string UpdateDispositionOperation = "com.microsoft:update-disposition";

    public int Credit => 100;

    public void Process(RequestContext requestContext)
    {
        var op = requestContext.Message.ApplicationProperties?[OperationKey] as string;
        _logger.LogTrace("Management request operation={Operation}", op);

        Message response;
        try
        {
            response = op switch
            {
                RenewLockOperation => RenewLock(requestContext.Message),
                PeekMessageOperation => Peek(requestContext.Message),
                ScheduleMessageOperation => Schedule(requestContext.Message),
                CancelScheduledMessageOperation => CancelScheduled(requestContext.Message),
                RenewSessionLockOperation => RenewSessionLock(requestContext.Message),
                GetSessionStateOperation => GetSessionState(requestContext.Message),
                SetSessionStateOperation => SetSessionState(requestContext.Message),
                AddRuleOperation => AddRule(requestContext.Message),
                RemoveRuleOperation => RemoveRule(requestContext.Message),
                EnumerateRulesOperation => EnumerateRules(requestContext.Message),
                ReceiveBySequenceNumberOperation => ReceiveBySequenceNumber(requestContext.Message),
                UpdateDispositionOperation => UpdateDisposition(requestContext.Message),
                _ => Status(501, $"Operation '{op}' is not supported."),
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Management request failed operation={Operation}", op);
            response = Status(500, ex.Message);
        }
        requestContext.Complete(response);
    }

    private Message RenewLock(Message request)
    {
        if ((request.Body as Map)?["lock-tokens"] is not Array tokens)
            return Status(400, "Missing lock-tokens.");

        var expirations = new DateTime[tokens.Length];
        for (int i = 0; i < tokens.Length; i++)
        {
            if (tokens.GetValue(i) is not Guid token)
                return Status(400, "Lock token is not a uuid.");
            if (!_endpoint.TryRenewLock(token, out var expiresAt))
                return Status(410, $"Lock token {token} is no longer held.");
            expirations[i] = expiresAt;
        }

        return Ok(new Map { ["expirations"] = expirations });
    }

    private Message Peek(Message request)
    {
        if (request.Body is not Map body)
            return Status(400, "Missing peek body.");
        if (body["from-sequence-number"] is not long fromSeq)
            return Status(400, "Missing from-sequence-number.");
        var maxCount = body["message-count"] is int n ? n : 1;

        var messages = _endpoint.Peek(fromSeq, maxCount);
        if (messages.Count == 0)
            return Status(204, "No messages available.");

        var entries = new List();
        foreach (var message in messages)
        {
            var encoded = message.Encode();
            var bytes = new byte[encoded.Length];
            Buffer.BlockCopy(encoded.Buffer, encoded.Offset, bytes, 0, encoded.Length);
            entries.Add(new Map { ["message"] = bytes });
        }

        return new Message
        {
            BodySection = new AmqpValue { Value = new Map { ["messages"] = entries } },
            ApplicationProperties = new ApplicationProperties
            {
                [StatusCodeKey] = 200,
                [StatusDescriptionKey] = "OK",
            },
        };
    }

    private Message Schedule(Message request)
    {
        if (_queue is null) return Status(501, "Schedule is not supported on this entity.");
        if (request.Body is not Map body) return Status(400, "Missing schedule body.");
        if (body["messages"] is not System.Collections.IList entries || entries.Count == 0)
            return Status(400, "Missing messages.");

        var sequenceNumbers = new long[entries.Count];
        for (int i = 0; i < entries.Count; i++)
        {
            if (entries[i] is not Map entry) return Status(400, "Schedule entry is not a map.");
            var payload = entry["message"];
            byte[] bytes = payload switch
            {
                byte[] b => b,
                ArraySegment<byte> seg => seg.ToArray(),
                _ => null!,
            };
            if (bytes is null) return Status(400, "Schedule entry message must be binary.");

            var decoded = Message.Decode(new ByteBuffer(bytes, 0, bytes.Length, bytes.Length));
            sequenceNumbers[i] = _queue.Schedule(decoded);
        }

        return Ok(new Map { ["sequence-numbers"] = sequenceNumbers });
    }

    private Message CancelScheduled(Message request)
    {
        if (_queue is null) return Status(501, "CancelScheduled is not supported on this entity.");
        if (request.Body is not Map body) return Status(400, "Missing cancel body.");
        if (body["sequence-numbers"] is not Array tokens) return Status(400, "Missing sequence-numbers.");
        for (int i = 0; i < tokens.Length; i++)
        {
            if (tokens.GetValue(i) is long seq) _queue.CancelScheduled(seq);
        }
        return Status(200, "OK");
    }

    private Message RenewSessionLock(Message request)
    {
        if (_queue is null) return Status(501, "Sessions are not supported on this entity.");
        if (request.Body is not Map body) return Status(400, "Missing body.");
        if (body["session-id"] is not string sessionId) return Status(400, "Missing session-id.");
        var session = _queue.Sessions.Find(sessionId);
        if (session is null || !session.TryRenewLock(out var expiresAt))
            return Status(410, $"Session '{sessionId}' lock is not held.");
        return Ok(new Map { ["expiration"] = expiresAt });
    }

    private Message GetSessionState(Message request)
    {
        if (_queue is null) return Status(501, "Sessions are not supported on this entity.");
        if (request.Body is not Map body) return Status(400, "Missing body.");
        if (body["session-id"] is not string sessionId) return Status(400, "Missing session-id.");
        var session = _queue.Sessions.Find(sessionId);
        var state = session?.State ?? [];
        return Ok(new Map { ["session-state"] = state.Length > 0 ? state : null });
    }

    private Message SetSessionState(Message request)
    {
        if (_queue is null) return Status(501, "Sessions are not supported on this entity.");
        if (request.Body is not Map body) return Status(400, "Missing body.");
        if (body["session-id"] is not string sessionId) return Status(400, "Missing session-id.");
        var session = _queue.Sessions.GetOrCreate(sessionId);
        session.State = body["session-state"] switch
        {
            null => [],
            byte[] b => b,
            ArraySegment<byte> seg => seg.ToArray(),
            _ => session.State,
        };
        return Status(200, "OK");
    }

    private Message AddRule(Message request)
    {
        if (_rules is null) return Status(501, "Rule management is not supported on this entity.");
        if (request.Body is not Map body) return Status(400, "Missing add-rule body.");
        if (body["rule-name"] is not string ruleName || string.IsNullOrEmpty(ruleName))
            return Status(400, "Missing rule-name.");
        if (body["rule-description"] is not Map ruleDescription)
            return Status(400, "Missing rule-description.");
        var (filter, action) = RuleCodec.DecodeRuleDescription(ruleDescription);
        if (filter is null) return Status(400, "Rule must specify a sql-filter or correlation-filter.");

        return _rules.Add(ruleName, filter, action) switch
        {
            SubscriptionRules.AddResult.Added => Status(200, "OK"),
            SubscriptionRules.AddResult.Conflict => Status(409, $"Rule '{ruleName}' already exists."),
            _ => Status(500, "Unknown add-rule result."),
        };
    }

    private Message RemoveRule(Message request)
    {
        if (_rules is null) return Status(501, "Rule management is not supported on this entity.");
        if (request.Body is not Map body) return Status(400, "Missing remove-rule body.");
        if (body["rule-name"] is not string ruleName || string.IsNullOrEmpty(ruleName))
            return Status(400, "Missing rule-name.");
        return _rules.Remove(ruleName)
            ? Status(200, "OK")
            : Status(404, $"Rule '{ruleName}' does not exist.");
    }

    private Message EnumerateRules(Message request)
    {
        if (_rules is null) return Status(501, "Rule management is not supported on this entity.");
        var body = request.Body as Map;
        var skip = body?["skip"] is int s ? s : 0;
        var top = body?["top"] is int t ? t : 100;

        var snapshot = _rules.Snapshot();
        var entries = new List();
        var end = Math.Min(snapshot.Count, skip + top);
        for (int i = Math.Max(0, skip); i < end; i++) entries.Add(RuleCodec.EncodeRuleEntry(snapshot[i]));

        if (entries.Count == 0) return Status(204, "No rules.");
        return Ok(new Map { ["rules"] = entries });
    }

    private Message ReceiveBySequenceNumber(Message request)
    {
        if (_queue is null) return Status(501, "ReceiveBySequenceNumber is not supported on this entity.");
        if (request.Body is not Map body) return Status(400, "Missing receive-by-sequence-number body.");
        if (body["sequence-numbers"] is not Array seqs) return Status(400, "Missing sequence-numbers.");

        var entries = new List();
        for (int i = 0; i < seqs.Length; i++)
        {
            if (seqs.GetValue(i) is not long seq) continue;
            if (!_queue.TryReceiveDeferred(seq, out var message, out var lockToken, out var lockedUntil))
                continue;
            // The SDK reads LockedUntil from the AMQP MessageAnnotations on the encoded
            // message, not from the response map. Stamp it before we encode.
            message.MessageAnnotations ??= new Amqp.Framing.MessageAnnotations();
            message.MessageAnnotations.Map[(Amqp.Types.Symbol)"x-opt-locked-until"] = lockedUntil;
            var encoded = message.Encode();
            var bytes = new byte[encoded.Length];
            Buffer.BlockCopy(encoded.Buffer, encoded.Offset, bytes, 0, encoded.Length);
            entries.Add(new Map
            {
                ["message"] = bytes,
                ["lock-token"] = lockToken,
            });
        }

        if (entries.Count == 0) return Status(204, "No messages found.");
        return Ok(new Map { ["messages"] = entries });
    }

    private Message UpdateDisposition(Message request)
    {
        if (_queue is null) return Status(501, "UpdateDisposition is not supported on this entity.");
        if (request.Body is not Map body) return Status(400, "Missing update-disposition body.");
        if (body["lock-tokens"] is not Array tokens) return Status(400, "Missing lock-tokens.");
        if (body["disposition-status"] is not string status) return Status(400, "Missing disposition-status.");

        for (int i = 0; i < tokens.Length; i++)
        {
            if (tokens.GetValue(i) is not Guid lockToken) return Status(400, "Lock token is not a uuid.");
            bool ok = status switch
            {
                "completed" => _queue.CompleteDeferred(lockToken),
                "abandoned" => _queue.AbandonDeferred(lockToken),
                "defered" => true, // already deferred; lock is released by the next call too
                "suspended" => _queue.RejectDeferred(lockToken, ReadDeadLetterInfo(body)),
                _ => false,
            };
            if (!ok && status != "defered")
                return Status(410, $"Lock token {lockToken} is no longer held.");
            if (status == "defered") _queue.AbandonDeferred(lockToken);
        }
        return Status(200, "OK");
    }

    private static DeadLetterInfo ReadDeadLetterInfo(Map body)
    {
        var reason = body["deadletter-reason"] as string ?? "DeadLetteredByReceiver";
        var description = body["deadletter-description"] as string ?? "";
        return new DeadLetterInfo(reason, description);
    }

    private static Message Status(int code, string description) =>
        new()
        {
            ApplicationProperties = new ApplicationProperties
            {
                [StatusCodeKey] = code,
                [StatusDescriptionKey] = description,
            },
        };

    private static Message Ok(Map body) =>
        new()
        {
            BodySection = new ArrayAwareValueBody(body),
            ApplicationProperties = new ApplicationProperties
            {
                [StatusCodeKey] = 200,
                [StatusDescriptionKey] = "OK",
            },
        };
}

// AMQPNetLite's default Map encoding routes any IList — including managed arrays like
// DateTime[] — through WriteList, which the Microsoft.Azure.Amqp peer (used by the
// Service Bus SDK) decodes as List<object>. The SDK's GetValue<DateTime[]> then fails
// the type check. Encode our top-level map by hand so concrete arrays go out as proper
// AMQP arrays (homogeneous, single constructor) instead of lists.
sealed class ArrayAwareValueBody : AmqpValue
{
    private const byte Map32 = 0xd1;
    private readonly Map _map;

    public ArrayAwareValueBody(Map map)
    {
        _map = map;
        Value = map;
    }

    protected override void WriteValue(ByteBuffer buffer, object _)
    {
        int pos = buffer.WritePos;
        AmqpBitConverter.WriteUByte(buffer, Map32);
        AmqpBitConverter.WriteInt(buffer, 0);
        AmqpBitConverter.WriteInt(buffer, _map.Count * 2);

        foreach (var key in _map.Keys)
        {
            Encoder.WriteObject(buffer, key);
            var v = _map[key];
            if (v is Array array && v is not byte[])
                Encoder.WriteArray(buffer, array);
            else
                Encoder.WriteObject(buffer, v);
        }

        int size = buffer.WritePos - pos - 5;
        AmqpBitConverter.WriteInt(buffer.Buffer, pos + 1, size + 4);
    }
}
