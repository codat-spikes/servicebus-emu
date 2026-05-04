using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using Amqp.Types;
using Encoder = Amqp.Types.Encoder;

sealed class ManagementRequestProcessor(InMemoryQueue queue) : IRequestProcessor
{
    private const string OperationKey = "operation";
    private const string StatusCodeKey = "statusCode";
    private const string StatusDescriptionKey = "statusDescription";
    private const string RenewLockOperation = "com.microsoft:renew-lock";
    private const string PeekMessageOperation = "com.microsoft:peek-message";
    private const string ScheduleMessageOperation = "com.microsoft:schedule-message";
    private const string CancelScheduledMessageOperation = "com.microsoft:cancel-scheduled-message";

    public int Credit => 100;

    public void Process(RequestContext requestContext)
    {
        var op = requestContext.Message.ApplicationProperties?[OperationKey] as string;
        Console.WriteLine($"Management request: operation={op}");

        var response = op switch
        {
            RenewLockOperation => RenewLock(requestContext.Message),
            PeekMessageOperation => Peek(requestContext.Message),
            ScheduleMessageOperation => Schedule(requestContext.Message),
            CancelScheduledMessageOperation => CancelScheduled(requestContext.Message),
            _ => Status(501, $"Operation '{op}' is not supported."),
        };
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
            if (!queue.TryRenewLock(token, out var expiresAt))
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

        var messages = queue.Peek(fromSeq, maxCount);
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
            sequenceNumbers[i] = queue.Schedule(decoded);
        }

        return Ok(new Map { ["sequence-numbers"] = sequenceNumbers });
    }

    private Message CancelScheduled(Message request)
    {
        if (request.Body is not Map body) return Status(400, "Missing cancel body.");
        if (body["sequence-numbers"] is not Array tokens) return Status(400, "Missing sequence-numbers.");
        for (int i = 0; i < tokens.Length; i++)
        {
            if (tokens.GetValue(i) is long seq) queue.CancelScheduled(seq);
        }
        return Status(200, "OK");
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
