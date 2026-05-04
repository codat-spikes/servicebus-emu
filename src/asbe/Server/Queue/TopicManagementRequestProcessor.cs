using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using Amqp.Types;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

// Service Bus exposes scheduled-message ops at the topic root (not per-subscription) —
// the SDK's ServiceBusSender.ScheduleMessageAsync targets `<topic>/$management`. We mirror
// that surface here. Other operations (peek, renew-lock, sessions) are subscription-scoped
// and live on the per-subscription management endpoint instead.
sealed class TopicManagementRequestProcessor(Topic topic, ILogger<TopicManagementRequestProcessor>? logger = null) : IRequestProcessor
{
    private readonly ILogger<TopicManagementRequestProcessor> _logger = logger ?? NullLogger<TopicManagementRequestProcessor>.Instance;
    private const string OperationKey = "operation";
    private const string StatusCodeKey = "statusCode";
    private const string StatusDescriptionKey = "statusDescription";
    private const string ScheduleMessageOperation = "com.microsoft:schedule-message";
    private const string CancelScheduledMessageOperation = "com.microsoft:cancel-scheduled-message";

    public int Credit => 100;

    public void Process(RequestContext requestContext)
    {
        var op = requestContext.Message.ApplicationProperties?[OperationKey] as string;
        Message response;
        try
        {
            response = op switch
            {
                ScheduleMessageOperation => Schedule(requestContext.Message),
                CancelScheduledMessageOperation => CancelScheduled(requestContext.Message),
                _ => Status(501, $"Operation '{op}' is not supported on a topic."),
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Topic management request failed operation={Operation}", op);
            response = Status(500, ex.Message);
        }
        requestContext.Complete(response);
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
            sequenceNumbers[i] = topic.Schedule(decoded);
        }

        return Ok(new Map { ["sequence-numbers"] = sequenceNumbers });
    }

    private Message CancelScheduled(Message request)
    {
        if (request.Body is not Map body) return Status(400, "Missing cancel body.");
        if (body["sequence-numbers"] is not Array tokens) return Status(400, "Missing sequence-numbers.");
        for (int i = 0; i < tokens.Length; i++)
        {
            if (tokens.GetValue(i) is long seq) topic.CancelScheduled(seq);
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
