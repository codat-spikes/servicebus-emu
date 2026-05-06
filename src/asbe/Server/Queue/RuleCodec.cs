using Amqp;
using Amqp.Framing;
using Amqp.Types;
using Encoder = Amqp.Types.Encoder;

// Service Bus rule wire format. Filters and actions are AMQP described-list types
// (descriptor codes match Microsoft.Azure.Amqp's known-type registrations):
//   sql-filter:list           0x0000013700000006
//   true-filter:list          0x0000013700000007
//   false-filter:list         0x0000013700000008
//   correlation-filter:list   0x0000013700000009
//   empty-rule-action:list    0x0000013700000005
//   sql-rule-action:list      0x000001370000000A
//   rule-description:list     0x0000013700000004
//
// AMQPNetLite's DescribedList helper has internal-only Write/ReadField hooks, so we use
// the public DescribedValue type instead — it accepts a descriptor + value and lets
// Encoder.WriteObject serialize the inner List as a list32, which is the same wire form
// as a described-list.
static class RuleCodec
{
    public const string SqlFilterKey = "sql-filter";
    public const string CorrelationFilterKey = "correlation-filter";
    public const string CorrelationPropertiesKey = "properties";
    public const string SqlRuleActionKey = "sql-rule-action";

    // Descriptors are emitted as symbols rather than ulongs. Microsoft.Azure.Amqp
    // registers known types under both the symbolic name and the ulong code, but its
    // generic-Map decoder mis-handles a 0x80 (ULong) descriptor when it encounters a
    // described value mid-map — the symbol encoding ("com.microsoft:...") routes through
    // its known-type table cleanly.
    private static readonly Symbol RuleDescriptionDescriptor = "com.microsoft:rule-description:list";
    private static readonly Symbol EmptyRuleActionDescriptor = "com.microsoft:empty-rule-action:list";
    private static readonly Symbol SqlRuleActionDescriptor = "com.microsoft:sql-rule-action:list";
    private static readonly Symbol SqlRuleFilterDescriptor = "com.microsoft:sql-filter:list";
    private static readonly Symbol TrueRuleFilterDescriptor = "com.microsoft:true-filter:list";
    private static readonly Symbol FalseRuleFilterDescriptor = "com.microsoft:false-filter:list";
    private static readonly Symbol CorrelationRuleFilterDescriptor = "com.microsoft:correlation-filter:list";

    public static (RuleFilter? Filter, RuleAction Action) DecodeRuleDescription(Map ruleDescription)
    {
        var filter = DecodeFilter(ruleDescription);
        RuleAction action = RuleAction.Empty;
        if (ruleDescription[SqlRuleActionKey] is Map sa
            && sa["expression"] is string expr
            && !string.IsNullOrWhiteSpace(expr))
        {
            action = new SqlRuleAction(expr);
        }
        return (filter, action);
    }

    public static RuleFilter? DecodeFilter(Map ruleDescription)
    {
        if (ruleDescription[SqlFilterKey] is Map sql)
        {
            var expr = sql["expression"] as string;
            return expr is null ? null : new SqlRuleFilter(expr);
        }
        if (ruleDescription[CorrelationFilterKey] is Map corr)
        {
            Dictionary<string, object>? props = null;
            if (corr[CorrelationPropertiesKey] is Map propMap && propMap.Count > 0)
            {
                props = new Dictionary<string, object>(propMap.Count, StringComparer.Ordinal);
                foreach (var key in propMap.Keys)
                    if (key is string s && propMap[key] is { } v) props[s] = v;
            }
            return new CorrelationRuleFilter(
                CorrelationId: corr["correlation-id"] as string,
                MessageId: corr["message-id"] as string,
                To: corr["to"] as string,
                ReplyTo: corr["reply-to"] as string,
                Label: corr["label"] as string,
                SessionId: corr["session-id"] as string,
                ReplyToSessionId: corr["reply-to-session-id"] as string,
                ContentType: corr["content-type"] as string,
                Properties: props);
        }
        return null;
    }

    public static Map EncodeRuleEntry(NamedRule rule) => new()
    {
        ["rule-description"] = new DescribedValue(RuleDescriptionDescriptor, new List
        {
            EncodeFilter(rule.Filter),
            EncodeAction(rule.Action),
            rule.Name,
            rule.CreatedAt,
        }),
    };

    // True/False filters are 0-field described lists in the Service Bus wire schema —
    // their string representation ("1=1"/"1=0") is a client-side model concern, not on
    // the wire. SqlFilter has Expression + CompatibilityLevel.
    private static DescribedValue EncodeFilter(RuleFilter filter) => filter switch
    {
        TrueRuleFilter => new DescribedValue(TrueRuleFilterDescriptor, new List()),
        FalseRuleFilter => new DescribedValue(FalseRuleFilterDescriptor, new List()),
        SqlRuleFilter sql => new DescribedValue(SqlRuleFilterDescriptor, new List { sql.Expression, 20 }),
        CorrelationRuleFilter c => EncodeCorrelationFilter(c),
        _ => throw new NotSupportedException($"Cannot encode {filter.GetType().Name} as AMQP rule filter."),
    };

    private static DescribedValue EncodeCorrelationFilter(CorrelationRuleFilter c)
    {
        var props = new Map();
        if (c.Properties is { Count: > 0 })
            foreach (var (k, v) in c.Properties) props[k] = v;
        return new DescribedValue(CorrelationRuleFilterDescriptor, new List
        {
            c.CorrelationId, c.MessageId, c.To, c.ReplyTo, c.Label,
            c.SessionId, c.ReplyToSessionId, c.ContentType, props,
        });
    }

    private static DescribedValue EncodeAction(RuleAction action) => action switch
    {
        SqlRuleAction sql => new DescribedValue(SqlRuleActionDescriptor, new List { sql.Expression, 20 }),
        _ => new DescribedValue(EmptyRuleActionDescriptor, new List()),
    };
}
