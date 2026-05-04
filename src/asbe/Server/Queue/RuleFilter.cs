using Amqp;

abstract record RuleFilter
{
    public abstract bool Matches(Message message);
    public static RuleFilter MatchAll { get; } = new TrueRuleFilter();
}

sealed record TrueRuleFilter : RuleFilter
{
    public override bool Matches(Message message) => true;
}

// Placeholder. SQL filter parsing is a real chunk of work (expression grammar +
// evaluator); ship correlation filters first, wire SQL up later.
sealed record SqlRuleFilter(string Expression) : RuleFilter
{
    public override bool Matches(Message message) =>
        throw new NotSupportedException(
            "SQL rule filters are not yet implemented. Use a CorrelationRuleFilter or TrueRuleFilter for now.");
}

sealed record CorrelationRuleFilter(
    string? CorrelationId = null,
    string? MessageId = null,
    string? To = null,
    string? ReplyTo = null,
    string? Label = null,
    string? SessionId = null,
    string? ReplyToSessionId = null,
    string? ContentType = null,
    IReadOnlyDictionary<string, object>? Properties = null) : RuleFilter
{
    public override bool Matches(Message message)
    {
        var props = message.Properties;
        if (!SystemPropMatches(CorrelationId, props?.CorrelationId)) return false;
        if (!SystemPropMatches(MessageId, props?.MessageId)) return false;
        if (!SystemPropMatches(To, props?.To)) return false;
        if (!SystemPropMatches(ReplyTo, props?.ReplyTo)) return false;
        if (Label is not null && !string.Equals(Label, props?.Subject, StringComparison.Ordinal)) return false;
        if (SessionId is not null && !string.Equals(SessionId, props?.GroupId, StringComparison.Ordinal)) return false;
        if (ReplyToSessionId is not null && !string.Equals(ReplyToSessionId, props?.ReplyToGroupId, StringComparison.Ordinal)) return false;
        if (ContentType is not null && !string.Equals(ContentType, props?.ContentType?.ToString(), StringComparison.Ordinal)) return false;

        if (Properties is { Count: > 0 })
        {
            var appProps = message.ApplicationProperties?.Map;
            foreach (var (key, expected) in Properties)
            {
                var actual = appProps?[key];
                if (!Equals(expected, actual)) return false;
            }
        }
        return true;
    }

    // CorrelationId / MessageId / To / ReplyTo come back as object on the AMQP wire
    // (string, Guid, ulong, byte[]). Service Bus correlation filters compare as strings,
    // so stringify both sides.
    private static bool SystemPropMatches(string? expected, object? actual)
    {
        if (expected is null) return true;
        return string.Equals(expected, actual?.ToString(), StringComparison.Ordinal);
    }
}
