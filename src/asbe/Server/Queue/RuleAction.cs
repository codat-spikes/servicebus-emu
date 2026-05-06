using Amqp;
using Amqp.Framing;

// Rule actions mutate the matched message before it lands on the subscription buffer.
// Service Bus only ships SQL rule actions in practice — Empty is the default no-op.
abstract record RuleAction
{
    public abstract void Apply(Message message);
    public static RuleAction Empty { get; } = new EmptyRuleAction();
}

sealed record EmptyRuleAction : RuleAction
{
    public override void Apply(Message message) { }
}

sealed record SqlRuleAction(string Expression) : RuleAction
{
    private IReadOnlyList<SqlActionStatement>? _parsed;

    public override void Apply(Message message)
    {
        var stmts = _parsed ??= SqlActionParser.Parse(Expression);
        for (int i = 0; i < stmts.Count; i++) stmts[i].Apply(message);
    }
}
