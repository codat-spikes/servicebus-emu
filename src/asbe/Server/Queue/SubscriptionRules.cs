using Amqp;

// Mutable, thread-safe ordered store of named rule filters for a subscription.
// Service Bus rule semantics: messages are delivered when *any* rule matches (OR).
// A subscription with zero rules drops every message — matches Azure when the user
// removes $Default without adding a replacement.
sealed class SubscriptionRules
{
    public const string DefaultRuleName = "$Default";

    private readonly Lock _gate = new();
    private readonly List<NamedRule> _rules = [];

    public SubscriptionRules(IReadOnlyList<RuleFilter>? seed)
    {
        if (seed is null || seed.Count == 0)
        {
            _rules.Add(new NamedRule(DefaultRuleName, RuleFilter.MatchAll, DateTime.UtcNow));
            return;
        }
        for (int i = 0; i < seed.Count; i++)
        {
            var name = i == 0 ? DefaultRuleName : $"rule{i}";
            _rules.Add(new NamedRule(name, seed[i], DateTime.UtcNow));
        }
    }

    public bool Matches(Message message)
    {
        lock (_gate)
        {
            for (int i = 0; i < _rules.Count; i++)
                if (_rules[i].Filter.Matches(message)) return true;
            return false;
        }
    }

    public IReadOnlyList<NamedRule> Snapshot()
    {
        lock (_gate) return _rules.ToArray();
    }

    public AddResult Add(string name, RuleFilter filter)
    {
        lock (_gate)
        {
            for (int i = 0; i < _rules.Count; i++)
                if (string.Equals(_rules[i].Name, name, StringComparison.Ordinal))
                    return AddResult.Conflict;
            _rules.Add(new NamedRule(name, filter, DateTime.UtcNow));
            return AddResult.Added;
        }
    }

    public bool Remove(string name)
    {
        lock (_gate)
        {
            for (int i = 0; i < _rules.Count; i++)
            {
                if (string.Equals(_rules[i].Name, name, StringComparison.Ordinal))
                {
                    _rules.RemoveAt(i);
                    return true;
                }
            }
            return false;
        }
    }

    public enum AddResult { Added, Conflict }
}

readonly record struct NamedRule(string Name, RuleFilter Filter, DateTime CreatedAt);
