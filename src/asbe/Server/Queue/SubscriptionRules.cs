using Amqp;

// Mutable, thread-safe ordered store of named rule filter+action pairs for a
// subscription. Service Bus rule semantics: each matching rule produces its own
// delivered copy (the message is fanned out per-match, not just once per subscription),
// so callers iterate Matching() rather than asking a single Matches bool. A subscription
// with zero rules drops every message — matches Azure when the user removes $Default
// without adding a replacement.
sealed class SubscriptionRules
{
    public const string DefaultRuleName = "$Default";

    private readonly Lock _gate = new();
    private readonly List<NamedRule> _rules = [];

    public SubscriptionRules(IReadOnlyList<RuleFilter>? seed)
    {
        if (seed is null || seed.Count == 0)
        {
            _rules.Add(new NamedRule(DefaultRuleName, RuleFilter.MatchAll, RuleAction.Empty, DateTime.UtcNow));
            return;
        }
        for (int i = 0; i < seed.Count; i++)
        {
            var name = i == 0 ? DefaultRuleName : $"rule{i}";
            _rules.Add(new NamedRule(name, seed[i], RuleAction.Empty, DateTime.UtcNow));
        }
    }

    public IReadOnlyList<NamedRule> Matching(Message message)
    {
        lock (_gate)
        {
            List<NamedRule>? hits = null;
            for (int i = 0; i < _rules.Count; i++)
            {
                if (_rules[i].Filter.Matches(message))
                    (hits ??= []).Add(_rules[i]);
            }
            return (IReadOnlyList<NamedRule>?)hits ?? Array.Empty<NamedRule>();
        }
    }

    public IReadOnlyList<NamedRule> Snapshot()
    {
        lock (_gate) return _rules.ToArray();
    }

    public AddResult Add(string name, RuleFilter filter, RuleAction action)
    {
        lock (_gate)
        {
            for (int i = 0; i < _rules.Count; i++)
                if (string.Equals(_rules[i].Name, name, StringComparison.Ordinal))
                    return AddResult.Conflict;
            _rules.Add(new NamedRule(name, filter, action, DateTime.UtcNow));
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

readonly record struct NamedRule(string Name, RuleFilter Filter, RuleAction Action, DateTime CreatedAt);
