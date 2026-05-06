using Amqp;
using Amqp.Framing;

// Service Bus SQL rule action grammar — comma-separated list of SET <prop> = <expr>
// or REMOVE <prop> statements. RHS expressions reuse the SqlFilter expression grammar,
// so anything legal in a filter is legal on the SET right-hand side.

abstract record SqlActionStatement
{
    public abstract void Apply(Message message);
}

sealed record SetStatement(PropertyTarget Target, SqlExpr Value) : SqlActionStatement
{
    public override void Apply(Message message)
    {
        var v = Value.Eval(message);
        Target.Write(message, v);
    }
}

sealed record RemoveStatement(PropertyTarget Target) : SqlActionStatement
{
    public override void Apply(Message message) => Target.Remove(message);
}

// A property target is either an application property (bare identifier) or a writable
// system property (sys.<name>). Sys writes go through the AMQP Properties section;
// app writes touch ApplicationProperties with case-insensitive key match (Service Bus
// identifiers are case-insensitive — so `set foo='x'` and `set Foo='y'` target the same
// slot).
sealed record PropertyTarget(string? AppPropName = null, string? SystemPropName = null)
{
    public void Write(Message message, object? value)
    {
        if (AppPropName is { } app)
        {
            message.ApplicationProperties ??= new ApplicationProperties();
            var map = message.ApplicationProperties.Map;
            object? existingKey = null;
            foreach (var k in map.Keys)
            {
                if (string.Equals(k?.ToString(), app, StringComparison.OrdinalIgnoreCase))
                {
                    existingKey = k;
                    break;
                }
            }
            if (existingKey is not null) map[existingKey] = value;
            else map[app] = value;
            return;
        }

        var sys = SystemPropName!;
        message.Properties ??= new Properties();
        var p = message.Properties;
        switch (sys.ToLowerInvariant())
        {
            case "messageid": p.MessageId = value?.ToString(); break;
            case "correlationid": p.CorrelationId = value?.ToString(); break;
            case "to": p.To = value?.ToString(); break;
            case "replyto": p.ReplyTo = value?.ToString(); break;
            case "label":
            case "subject": p.Subject = value?.ToString(); break;
            case "sessionid":
            case "groupid": p.GroupId = value?.ToString(); break;
            case "replytosessionid":
            case "replytogroupid": p.ReplyToGroupId = value?.ToString(); break;
            case "contenttype": p.ContentType = value?.ToString(); break;
        }
    }

    public void Remove(Message message)
    {
        if (AppPropName is { } app)
        {
            var map = message.ApplicationProperties?.Map;
            if (map is null) return;
            object? hit = null;
            foreach (var k in map.Keys)
            {
                if (string.Equals(k?.ToString(), app, StringComparison.OrdinalIgnoreCase))
                {
                    hit = k;
                    break;
                }
            }
            if (hit is not null) map.Remove(hit);
            return;
        }
        Write(message, null);
    }

    public static bool IsWritableSystemProp(string name) => name.ToLowerInvariant() switch
    {
        "messageid" or "correlationid" or "to" or "replyto" or "label" or "subject"
            or "sessionid" or "groupid" or "replytosessionid" or "replytogroupid"
            or "contenttype" => true,
        _ => false,
    };
}

static class SqlActionParser
{
    // Service Bus action grammar (per Microsoft Learn):
    //   statements ::= statement [, ...n]
    //   statement  ::= action [;]
    //   action     ::= SET <property> = <expression> | REMOVE <property>
    // Each SET assigns exactly one property — multi-assignment uses repeated comma-
    // separated SETs. Trailing/inter-statement ';' is an optional terminator.
    public static IReadOnlyList<SqlActionStatement> Parse(string action)
    {
        var tokens = SqlLexer.Tokenize(action);
        var p = new SqlParser(tokens);
        var stmts = new List<SqlActionStatement>();
        while (p.Peek().Kind != TokKind.End)
        {
            stmts.Add(ParseStatement(p));
            while (p.MatchSym(";")) { /* optional terminator */ }
            if (p.Peek().Kind == TokKind.End) break;
            if (!p.MatchSym(","))
                throw new FormatException($"Expected ',' between action statements, got '{p.Peek().Text}'.");
        }
        if (stmts.Count == 0) throw new FormatException("Empty rule action.");
        return stmts;
    }

    private static SqlActionStatement ParseStatement(SqlParser p)
    {
        if (p.MatchKeyword("set"))
        {
            var target = ParseTarget(p);
            if (!p.MatchSym("="))
                throw new FormatException($"Expected '=' after SET target, got '{p.Peek().Text}'.");
            var expr = p.ParseOr();
            return new SetStatement(target, expr);
        }
        if (p.MatchKeyword("remove"))
        {
            var target = ParseTarget(p);
            return new RemoveStatement(target);
        }
        throw new FormatException($"Expected SET or REMOVE, got '{p.Peek().Text}'.");
    }

    private static PropertyTarget ParseTarget(SqlParser p)
    {
        var t = p.Peek();
        if (t.Kind != TokKind.Ident)
            throw new FormatException($"Expected property name, got '{t.Text}'.");
        p.Next();
        if (p.MatchSym("."))
        {
            var sub = p.Peek();
            if (sub.Kind != TokKind.Ident)
                throw new FormatException("Expected identifier after '.'.");
            p.Next();
            if (!t.Text.Equals("sys", StringComparison.OrdinalIgnoreCase))
                throw new FormatException($"Unknown property scope '{t.Text}'.");
            if (!PropertyTarget.IsWritableSystemProp(sub.Text))
                throw new FormatException($"sys.{sub.Text} is not writable in a rule action.");
            return new PropertyTarget(SystemPropName: sub.Text);
        }
        return new PropertyTarget(AppPropName: t.Text);
    }
}
