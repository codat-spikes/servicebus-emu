using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Amqp;

// Service Bus SQL filter dialect — recursive-descent parser and tree-walking
// evaluator. Three-valued logic (true / false / unknown=null) follows SQL92
// Kleene semantics: NULL propagates through comparisons and arithmetic-style
// operators, but IS NULL / EXISTS always return a definite bool.

abstract record SqlExpr
{
    public abstract object? Eval(Message m);
}

sealed record StringLit(string Value) : SqlExpr
{
    public override object? Eval(Message _) => Value;
}

sealed record IntLit(long Value) : SqlExpr
{
    public override object? Eval(Message _) => Value;
}

sealed record DoubleLit(double Value) : SqlExpr
{
    public override object? Eval(Message _) => Value;
}

sealed record BoolLit(bool Value) : SqlExpr
{
    public override object? Eval(Message _) => Value;
}

sealed record NullLit : SqlExpr
{
    public override object? Eval(Message _) => null;
}

sealed record AppPropRef(string Name) : SqlExpr
{
    public override object? Eval(Message m)
    {
        var map = m.ApplicationProperties?.Map;
        if (map is null) return null;
        // Service Bus identifiers are case-insensitive; AMQP keys are stored as-given.
        foreach (var key in map.Keys)
        {
            if (string.Equals(key?.ToString(), Name, StringComparison.OrdinalIgnoreCase))
                return map[key];
        }
        return null;
    }
}

sealed record SysPropRef(string Name) : SqlExpr
{
    public override object? Eval(Message m)
    {
        var p = m.Properties;
        if (p is null) return null;
        return Name.ToLowerInvariant() switch
        {
            "messageid" => p.MessageId,
            "correlationid" => p.CorrelationId,
            "to" => p.To,
            "replyto" => p.ReplyTo,
            "label" or "subject" => p.Subject,
            "sessionid" or "groupid" => p.GroupId,
            "replytosessionid" or "replytogroupid" => p.ReplyToGroupId,
            "contenttype" => p.ContentType?.ToString(),
            _ => null,
        };
    }
}

enum CmpOp { Eq, Ne, Lt, Le, Gt, Ge }

sealed record Compare(SqlExpr Left, CmpOp Op, SqlExpr Right) : SqlExpr
{
    public override object? Eval(Message m)
    {
        var l = Left.Eval(m);
        var r = Right.Eval(m);
        if (l is null || r is null) return null;
        return SqlValue.Compare(l, r, Op);
    }
}

sealed record IsNullExpr(SqlExpr Inner, bool Negate) : SqlExpr
{
    public override object? Eval(Message m)
    {
        var v = Inner.Eval(m);
        var isNull = v is null;
        return Negate ? !isNull : isNull;
    }
}

sealed record ExistsExpr(SqlExpr Inner) : SqlExpr
{
    // EXISTS treats a property whose value is null the same as a missing property.
    public override object? Eval(Message m) => Inner.Eval(m) is not null;
}

sealed record LikeExpr : SqlExpr
{
    public SqlExpr Value { get; }
    public string Pattern { get; }
    public char? Escape { get; }
    public bool Negate { get; }
    private readonly Regex _regex;

    public LikeExpr(SqlExpr value, string pattern, char? escape, bool negate)
    {
        Value = value;
        Pattern = pattern;
        Escape = escape;
        Negate = negate;
        _regex = new Regex("^" + LikeToRegex(pattern, escape) + "$", RegexOptions.Singleline);
    }

    public override object? Eval(Message m)
    {
        var v = Value.Eval(m);
        if (v is null) return null;
        if (v is not string s) return null;
        var match = _regex.IsMatch(s);
        return Negate ? !match : match;
    }

    private static string LikeToRegex(string pattern, char? escape)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < pattern.Length; i++)
        {
            var c = pattern[i];
            if (escape is char e && c == e && i + 1 < pattern.Length)
                sb.Append(Regex.Escape(pattern[++i].ToString()));
            else if (c == '%') sb.Append(".*");
            else if (c == '_') sb.Append('.');
            else sb.Append(Regex.Escape(c.ToString()));
        }
        return sb.ToString();
    }
}

sealed record InExpr(SqlExpr Value, IReadOnlyList<SqlExpr> List, bool Negate) : SqlExpr
{
    public override object? Eval(Message m)
    {
        var v = Value.Eval(m);
        if (v is null) return null;
        var anyUnknown = false;
        foreach (var item in List)
        {
            var iv = item.Eval(m);
            if (iv is null) { anyUnknown = true; continue; }
            if (SqlValue.Compare(v, iv, CmpOp.Eq) is true)
                return !Negate;
        }
        if (anyUnknown) return null;
        return Negate;
    }
}

sealed record NotExpr(SqlExpr Inner) : SqlExpr
{
    public override object? Eval(Message m) => Inner.Eval(m) switch
    {
        null => null,
        bool b => !b,
        _ => null,
    };
}

sealed record AndExpr(SqlExpr Left, SqlExpr Right) : SqlExpr
{
    public override object? Eval(Message m)
    {
        var l = Left.Eval(m) as bool?;
        var r = Right.Eval(m) as bool?;
        if (l == false || r == false) return false;
        if (l is null || r is null) return null;
        return true;
    }
}

sealed record OrExpr(SqlExpr Left, SqlExpr Right) : SqlExpr
{
    public override object? Eval(Message m)
    {
        var l = Left.Eval(m) as bool?;
        var r = Right.Eval(m) as bool?;
        if (l == true || r == true) return true;
        if (l is null || r is null) return null;
        return false;
    }
}

static class SqlValue
{
    public static object? Compare(object l, object r, CmpOp op)
    {
        if (TryNumeric(l, out var ld) && TryNumeric(r, out var rd))
            return Apply(ld.CompareTo(rd), op);
        if (l is string ls && r is string rs)
            return Apply(string.CompareOrdinal(ls, rs), op);
        if (l is bool lb && r is bool rb)
            return op switch
            {
                CmpOp.Eq => lb == rb,
                CmpOp.Ne => lb != rb,
                _ => null,
            };
        return null;
    }

    private static bool TryNumeric(object o, out double d)
    {
        switch (o)
        {
            case sbyte v: d = v; return true;
            case byte v: d = v; return true;
            case short v: d = v; return true;
            case ushort v: d = v; return true;
            case int v: d = v; return true;
            case uint v: d = v; return true;
            case long v: d = v; return true;
            case ulong v: d = v; return true;
            case float v: d = v; return true;
            case double v: d = v; return true;
            case decimal v: d = (double)v; return true;
            default: d = 0; return false;
        }
    }

    private static bool Apply(int sign, CmpOp op) => op switch
    {
        CmpOp.Eq => sign == 0,
        CmpOp.Ne => sign != 0,
        CmpOp.Lt => sign < 0,
        CmpOp.Le => sign <= 0,
        CmpOp.Gt => sign > 0,
        CmpOp.Ge => sign >= 0,
        _ => throw new InvalidOperationException(),
    };
}

enum TokKind { Ident, Str, Int, Real, Sym, End }

readonly record struct Tok(TokKind Kind, string Text);

static class SqlLexer
{
    public static List<Tok> Tokenize(string s)
    {
        var result = new List<Tok>();
        int i = 0;
        while (i < s.Length)
        {
            var c = s[i];
            if (char.IsWhiteSpace(c)) { i++; continue; }

            if (c == '\'')
            {
                i++;
                var sb = new StringBuilder();
                while (true)
                {
                    if (i >= s.Length) throw new FormatException("Unterminated string literal.");
                    if (s[i] == '\'')
                    {
                        if (i + 1 < s.Length && s[i + 1] == '\'') { sb.Append('\''); i += 2; continue; }
                        i++;
                        break;
                    }
                    sb.Append(s[i++]);
                }
                result.Add(new Tok(TokKind.Str, sb.ToString()));
                continue;
            }

            if (char.IsDigit(c) || (c == '.' && i + 1 < s.Length && char.IsDigit(s[i + 1])))
            {
                int start = i;
                bool isReal = false;
                while (i < s.Length && (char.IsDigit(s[i]) || s[i] == '.'))
                {
                    if (s[i] == '.') isReal = true;
                    i++;
                }
                result.Add(new Tok(isReal ? TokKind.Real : TokKind.Int, s[start..i]));
                continue;
            }

            if (char.IsLetter(c) || c == '_')
            {
                int start = i;
                while (i < s.Length && (char.IsLetterOrDigit(s[i]) || s[i] == '_')) i++;
                result.Add(new Tok(TokKind.Ident, s[start..i]));
                continue;
            }

            if (c == '<' && i + 1 < s.Length && s[i + 1] == '=') { result.Add(new Tok(TokKind.Sym, "<=")); i += 2; continue; }
            if (c == '>' && i + 1 < s.Length && s[i + 1] == '=') { result.Add(new Tok(TokKind.Sym, ">=")); i += 2; continue; }
            if (c == '<' && i + 1 < s.Length && s[i + 1] == '>') { result.Add(new Tok(TokKind.Sym, "<>")); i += 2; continue; }
            if (c == '!' && i + 1 < s.Length && s[i + 1] == '=') { result.Add(new Tok(TokKind.Sym, "!=")); i += 2; continue; }
            if ("=<>(),.".IndexOf(c) >= 0) { result.Add(new Tok(TokKind.Sym, c.ToString())); i++; continue; }

            throw new FormatException($"Unexpected character '{c}' at position {i}.");
        }
        result.Add(new Tok(TokKind.End, ""));
        return result;
    }
}

// Recursive-descent parser. Precedence (low → high):
//   OR → AND → NOT → comparison/postfix → primary
sealed class SqlParser
{
    private readonly List<Tok> _t;
    private int _p;

    private SqlParser(List<Tok> tokens) { _t = tokens; }

    public static SqlExpr Parse(string expression)
    {
        var p = new SqlParser(SqlLexer.Tokenize(expression));
        var e = p.ParseOr();
        if (p.Peek().Kind != TokKind.End)
            throw new FormatException($"Unexpected trailing token '{p.Peek().Text}'.");
        return e;
    }

    private Tok Peek() => _t[_p];
    private Tok Next() => _t[_p++];

    private bool MatchKeyword(string kw)
    {
        var t = Peek();
        if (t.Kind == TokKind.Ident && t.Text.Equals(kw, StringComparison.OrdinalIgnoreCase))
        {
            _p++;
            return true;
        }
        return false;
    }

    private bool MatchSym(string sym)
    {
        var t = Peek();
        if (t.Kind == TokKind.Sym && t.Text == sym) { _p++; return true; }
        return false;
    }

    private void ExpectSym(string sym)
    {
        if (!MatchSym(sym))
            throw new FormatException($"Expected '{sym}', got '{Peek().Text}'.");
    }

    private SqlExpr ParseOr()
    {
        var left = ParseAnd();
        while (MatchKeyword("or")) left = new OrExpr(left, ParseAnd());
        return left;
    }

    private SqlExpr ParseAnd()
    {
        var left = ParseNot();
        while (MatchKeyword("and")) left = new AndExpr(left, ParseNot());
        return left;
    }

    private SqlExpr ParseNot()
    {
        if (MatchKeyword("not")) return new NotExpr(ParseNot());
        return ParseComparison();
    }

    private SqlExpr ParseComparison()
    {
        var left = ParsePrimary();

        if (MatchKeyword("is"))
        {
            bool not = MatchKeyword("not");
            if (!MatchKeyword("null")) throw new FormatException("Expected NULL after IS.");
            return new IsNullExpr(left, Negate: not);
        }

        if (MatchKeyword("not"))
        {
            // Postfix NOT must be followed by LIKE or IN.
            if (MatchKeyword("like")) return ParseLikeTail(left, negate: true);
            if (MatchKeyword("in")) return ParseInTail(left, negate: true);
            throw new FormatException("Expected LIKE or IN after NOT.");
        }

        if (MatchKeyword("like")) return ParseLikeTail(left, negate: false);
        if (MatchKeyword("in")) return ParseInTail(left, negate: false);

        var t = Peek();
        if (t.Kind == TokKind.Sym && t.Text is "=" or "<>" or "!=" or "<" or "<=" or ">" or ">=")
        {
            _p++;
            var right = ParsePrimary();
            var op = t.Text switch
            {
                "=" => CmpOp.Eq,
                "<>" or "!=" => CmpOp.Ne,
                "<" => CmpOp.Lt,
                "<=" => CmpOp.Le,
                ">" => CmpOp.Gt,
                ">=" => CmpOp.Ge,
                _ => throw new InvalidOperationException(),
            };
            return new Compare(left, op, right);
        }

        return left;
    }

    private SqlExpr ParseLikeTail(SqlExpr value, bool negate)
    {
        if (Peek().Kind != TokKind.Str) throw new FormatException("Expected string literal after LIKE.");
        var pattern = Next().Text;
        char? esc = null;
        if (MatchKeyword("escape"))
        {
            if (Peek().Kind != TokKind.Str) throw new FormatException("Expected string literal after ESCAPE.");
            var e = Next().Text;
            if (e.Length != 1) throw new FormatException("ESCAPE must be a single character.");
            esc = e[0];
        }
        return new LikeExpr(value, pattern, esc, negate);
    }

    private SqlExpr ParseInTail(SqlExpr value, bool negate)
    {
        ExpectSym("(");
        var items = new List<SqlExpr>();
        if (!MatchSym(")"))
        {
            items.Add(ParsePrimary());
            while (MatchSym(",")) items.Add(ParsePrimary());
            ExpectSym(")");
        }
        if (items.Count == 0) throw new FormatException("IN list must not be empty.");
        return new InExpr(value, items, negate);
    }

    private SqlExpr ParsePrimary()
    {
        var t = Peek();

        if (t.Kind == TokKind.Sym && t.Text == "(")
        {
            _p++;
            var e = ParseOr();
            ExpectSym(")");
            return e;
        }

        if (t.Kind == TokKind.Str) { _p++; return new StringLit(t.Text); }
        if (t.Kind == TokKind.Int) { _p++; return new IntLit(long.Parse(t.Text, CultureInfo.InvariantCulture)); }
        if (t.Kind == TokKind.Real) { _p++; return new DoubleLit(double.Parse(t.Text, CultureInfo.InvariantCulture)); }

        if (t.Kind == TokKind.Ident)
        {
            var lower = t.Text.ToLowerInvariant();
            if (lower == "true") { _p++; return new BoolLit(true); }
            if (lower == "false") { _p++; return new BoolLit(false); }
            if (lower == "null") { _p++; return new NullLit(); }
            if (lower == "exists")
            {
                _p++;
                ExpectSym("(");
                var inner = ParsePrimary();
                ExpectSym(")");
                return new ExistsExpr(inner);
            }

            _p++;
            if (MatchSym("."))
            {
                if (Peek().Kind != TokKind.Ident)
                    throw new FormatException("Expected identifier after '.'.");
                var sub = Next().Text;
                if (!lower.Equals("sys", StringComparison.OrdinalIgnoreCase))
                    throw new FormatException($"Unknown property scope '{t.Text}'.");
                return new SysPropRef(sub);
            }

            return new AppPropRef(t.Text);
        }

        throw new FormatException($"Unexpected token '{t.Text}'.");
    }
}
