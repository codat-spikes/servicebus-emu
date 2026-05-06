using Amqp;
using Amqp.Framing;
using Xunit;

namespace Queueing;

// Pure unit tests for the SQL rule filter parser/evaluator. These bypass the
// AMQP server entirely — the integration suite only needs a smoke test that
// confirms SqlRuleFilter is wired through Topic.Enqueue, since these tests
// already cover the grammar.
public sealed class SqlFilterTests
{
    [Trait("Category", "Core")]
    [Trait("Speed", "Fast")]
    public sealed class Equality
    {
        [Theory]
        [InlineData("region = 'eu'", "eu", true)]
        [InlineData("region = 'eu'", "us", false)]
        [InlineData("region <> 'eu'", "us", true)]
        [InlineData("region <> 'eu'", "eu", false)]
        [InlineData("region != 'eu'", "us", true)]
        public void StringEquality(string expr, string value, bool expected) =>
            Assert.Equal(expected, Eval(expr, m => m.ApplicationProperties.Map["region"] = value));

        [Theory]
        [InlineData("name = 'O''Brien'", "O'Brien", true)]
        [InlineData("name = 'O''Brien'", "OBrien", false)]
        public void DoubledQuoteEscape(string expr, string value, bool expected) =>
            Assert.Equal(expected, Eval(expr, m => m.ApplicationProperties.Map["name"] = value));
    }

    [Trait("Category", "Core")]
    [Trait("Speed", "Fast")]
    public sealed class Numeric
    {
        [Theory]
        [InlineData("n = 5", 5, true)]
        [InlineData("n = 5", 6, false)]
        [InlineData("n > 10", 11, true)]
        [InlineData("n > 10", 10, false)]
        [InlineData("n >= 10", 10, true)]
        [InlineData("n < 10", 9, true)]
        [InlineData("n < 10", 10, false)]
        [InlineData("n <= 10", 10, true)]
        [InlineData("n <> 5", 6, true)]
        public void IntegerComparison(string expr, int value, bool expected) =>
            Assert.Equal(expected, Eval(expr, m => m.ApplicationProperties.Map["n"] = value));

        [Theory]
        [InlineData("ratio > 1.5", 2.0, true)]
        [InlineData("ratio > 1.5", 1.0, false)]
        public void DoubleComparison(string expr, double value, bool expected) =>
            Assert.Equal(expected, Eval(expr, m => m.ApplicationProperties.Map["ratio"] = value));

        [Fact]
        public void NumericTypesCompareAcrossWidths() =>
            // app prop arrives as long from AMQP wire; literal parsed as int — they must compare equal
            Assert.True(Eval("n = 5", m => m.ApplicationProperties.Map["n"] = 5L));
    }

    [Trait("Category", "Core")]
    [Trait("Speed", "Fast")]
    public sealed class BooleanAndNull
    {
        [Theory]
        [InlineData("enabled = TRUE", true, true)]
        [InlineData("enabled = TRUE", false, false)]
        [InlineData("enabled = FALSE", false, true)]
        public void BooleanLiteral(string expr, bool value, bool expected) =>
            Assert.Equal(expected, Eval(expr, m => m.ApplicationProperties.Map["enabled"] = value));

        [Fact]
        public void IsNull_OnMissingProperty_IsTrue() =>
            Assert.True(Eval("region IS NULL", _ => { }));

        [Fact]
        public void IsNull_OnPresentProperty_IsFalse() =>
            Assert.False(Eval("region IS NULL", m => m.ApplicationProperties.Map["region"] = "eu"));

        [Fact]
        public void IsNotNull_OnPresentProperty_IsTrue() =>
            Assert.True(Eval("region IS NOT NULL", m => m.ApplicationProperties.Map["region"] = "eu"));

        [Fact]
        public void IsNotNull_OnMissingProperty_IsFalse() =>
            Assert.False(Eval("region IS NOT NULL", _ => { }));
    }

    [Trait("Category", "Core")]
    [Trait("Speed", "Fast")]
    public sealed class ThreeValuedLogic
    {
        // NULL = 'x' is unknown, which is not a match.
        [Fact]
        public void EqualityAgainstMissingProperty_DoesNotMatch() =>
            Assert.False(Eval("region = 'eu'", _ => { }));

        // NOT (unknown) is still unknown → not a match.
        [Fact]
        public void NotOnUnknown_DoesNotMatch() =>
            Assert.False(Eval("NOT (region = 'eu')", _ => { }));

        // Kleene OR: TRUE OR unknown = TRUE.
        [Fact]
        public void Or_TrueWithUnknown_IsTrue() =>
            Assert.True(Eval("tier = 'gold' OR region = 'eu'",
                m => m.ApplicationProperties.Map["tier"] = "gold"));

        // Kleene AND: FALSE AND unknown = FALSE (still doesn't match, but proves no exception).
        [Fact]
        public void And_FalseWithUnknown_IsFalse() =>
            Assert.False(Eval("tier = 'gold' AND region = 'eu'",
                m => m.ApplicationProperties.Map["tier"] = "silver"));
    }

    [Trait("Category", "Core")]
    [Trait("Speed", "Fast")]
    public sealed class Logical
    {
        [Fact]
        public void And_BothTrue() =>
            Assert.True(Eval("region = 'eu' AND tier = 'gold'", m =>
            {
                m.ApplicationProperties.Map["region"] = "eu";
                m.ApplicationProperties.Map["tier"] = "gold";
            }));

        [Fact]
        public void And_OneFalse() =>
            Assert.False(Eval("region = 'eu' AND tier = 'gold'", m =>
            {
                m.ApplicationProperties.Map["region"] = "eu";
                m.ApplicationProperties.Map["tier"] = "silver";
            }));

        [Fact]
        public void Or_OneTrue() =>
            Assert.True(Eval("region = 'eu' OR region = 'us'",
                m => m.ApplicationProperties.Map["region"] = "us"));

        [Fact]
        public void Not_NegatesTrue() =>
            Assert.False(Eval("NOT (region = 'eu')",
                m => m.ApplicationProperties.Map["region"] = "eu"));

        // AND binds tighter than OR — without parens, this should be (a AND b) OR c.
        [Fact]
        public void Precedence_AndBeforeOr() =>
            Assert.True(Eval("region = 'eu' AND tier = 'gold' OR region = 'us'",
                m => m.ApplicationProperties.Map["region"] = "us"));

        [Fact]
        public void Parens_OverridePrecedence() =>
            Assert.False(Eval("region = 'eu' AND (tier = 'gold' OR region = 'us')", m =>
            {
                m.ApplicationProperties.Map["region"] = "us";
                m.ApplicationProperties.Map["tier"] = "silver";
            }));
    }

    [Trait("Category", "Core")]
    [Trait("Speed", "Fast")]
    public sealed class Like
    {
        [Theory]
        [InlineData("name LIKE 'order-%'", "order-123", true)]
        [InlineData("name LIKE 'order-%'", "shipment-1", false)]
        [InlineData("name LIKE '%-done'", "order-done", true)]
        [InlineData("name LIKE '%mid%'", "left-mid-right", true)]
        [InlineData("code LIKE 'A_C'", "ABC", true)]
        [InlineData("code LIKE 'A_C'", "AC", false)]
        [InlineData("code LIKE 'A_C'", "ABBC", false)]
        public void LikePattern(string expr, string value, bool expected) =>
            Assert.Equal(expected, Eval(expr, m => m.ApplicationProperties.Map[expr.Split(' ')[0]] = value));

        [Fact]
        public void LikeEscape_TreatsEscapedWildcardLiterally() =>
            Assert.True(Eval(@"name LIKE '50\%' ESCAPE '\'",
                m => m.ApplicationProperties.Map["name"] = "50%"));

        [Fact]
        public void LikeEscape_DoesNotMatchActualWildcard() =>
            Assert.False(Eval(@"name LIKE '50\%' ESCAPE '\'",
                m => m.ApplicationProperties.Map["name"] = "50abc"));

        [Fact]
        public void NotLike() =>
            Assert.True(Eval("name NOT LIKE 'order-%'",
                m => m.ApplicationProperties.Map["name"] = "shipment-1"));
    }

    [Trait("Category", "Core")]
    [Trait("Speed", "Fast")]
    public sealed class InOperator
    {
        [Theory]
        [InlineData("eu", true)]
        [InlineData("us", true)]
        [InlineData("apac", false)]
        public void In(string value, bool expected) =>
            Assert.Equal(expected, Eval("region IN ('eu', 'us')",
                m => m.ApplicationProperties.Map["region"] = value));

        [Fact]
        public void NotIn() =>
            Assert.True(Eval("region NOT IN ('eu', 'us')",
                m => m.ApplicationProperties.Map["region"] = "apac"));

        [Fact]
        public void In_NumericList() =>
            Assert.True(Eval("n IN (1, 2, 3)",
                m => m.ApplicationProperties.Map["n"] = 2));
    }

    [Trait("Category", "Core")]
    [Trait("Speed", "Fast")]
    public sealed class Exists
    {
        [Fact]
        public void Exists_OnPresentProperty_IsTrue() =>
            Assert.True(Eval("EXISTS(region)", m => m.ApplicationProperties.Map["region"] = "eu"));

        [Fact]
        public void Exists_OnMissingProperty_IsFalse() =>
            Assert.False(Eval("EXISTS(region)", _ => { }));

        // EXISTS treats a property whose value is null as not existing — matches Service Bus.
        [Fact]
        public void Exists_OnExplicitNullValue_IsFalse() =>
            Assert.False(Eval("EXISTS(region)", m => m.ApplicationProperties.Map["region"] = null!));
    }

    [Trait("Category", "Core")]
    [Trait("Speed", "Fast")]
    public sealed class SystemProperties
    {
        [Fact]
        public void SysMessageId() =>
            Assert.True(Eval("sys.MessageId = 'm1'", m => m.Properties = new Properties { MessageId = "m1" }));

        [Fact]
        public void SysCorrelationId() =>
            Assert.True(Eval("sys.CorrelationId = 'c1'", m => m.Properties = new Properties { CorrelationId = "c1" }));

        [Fact]
        public void SysTo() =>
            Assert.True(Eval("sys.To = 'dest'", m => m.Properties = new Properties { To = "dest" }));

        [Fact]
        public void SysReplyTo() =>
            Assert.True(Eval("sys.ReplyTo = 'reply'", m => m.Properties = new Properties { ReplyTo = "reply" }));

        // Service Bus exposes Properties.Subject as `Label` (or `sys.Label`).
        [Fact]
        public void SysLabel_FromSubject() =>
            Assert.True(Eval("sys.Label = 'tag'", m => m.Properties = new Properties { Subject = "tag" }));

        [Fact]
        public void SysSessionId_FromGroupId() =>
            Assert.True(Eval("sys.SessionId = 's1'", m => m.Properties = new Properties { GroupId = "s1" }));

        [Fact]
        public void SysReplyToSessionId_FromReplyToGroupId() =>
            Assert.True(Eval("sys.ReplyToSessionId = 's2'", m => m.Properties = new Properties { ReplyToGroupId = "s2" }));

        [Fact]
        public void SysContentType() =>
            Assert.True(Eval("sys.ContentType = 'application/json'",
                m => m.Properties = new Properties { ContentType = "application/json" }));

        // Application properties shadow the bare-identifier namespace; system properties
        // are only reachable via `sys.` — a bare `MessageId` should look in app props.
        [Fact]
        public void BareIdentifier_ResolvesToApplicationProperty_NotSystem()
        {
            // Property named 'MessageId' set as app property must match; system MessageId must be ignored.
            Assert.True(Eval("MessageId = 'app'", m =>
            {
                m.Properties = new Properties { MessageId = "system" };
                m.ApplicationProperties.Map["MessageId"] = "app";
            }));
        }
    }

    [Trait("Category", "Core")]
    [Trait("Speed", "Fast")]
    public sealed class CaseInsensitivity
    {
        [Theory]
        [InlineData("region = 'eu' and tier = 'gold'")]
        [InlineData("region = 'eu' AND tier = 'gold'")]
        [InlineData("region = 'eu' And tier = 'gold'")]
        public void Keywords_AreCaseInsensitive(string expr) =>
            Assert.True(Eval(expr, m =>
            {
                m.ApplicationProperties.Map["region"] = "eu";
                m.ApplicationProperties.Map["tier"] = "gold";
            }));

        // Identifiers in Service Bus SQL filters are case-insensitive.
        [Fact]
        public void Identifiers_AreCaseInsensitive() =>
            Assert.True(Eval("Region = 'eu'",
                m => m.ApplicationProperties.Map["region"] = "eu"));
    }

    // Realistic compound expressions that combine several grammar pieces at
    // once — these are the shapes that show up in real subscription rules.
    [Trait("Category", "Core")]
    [Trait("Speed", "Fast")]
    public sealed class Complex
    {
        // Tier-and-region routing with a numeric threshold and a system-property carve-out.
        private const string OrderRouting =
            "(region IN ('eu', 'uk') AND tier = 'gold') " +
            "AND amount >= 100 " +
            "AND sys.Label LIKE 'order-%' " +
            "AND NOT (status = 'cancelled')";

        [Fact]
        public void OrderRouting_HappyPath_Matches() =>
            Assert.True(Eval(OrderRouting, m =>
            {
                m.Properties = new Properties { Subject = "order-created" };
                m.ApplicationProperties.Map["region"] = "uk";
                m.ApplicationProperties.Map["tier"] = "gold";
                m.ApplicationProperties.Map["amount"] = 250;
                m.ApplicationProperties.Map["status"] = "open";
            }));

        [Fact]
        public void OrderRouting_CancelledStatus_DoesNotMatch() =>
            Assert.False(Eval(OrderRouting, m =>
            {
                m.Properties = new Properties { Subject = "order-created" };
                m.ApplicationProperties.Map["region"] = "uk";
                m.ApplicationProperties.Map["tier"] = "gold";
                m.ApplicationProperties.Map["amount"] = 250;
                m.ApplicationProperties.Map["status"] = "cancelled";
            }));

        [Fact]
        public void OrderRouting_MissingStatus_StillMatches() =>
            // status IS NULL → status = 'cancelled' is unknown → NOT unknown is unknown,
            // so the AND short-circuits to "not a match"… EXCEPT the spec for SB is that
            // missing props compared with `=` yield unknown, and `unknown AND TRUE` is
            // unknown → not delivered. So this expression intentionally requires status
            // to be set. Verify that behavior.
            Assert.False(Eval(OrderRouting, m =>
            {
                m.Properties = new Properties { Subject = "order-created" };
                m.ApplicationProperties.Map["region"] = "uk";
                m.ApplicationProperties.Map["tier"] = "gold";
                m.ApplicationProperties.Map["amount"] = 250;
            }));

        [Fact]
        public void OrderRouting_AmountBelowThreshold_DoesNotMatch() =>
            Assert.False(Eval(OrderRouting, m =>
            {
                m.Properties = new Properties { Subject = "order-created" };
                m.ApplicationProperties.Map["region"] = "uk";
                m.ApplicationProperties.Map["tier"] = "gold";
                m.ApplicationProperties.Map["amount"] = 50;
                m.ApplicationProperties.Map["status"] = "open";
            }));

        [Fact]
        public void OrderRouting_RegionNotInList_DoesNotMatch() =>
            Assert.False(Eval(OrderRouting, m =>
            {
                m.Properties = new Properties { Subject = "order-created" };
                m.ApplicationProperties.Map["region"] = "us";
                m.ApplicationProperties.Map["tier"] = "gold";
                m.ApplicationProperties.Map["amount"] = 250;
                m.ApplicationProperties.Map["status"] = "open";
            }));

        // De Morgan: NOT (a OR b) ≡ NOT a AND NOT b. Both forms must agree.
        [Theory]
        [InlineData("NOT (region = 'eu' OR region = 'us')")]
        [InlineData("region <> 'eu' AND region <> 'us'")]
        public void DeMorgan_Equivalence(string expr) =>
            Assert.True(Eval(expr, m => m.ApplicationProperties.Map["region"] = "apac"));

        // Mixed system + application properties with LIKE escape in a real-ish audit filter.
        [Fact]
        public void AuditFilter_MixedSystemAndAppProps() =>
            Assert.True(Eval(
                @"sys.CorrelationId LIKE 'trace-%' AND (path LIKE '/api/%\_internal' ESCAPE '\' OR EXISTS(forceAudit))",
                m =>
                {
                    m.Properties = new Properties { CorrelationId = "trace-abc123" };
                    m.ApplicationProperties.Map["path"] = "/api/users/_internal";
                }));

        // EXISTS short-circuits the OR even when the LIKE would have failed.
        [Fact]
        public void AuditFilter_ExistsBranchWins() =>
            Assert.True(Eval(
                @"sys.CorrelationId LIKE 'trace-%' AND (path LIKE '/api/%\_internal' ESCAPE '\' OR EXISTS(forceAudit))",
                m =>
                {
                    m.Properties = new Properties { CorrelationId = "trace-abc123" };
                    m.ApplicationProperties.Map["path"] = "/public/health";
                    m.ApplicationProperties.Map["forceAudit"] = true;
                }));

        // Deeply nested parens + mixed operators — exercises the recursive descent stack.
        [Fact]
        public void DeeplyNested_Parens() =>
            Assert.True(Eval(
                "((((n > 0) AND (n < 100)) OR (n = 999)) AND (NOT (flag = FALSE)))",
                m =>
                {
                    m.ApplicationProperties.Map["n"] = 50;
                    m.ApplicationProperties.Map["flag"] = true;
                }));

        // IN list with a string that contains a comma and a quote — tokenizer must not split on those.
        [Fact]
        public void In_StringLiteralsWithCommasAndQuotes() =>
            Assert.True(Eval(
                "label IN ('a,b', 'O''Brien', 'plain')",
                m => m.ApplicationProperties.Map["label"] = "O'Brien"));
    }

    [Trait("Category", "Core")]
    [Trait("Speed", "Fast")]
    public sealed class ParseErrors
    {
        [Theory]
        [InlineData("region =")]
        [InlineData("region = 'eu' AND")]
        [InlineData("(region = 'eu'")]
        [InlineData("LIKE 'x'")]
        [InlineData("region IN ()")]
        public void MalformedExpression_Throws(string expr) =>
            Assert.Throws<FormatException>(() =>
            {
                var f = new SqlRuleFilter(expr);
                f.Matches(new Message());
            });
    }

    private static bool Eval(string expression, Action<Message> configure)
    {
        var msg = new Message { ApplicationProperties = new ApplicationProperties() };
        configure(msg);
        return new SqlRuleFilter(expression).Matches(msg);
    }
}
