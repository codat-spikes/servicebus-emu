using System.Text;
using System.Xml;
using System.Xml.Linq;

// Atom-XML response builders for the Service Bus management plane. Element ordering
// inside each {Queue,Topic,Subscription}Description matches the SDK's serializer in
// `Azure.Messaging.ServiceBus.Administration.QueuePropertiesExtensions.Serialize` so
// that round-trip through the SDK works without surprises. The runtime-side fields
// (CreatedAt/UpdatedAt/AccessedAt/MessageCount/SizeInBytes/CountDetails) are parsed by
// `QueueRuntimePropertiesExtensions.ParseFromEntryElement`.
//
// We always emit the *full* description (static + runtime) on a single GET — Service
// Bus does the same; its REST API has no "runtime properties only" endpoint, just the
// SDK splits them client-side.
static class AtomResponses
{
    public const string AtomNs = "http://www.w3.org/2005/Atom";
    public const string SbNs = "http://schemas.microsoft.com/netservices/2010/10/servicebus/connect";
    public const string AtomEntryContentType = "application/atom+xml;type=entry;charset=utf-8";
    public const string AtomFeedContentType = "application/atom+xml;type=feed;charset=utf-8";

    static readonly XNamespace Atom = AtomNs;
    static readonly XNamespace Sb = SbNs;

    public static string Queue(string baseUri, string name, QueueOptions options, EntityRuntimeSnapshot snapshot)
    {
        var entry = BuildQueueEntry(baseUri, name, options, snapshot);
        return Render(entry);
    }

    public static string Topic(string baseUri, string name, TopicOptions options, TopicRuntimeSnapshot snapshot)
    {
        var entry = BuildTopicEntry(baseUri, name, options, snapshot);
        return Render(entry);
    }

    public static string Subscription(string baseUri, string topicName, string subName, QueueOptions options, EntityRuntimeSnapshot snapshot)
    {
        var entry = BuildSubscriptionEntry(baseUri, topicName, subName, options, snapshot);
        return Render(entry);
    }

    public static string QueueFeed(string baseUri, IReadOnlyList<(string Name, QueueOptions Options, EntityRuntimeSnapshot Snapshot)> queues)
    {
        var entries = queues.Select(q => BuildQueueEntry(baseUri, q.Name, q.Options, q.Snapshot));
        return Render(BuildFeed(baseUri, "Queues", baseUri + "$Resources/queues", entries));
    }

    public static string TopicFeed(string baseUri, IReadOnlyList<(string Name, TopicOptions Options, TopicRuntimeSnapshot Snapshot)> topics)
    {
        var entries = topics.Select(t => BuildTopicEntry(baseUri, t.Name, t.Options, t.Snapshot));
        return Render(BuildFeed(baseUri, "Topics", baseUri + "$Resources/topics", entries));
    }

    public static string SubscriptionFeed(string baseUri, string topicName, IReadOnlyList<(string Name, QueueOptions Options, EntityRuntimeSnapshot Snapshot)> subs)
    {
        var entries = subs.Select(s => BuildSubscriptionEntry(baseUri, topicName, s.Name, s.Options, s.Snapshot));
        return Render(BuildFeed(baseUri, "Subscriptions", $"{baseUri}{topicName}/Subscriptions", entries));
    }

    static XElement BuildQueueEntry(string baseUri, string name, QueueOptions options, EntityRuntimeSnapshot snapshot)
    {
        var selfUrl = baseUri + name;
        var description = new XElement(Sb + "QueueDescription",
            // Order matches Azure SDK's Serialize() — see QueuePropertiesExtensions.cs.
            X("LockDuration", XmlConvert.ToString(options.LockDuration)),
            X("MaxSizeInMegabytes", "1024"),
            X("RequiresDuplicateDetection", BoolStr(options.RequiresDuplicateDetection)),
            X("RequiresSession", BoolStr(options.RequiresSession)),
            options.DefaultMessageTimeToLive is { } ttl ? X("DefaultMessageTimeToLive", XmlConvert.ToString(ttl)) : null,
            X("DeadLetteringOnMessageExpiration", BoolStr(options.DeadLetteringOnMessageExpiration)),
            options.RequiresDuplicateDetection && options.DuplicateDetectionHistoryTimeWindow is { } dw
                ? X("DuplicateDetectionHistoryTimeWindow", XmlConvert.ToString(dw)) : null,
            X("MaxDeliveryCount", options.MaxDeliveryCount.ToString(System.Globalization.CultureInfo.InvariantCulture)),
            X("EnableBatchedOperations", "true"),
            X("SizeInBytes", snapshot.SizeInBytes.ToString(System.Globalization.CultureInfo.InvariantCulture)),
            X("MessageCount", snapshot.TotalMessageCount.ToString(System.Globalization.CultureInfo.InvariantCulture)),
            BuildCountDetails(snapshot),
            X("CreatedAt", XmlConvert.ToString(snapshot.CreatedAt)),
            X("UpdatedAt", XmlConvert.ToString(snapshot.UpdatedAt)),
            X("AccessedAt", XmlConvert.ToString(snapshot.AccessedAt)),
            options.ForwardTo is { Length: > 0 } fwd ? X("ForwardTo", fwd) : null,
            options.ForwardDeadLetteredMessagesTo is { Length: > 0 } fwdDlq ? X("ForwardDeadLetteredMessagesTo", fwdDlq) : null,
            X("EnablePartitioning", "false"),
            X("EnableExpress", "false"));
        return BuildEntry(selfUrl, name, snapshot.CreatedAt, snapshot.UpdatedAt, description);
    }

    static XElement BuildTopicEntry(string baseUri, string name, TopicOptions options, TopicRuntimeSnapshot snapshot)
    {
        var selfUrl = baseUri + name;
        var description = new XElement(Sb + "TopicDescription",
            X("MaxSizeInMegabytes", "1024"),
            X("RequiresDuplicateDetection", BoolStr(options.RequiresDuplicateDetection)),
            options.RequiresDuplicateDetection && options.DuplicateDetectionHistoryTimeWindow is { } dw
                ? X("DuplicateDetectionHistoryTimeWindow", XmlConvert.ToString(dw)) : null,
            X("EnableBatchedOperations", "true"),
            X("SizeInBytes", snapshot.SizeInBytes.ToString(System.Globalization.CultureInfo.InvariantCulture)),
            BuildTopicCountDetails(snapshot),
            X("SubscriptionCount", snapshot.SubscriptionCount.ToString(System.Globalization.CultureInfo.InvariantCulture)),
            X("CreatedAt", XmlConvert.ToString(snapshot.CreatedAt)),
            X("UpdatedAt", XmlConvert.ToString(snapshot.UpdatedAt)),
            X("AccessedAt", XmlConvert.ToString(snapshot.AccessedAt)),
            X("SupportOrdering", "true"),
            X("EnablePartitioning", "false"),
            X("EnableExpress", "false"));
        return BuildEntry(selfUrl, name, snapshot.CreatedAt, snapshot.UpdatedAt, description);
    }

    static XElement BuildSubscriptionEntry(string baseUri, string topicName, string subName, QueueOptions options, EntityRuntimeSnapshot snapshot)
    {
        var selfUrl = $"{baseUri}{topicName}/Subscriptions/{subName}";
        var description = new XElement(Sb + "SubscriptionDescription",
            X("LockDuration", XmlConvert.ToString(options.LockDuration)),
            X("RequiresSession", BoolStr(options.RequiresSession)),
            options.DefaultMessageTimeToLive is { } ttl ? X("DefaultMessageTimeToLive", XmlConvert.ToString(ttl)) : null,
            X("DeadLetteringOnMessageExpiration", BoolStr(options.DeadLetteringOnMessageExpiration)),
            X("DeadLetteringOnFilterEvaluationExceptions", "false"),
            X("MessageCount", snapshot.TotalMessageCount.ToString(System.Globalization.CultureInfo.InvariantCulture)),
            X("MaxDeliveryCount", options.MaxDeliveryCount.ToString(System.Globalization.CultureInfo.InvariantCulture)),
            X("EnableBatchedOperations", "true"),
            BuildCountDetails(snapshot),
            X("CreatedAt", XmlConvert.ToString(snapshot.CreatedAt)),
            X("UpdatedAt", XmlConvert.ToString(snapshot.UpdatedAt)),
            X("AccessedAt", XmlConvert.ToString(snapshot.AccessedAt)),
            options.ForwardTo is { Length: > 0 } fwd ? X("ForwardTo", fwd) : null,
            options.ForwardDeadLetteredMessagesTo is { Length: > 0 } fwdDlq ? X("ForwardDeadLetteredMessagesTo", fwdDlq) : null);
        return BuildEntry(selfUrl, subName, snapshot.CreatedAt, snapshot.UpdatedAt, description);
    }

    static XElement BuildCountDetails(EntityRuntimeSnapshot s) => new(Sb + "CountDetails",
        // Note: SDK uses different XML namespace alias on CountDetails children — but
        // QueueRuntimePropertiesExtensions only matches on LocalName, so any namespace works.
        X("ActiveMessageCount", s.ActiveMessageCount.ToString(System.Globalization.CultureInfo.InvariantCulture)),
        X("DeadLetterMessageCount", s.DeadLetterMessageCount.ToString(System.Globalization.CultureInfo.InvariantCulture)),
        X("ScheduledMessageCount", s.ScheduledMessageCount.ToString(System.Globalization.CultureInfo.InvariantCulture)),
        X("TransferMessageCount", s.TransferMessageCount.ToString(System.Globalization.CultureInfo.InvariantCulture)),
        X("TransferDeadLetterMessageCount", s.TransferDeadLetterMessageCount.ToString(System.Globalization.CultureInfo.InvariantCulture)));

    static XElement BuildTopicCountDetails(TopicRuntimeSnapshot s) => new(Sb + "CountDetails",
        X("ActiveMessageCount", "0"),
        X("DeadLetterMessageCount", "0"),
        X("ScheduledMessageCount", s.ScheduledMessageCount.ToString(System.Globalization.CultureInfo.InvariantCulture)),
        X("TransferMessageCount", "0"),
        X("TransferDeadLetterMessageCount", "0"));

    static XElement BuildEntry(string selfUrl, string title, DateTimeOffset created, DateTimeOffset updated, XElement description) =>
        new(Atom + "entry",
            new XElement(Atom + "id", selfUrl),
            new XElement(Atom + "title", new XAttribute("type", "text"), title),
            new XElement(Atom + "published", XmlConvert.ToString(created)),
            new XElement(Atom + "updated", XmlConvert.ToString(updated)),
            new XElement(Atom + "link", new XAttribute("rel", "self"), new XAttribute("href", selfUrl)),
            new XElement(Atom + "content", new XAttribute("type", "application/xml"), description));

    static XElement BuildFeed(string baseUri, string title, string selfUrl, IEnumerable<XElement> entries) =>
        new(Atom + "feed",
            new XElement(Atom + "id", selfUrl),
            new XElement(Atom + "title", new XAttribute("type", "text"), title),
            new XElement(Atom + "updated", XmlConvert.ToString(DateTimeOffset.UtcNow)),
            new XElement(Atom + "link", new XAttribute("rel", "self"), new XAttribute("href", selfUrl)),
            entries.Cast<object>().ToArray());

    static XElement X(string name, string value) => new(Sb + name, value);

    static string BoolStr(bool b) => b ? "true" : "false";

    static string Render(XElement root)
    {
        // XmlWriter to a StringBuilder always uses UTF-16 for the declaration regardless
        // of XmlWriterSettings.Encoding, which lies about the bytes the response body
        // actually carries. Write to a MemoryStream so the declaration honours the byte
        // encoding, then decode back to a string for the response writer (which
        // re-encodes as UTF-8 anyway).
        var stream = new MemoryStream();
        using (var writer = XmlWriter.Create(stream, new XmlWriterSettings
        {
            OmitXmlDeclaration = false,
            Encoding = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false),
            Indent = false,
        }))
        {
            new XDocument(new XDeclaration("1.0", "utf-8", null), root).Save(writer);
        }
        return Encoding.UTF8.GetString(stream.ToArray());
    }
}
