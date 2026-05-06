using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Logging;

// Maps the Service Bus management Atom-XML routes onto the WebApplication. Read-only
// in this phase — Phase 2 will add PUT/DELETE for entity CRUD and Phase 3 will add
// rule CRUD.
//
// Route precedence:
//   /$Resources/queues|topics  →  list endpoints
//   /{topic}/Subscriptions     →  subscription list
//   /{topic}/Subscriptions/{sub}  →  subscription entity
//   /{name}                    →  queue OR topic (probe both, 404 if neither)
//
// The {name} catch-all goes last so the more specific patterns match first.
//
// Responses are written through HttpContext directly rather than IResult helpers —
// Results.Text dropped the body in some configurations; writing the bytes ourselves
// removes that variable and gives us explicit control over Content-Length.
static class AdminRouter
{
    public static void Map(IEndpointRouteBuilder app, AmqpServer broker, string baseUri, ILogger logger)
    {
        app.MapGet("/$Resources/queues", (HttpContext ctx) => QueueList(ctx, broker, baseUri));
        app.MapGet("/$Resources/topics", (HttpContext ctx) => TopicList(ctx, broker, baseUri));

        app.MapGet("/{topic}/Subscriptions", (HttpContext ctx, string topic) => SubscriptionList(ctx, broker, baseUri, topic));
        app.MapGet("/{topic}/Subscriptions/{sub}", (HttpContext ctx, string topic, string sub) => Subscription(ctx, broker, baseUri, topic, sub));

        app.MapGet("/{name}", (HttpContext ctx, string name) => QueueOrTopic(ctx, broker, baseUri, name));
    }

    static Task QueueList(HttpContext ctx, AmqpServer broker, string baseUri)
    {
        var rows = broker.EnumerateQueueEntities()
            .Select(kv => (kv.Key, kv.Value.Options, kv.Value.SnapshotRuntime()))
            .ToArray();
        return WriteAtomFeed(ctx, AtomResponses.QueueFeed(baseUri, rows));
    }

    static Task TopicList(HttpContext ctx, AmqpServer broker, string baseUri)
    {
        var rows = broker.EnumerateTopicEntities()
            .Select(kv => (kv.Key, kv.Value.Options, kv.Value.SnapshotRuntime()))
            .ToArray();
        return WriteAtomFeed(ctx, AtomResponses.TopicFeed(baseUri, rows));
    }

    static Task SubscriptionList(HttpContext ctx, AmqpServer broker, string baseUri, string topicName)
    {
        if (!broker.TryGetTopicEntity(topicName, out var topic))
            return WriteNotFound(ctx, $"Topic '{topicName}' was not found.");
        var rows = topic.Subscriptions
            .Select(kv => (kv.Key, kv.Value.Options, kv.Value.SnapshotRuntime()))
            .ToArray();
        return WriteAtomFeed(ctx, AtomResponses.SubscriptionFeed(baseUri, topicName, rows));
    }

    static Task Subscription(HttpContext ctx, AmqpServer broker, string baseUri, string topicName, string subName)
    {
        if (!broker.TryGetSubscriptionEntity(topicName, subName, out var queue))
            return WriteNotFound(ctx, $"Subscription '{topicName}/Subscriptions/{subName}' was not found.");
        return WriteAtomEntry(ctx, AtomResponses.Subscription(baseUri, topicName, subName, queue.Options, queue.SnapshotRuntime()));
    }

    static Task QueueOrTopic(HttpContext ctx, AmqpServer broker, string baseUri, string name)
    {
        if (broker.TryGetQueueEntity(name, out var queue))
            return WriteAtomEntry(ctx, AtomResponses.Queue(baseUri, name, queue.Options, queue.SnapshotRuntime()));
        if (broker.TryGetTopicEntity(name, out var topic))
            return WriteAtomEntry(ctx, AtomResponses.Topic(baseUri, name, topic.Options, topic.SnapshotRuntime()));
        return WriteNotFound(ctx, $"Entity '{name}' was not found.");
    }

    static Task WriteAtomEntry(HttpContext ctx, string xml) => WriteResponse(ctx, 200, AtomResponses.AtomEntryContentType, xml);
    static Task WriteAtomFeed(HttpContext ctx, string xml) => WriteResponse(ctx, 200, AtomResponses.AtomFeedContentType, xml);
    static Task WriteNotFound(HttpContext ctx, string description) => WriteResponse(ctx, 404, "text/plain;charset=utf-8", description);

    static async Task WriteResponse(HttpContext ctx, int statusCode, string contentType, string body)
    {
        var bytes = Encoding.UTF8.GetBytes(body);
        ctx.Response.StatusCode = statusCode;
        ctx.Response.ContentType = contentType;
        ctx.Response.ContentLength = bytes.Length;
        await ctx.Response.Body.WriteAsync(bytes, 0, bytes.Length);
    }
}
