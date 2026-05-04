using Amqp;
using Amqp.Framing;

// Service Bus's SDK packs `SendMessagesAsync(IEnumerable<>)` and ServiceBusMessageBatch
// into a single AMQP transfer with message-format = 0x80013700 and a body that's a list
// of Data sections, each containing one fully-encoded inner AMQP message. AMQPNetLite
// surfaces these as a DataList on the outer Message; we have to fan them back out.
static class BatchedMessage
{
    public const uint MessageFormat = 0x80013700u;

    public static IEnumerable<Message> Expand(Message outer)
    {
        if (outer.Format != MessageFormat)
        {
            yield return outer;
            yield break;
        }

        switch (outer.BodySection)
        {
            case DataList list:
                for (var i = 0; i < list.Count; i++)
                    yield return Decode(list[i].Binary);
                break;
            case Data single:
                yield return Decode(single.Binary);
                break;
            default:
                yield return outer;
                break;
        }
    }

    private static Message Decode(byte[] bytes) =>
        Message.Decode(new ByteBuffer(bytes, 0, bytes.Length, bytes.Length));
}
