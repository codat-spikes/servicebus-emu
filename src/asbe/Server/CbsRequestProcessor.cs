using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

sealed class CbsRequestProcessor(ILogger<CbsRequestProcessor>? logger = null) : IRequestProcessor
{
    private readonly ILogger<CbsRequestProcessor> _logger = logger ?? NullLogger<CbsRequestProcessor>.Instance;

    public int Credit => 100;

    public void Process(RequestContext requestContext)
    {
        var op = requestContext.Message.ApplicationProperties?["operation"] as string;
        _logger.LogTrace("CBS request operation={Operation}", op);

        requestContext.Complete(new Message
        {
            ApplicationProperties = new ApplicationProperties
            {
                ["status-code"] = 202,
                ["status-description"] = "Accepted",
            },
        });
    }
}
