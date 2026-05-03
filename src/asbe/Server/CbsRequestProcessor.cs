using Amqp;
using Amqp.Framing;
using Amqp.Listener;

sealed class CbsRequestProcessor : IRequestProcessor
{
    public int Credit => 100;

    public void Process(RequestContext requestContext)
    {
        var op = requestContext.Message.ApplicationProperties?["operation"] as string;
        Console.WriteLine($"CBS request: operation={op}");

        var response = new Message { ApplicationProperties = new ApplicationProperties() };
        response.ApplicationProperties["status-code"] = 202;
        response.ApplicationProperties["status-description"] = "Accepted";

        requestContext.Complete(response);
    }
}
