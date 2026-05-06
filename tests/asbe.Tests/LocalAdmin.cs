using Azure.Core;
using Azure.Core.Pipeline;
using Azure.Messaging.ServiceBus.Administration;

// Builds a ServiceBusAdministrationClient that talks to the local plain-HTTP listener
// instead of the SDK's default https endpoint. The SDK constructs URIs as
// https://{fqns}/{path}; we plug a custom HttpPipelineTransport whose HttpClient
// rewrites the request URI to the listener's actual scheme + host + port via a
// DelegatingHandler. Authorization is irrelevant — the listener doesn't check tokens —
// but the SDK still needs *some* TokenCredential to attach a header.
//
// Once the SDK supports a clean "custom endpoint" override (currently it doesn't —
// only namespace + credential or full connection string), we can drop this shim.
internal static class LocalAdmin
{
    public static ServiceBusAdministrationClient CreateClient()
    {
        var target = new Uri(LocalServer.HttpBaseUri);
        var inner = new HttpClientHandler();
        var rewriter = new SchemeRewritingHandler(target) { InnerHandler = inner };
        var httpClient = new HttpClient(rewriter);
        var options = new ServiceBusAdministrationClientOptions
        {
            Transport = new HttpClientTransport(httpClient),
        };
        // The SDK validates the FQNS shape (must look like "<name>.<suffix>", no port),
        // so we pass a synthetic well-formed name. The rewriter handler overwrites Host
        // and Port on every outgoing request before it leaves the process, so the actual
        // string here doesn't affect routing.
        const string syntheticFqns = "asbe-local.emulator.local";
        return new ServiceBusAdministrationClient(syntheticFqns, new EmulatorTokenCredential(), options);
    }

    sealed class SchemeRewritingHandler(Uri target) : DelegatingHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (request.RequestUri is { } original)
            {
                var ub = new UriBuilder(original)
                {
                    Scheme = target.Scheme,
                    Host = target.Host,
                    Port = target.Port,
                };
                request.RequestUri = ub.Uri;
            }
            return base.SendAsync(request, cancellationToken);
        }
    }

    sealed class EmulatorTokenCredential : TokenCredential
    {
        public override AccessToken GetToken(TokenRequestContext _, CancellationToken __) =>
            new("dev-token", DateTimeOffset.UtcNow.AddHours(1));
        public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext context, CancellationToken cancellationToken) =>
            new(GetToken(context, cancellationToken));
    }
}
