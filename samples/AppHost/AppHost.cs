// Sample AppHost demonstrating the Aspire.Hosting.Asbe integration.
//
// Run with:
//   dotnet run --project samples/AppHost
//
// The Aspire dashboard will start on http://localhost:18888 and the asbe
// container exposes its AMQP endpoint on a dynamically-allocated host port.
// Consuming projects added via builder.AddProject<T>(...).WithReference(asbe)
// receive a `ConnectionStrings__sb` env var pointing at the right port.

var builder = DistributedApplication.CreateBuilder(args);

var asbe = builder.AddAsbe("sb");

asbe.AddQueue("orders", q => q
    .WithLockDuration(TimeSpan.FromSeconds(30))
    .WithMaxDeliveryCount(10)
    .WithDuplicateDetection(TimeSpan.FromMinutes(10)));

asbe.AddQueue("invoices");

var events = asbe.AddTopic("events");
events.AddSubscription("billing", s => s.WithSqlFilter("Type = 'Invoice'"));
events.AddSubscription("audit");

// In a real AppHost you'd reference projects/services here:
//   builder.AddProject<Projects.AppA>("app-a").WithReference(asbe);
//   builder.AddProject<Projects.AppB>("app-b").WithReference(asbe).RequiresQueue(asbe, "orders");

builder.Build().Run();
