using System.Diagnostics;

// Single ActivitySource for the asbe server. Wired into the OTel SDK in Program.cs
// when `OTEL_EXPORTER_OTLP_ENDPOINT` is set — otherwise the SDK isn't loaded at
// all and the Activity instances created here are inert (zero-allocation no-ops).
//
// We follow OpenTelemetry's messaging semantic conventions where practical
// (https://opentelemetry.io/docs/specs/semconv/messaging/) but stay pragmatic:
// the goal is "useful spans show up in the Aspire dashboard", not strict spec
// compliance. Aspire maps these to the structured-log + traces panes
// automatically once the SDK forwards them over OTLP.
static class Telemetry
{
    public const string SourceName = "Asbe.Server";
    public static readonly ActivitySource ActivitySource = new(SourceName);
}
