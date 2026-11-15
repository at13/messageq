import { NodeSDK } from '@opentelemetry/sdk-node';
import { HttpInstrumentation } from '@opentelemetry/instrumentation-http';
import { PgInstrumentation } from '@opentelemetry/instrumentation-pg';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';

const sdk = process.env.OTEL_EXPORTER_OTLP_ENDPOINT ? new NodeSDK({
  traceExporter: new OTLPTraceExporter(),
  instrumentations: [new HttpInstrumentation(), new PgInstrumentation()],
}) : undefined;
sdk?.start();
export async function shutdownTelemetry() { await sdk?.shutdown(); }
