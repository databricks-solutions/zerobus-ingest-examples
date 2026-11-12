"""Dice roller: a small FastAPI service instrumented with OpenTelemetry.

Every call to GET /rolldice rolls a six-sided die and emits all three signals over OTLP
to Vector (OTEL_EXPORTER_OTLP_ENDPOINT, default http://vector:4317):
  - a trace span, "roll", with the result as an attribute
  - a log record, "Rolled a <n>", inside that span (so it carries the trace and span IDs)
  - a counter metric, dice.rolls, labelled with the result
"""
import logging
import os
import random

from fastapi import FastAPI
from opentelemetry import metrics, trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

resource = Resource.create({"service.name": os.getenv("OTEL_SERVICE_NAME", "dice-roller")})

tracer_provider = TracerProvider(resource=resource)
tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(insecure=True)))
trace.set_tracer_provider(tracer_provider)

meter_provider = MeterProvider(
    resource=resource,
    metric_readers=[PeriodicExportingMetricReader(OTLPMetricExporter(insecure=True), export_interval_millis=10000)],
)
metrics.set_meter_provider(meter_provider)

logger_provider = LoggerProvider(resource=resource)
logger_provider.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter(insecure=True)))
set_logger_provider(logger_provider)

log = logging.getLogger("dice")
log.setLevel(logging.INFO)
log.addHandler(LoggingHandler(level=logging.INFO, logger_provider=logger_provider))

tracer = trace.get_tracer("dice")
rolls = metrics.get_meter("dice").create_counter("dice.rolls", unit="{roll}", description="Dice rolls by result")

app = FastAPI(title="Dice roller")


@app.get("/rolldice")
def roll_dice(player: str = "anonymous"):
    with tracer.start_as_current_span("roll") as span:
        result = random.randint(1, 6)
        span.set_attribute("dice.player", player)
        span.set_attribute("dice.result", result)
        rolls.add(1, {"dice.result": result})
        if result == 6:
            log.warning("%s rolled a six", player, extra={"dice.player": player, "dice.result": result})
        else:
            log.info("%s rolled a %d", player, result, extra={"dice.player": player, "dice.result": result})
        return {"player": player, "result": result}


@app.on_event("shutdown")
def flush_telemetry():
    tracer_provider.shutdown()
    meter_provider.shutdown()
    logger_provider.shutdown()
