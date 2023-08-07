from celery import Celery
from celery import bootsteps
from celery.signals import worker_ready
from celery.signals import worker_shutdown
from pathlib import Path
import os
import logging
import etd
from etd.worker import Worker
import json
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
        OTLPSpanExporter)
from opentelemetry.sdk.resources import SERVICE_NAME
from opentelemetry.trace.propagation.tracecontext \
    import TraceContextTextMapPropagator

app = Celery()
app.config_from_object('celeryconfig')
etd.configure_logger()
logger = logging.getLogger('etd_dash')

FEATURE_FLAGS = "feature_flags"
DASH_FEATURE_FLAG = "dash_feature_flag"

# tracing setup
JAEGER_NAME = os.getenv('JAEGER_NAME')
JAEGER_SERVICE_NAME = os.getenv('JAEGER_SERVICE_NAME')

resource = Resource(attributes={SERVICE_NAME: JAEGER_SERVICE_NAME})
trace.set_tracer_provider(TracerProvider(resource=resource))
tracer = trace.get_tracer(__name__)

otlp_exporter = OTLPSpanExporter(endpoint=JAEGER_NAME, insecure=True)

span_processor = BatchSpanProcessor(otlp_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)

# heartbeat setup
# code is from
# https://github.com/celery/celery/issues/4079#issuecomment-1270085680
hbeat_path = os.getenv("HEARTBEAT_FILE", "/tmp/worker_heartbeat")
ready_path = os.getenv("READINESS_FILE", "/tmp/worker_ready")
update_interval = float(os.getenv("HEALTHCHECK_UPDATE_INTERVAL", 15.0))
HEARTBEAT_FILE = Path(hbeat_path)
READINESS_FILE = Path(ready_path)
UPDATE_INTERVAL = update_interval  # touch file every 15 seconds


class LivenessProbe(bootsteps.StartStopStep):
    requires = {'celery.worker.components:Timer'}

    def __init__(self, worker, **kwargs):  # pragma: no cover
        self.requests = []
        self.tref = None

    def start(self, worker):  # pragma: no cover
        self.tref = worker.timer.call_repeatedly(
            UPDATE_INTERVAL, self.update_heartbeat_file,
            (worker,), priority=10,
        )

    def stop(self, worker):  # pragma: no cover
        HEARTBEAT_FILE.unlink(missing_ok=True)

    def update_heartbeat_file(self, worker):  # pragma: no cover
        HEARTBEAT_FILE.touch()


@worker_ready.connect
def worker_ready(**_):  # pragma: no cover
    READINESS_FILE.touch()


@worker_shutdown.connect
def worker_shutdown(**_):  # pragma: no cover
    READINESS_FILE.unlink(missing_ok=True)


app.steps["worker"].add(LivenessProbe)


@app.task(serializer='json', name='etd-dash-service.tasks.send_to_dash')
def send_to_dash(json_message):
    with tracer.start_as_current_span("send_to_dash_task") as current_span:
        logger.info("message")
        logger.info(json_message)
        new_message = json_message   # {"hello": "from etd-dash-service"}
        carrier = {}
        TraceContextTextMapPropagator().inject(carrier)
        traceparent = carrier["traceparent"]
        if FEATURE_FLAGS in json_message:
            feature_flags = json_message[FEATURE_FLAGS]
            # new_message[FEATURE_FLAGS] = feature_flags
            new_message["traceparent"] = traceparent
            if (DASH_FEATURE_FLAG in feature_flags and
                    feature_flags[DASH_FEATURE_FLAG] == "on"):  # pragma: no cover, unit test should not run send_to_dash # noqa: E501
                # Send to DASH
                logger.debug("FEATURE IS ON>>>>>SEND TO DASH")
                current_span.add_event("FEATURE IS ON>>>>>SEND TO DASH")
                current_span.add_event(json.dumps(json_message))
                worker = Worker()
                msg = worker.send_to_dash(json_message)
                logger.debug(msg)
                if 'identifier' in json_message:
                    proquest_identifier = json_message['identifier']
                    new_message['identifier'] = proquest_identifier
                    current_span.set_attribute("identifier",
                                               proquest_identifier)
                    logger.debug("processing id: " + str(proquest_identifier))

            else:
                # Feature is off so do hello world
                logger.debug("FEATURE FLAGS FOUND")
                logger.debug(json_message[FEATURE_FLAGS])
                current_span.add_event("FEATURE FLAGS FOUND")
                current_span.add_event(json.dumps(json_message))

        # If only unit testing, return the message and
        # do not trigger the next task.
        if "unit_test" in json_message:
            return new_message

        current_span.add_event("to next queue")  # pragma: no cover, unit tests end before this span # noqa: E501
        app.send_task("etd-alma-service.tasks.send_to_alma",
                      args=[new_message], kwargs={},
                      queue=os.getenv('PUBLISH_QUEUE_NAME'))  # pragma: no cover, unit tests should not progress the message # noqa: E501
