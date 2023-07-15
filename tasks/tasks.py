from celery import Celery
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


@tracer.start_as_current_span("send_to_dash_task")
@app.task(serializer='json', name='etd-dash-service.tasks.send_to_dash')
def send_to_dash(json_message):
    logger.info("message")
    logger.info(json_message)
    new_message = {"hello": "from etd-dash-service"}
    if FEATURE_FLAGS in json_message:
        feature_flags = json_message[FEATURE_FLAGS]
        new_message[FEATURE_FLAGS] = feature_flags
        if DASH_FEATURE_FLAG in feature_flags and \
                feature_flags[DASH_FEATURE_FLAG] == "on":
            # Send to DASH
            logger.debug("FEATURE IS ON>>>>>SEND TO DASH")
            current_span = trace.get_current_span()
            current_span.add_event("FEATURE IS ON>>>>>SEND TO DASH")
            current_span.add_event(json.dumps(json_message))
            worker = Worker()
            msg = worker.send_to_dash(json_message)
            logger.debug(msg)

        else:
            # Feature is off so do hello world
            logger.debug("FEATURE FLAGS FOUND")
            logger.debug(json_message[FEATURE_FLAGS])

    # If only unit testing, return the message and
    # do not trigger the next task.
    if "unit_test" in json_message:
        return new_message

    current_span.add_event("to next queue")
    app.send_task("etd-alma-service.tasks.send_to_alma", args=[new_message],
                  kwargs={}, queue=os.getenv('PUBLISH_QUEUE_NAME'))
