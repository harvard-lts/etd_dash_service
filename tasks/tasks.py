from celery import Celery
import os
import logging
import etd
import json

app = Celery()
app.config_from_object('celeryconfig')
etd.configure_logger()
logger = logging.getLogger('etd_dash')

FEATURE_FLAGS = "feature_flags"
DASH_FEATURE_FLAG = "dash_feature_flag"


@app.task(serializer='json', name='etd-dash-service.tasks.send_to_dash')
def send_to_dash(message):
    logger.info("message")
    logger.info(message)
    json_message = json.loads(message)
    new_message = {"hello": "from etd-dash-service"}
    if FEATURE_FLAGS in json_message:
        feature_flags = json_message[FEATURE_FLAGS]
        new_message[FEATURE_FLAGS] = feature_flags
        if DASH_FEATURE_FLAG in feature_flags and \
                feature_flags[DASH_FEATURE_FLAG] == "on":
            # Send to DASH
            logger.debug("FEATURE IS ON>>>>>SEND TO DASH")
        else:
            # Feature is off so do hello world
            logger.debug("FEATURE FLAGS FOUND")
            logger.debug(json_message[FEATURE_FLAGS])

    # If only unit testing, return the message and
    # do not trigger the next task.
    if "unit_test" in json_message:
        return new_message

    app.send_task("etd-alma-service.tasks.send_to_alma", args=[new_message],
                  kwargs={}, queue=os.getenv('PUBLISH_QUEUE_NAME'))
