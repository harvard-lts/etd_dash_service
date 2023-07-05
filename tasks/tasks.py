from celery import Celery
import os
import logging
import etd
import json

app = Celery()
app.config_from_object('celeryconfig')
etd.configure_logger()
logger = logging.getLogger('etd_dash')

@app.task(serializer='json', name='etd-dash-service.tasks.send_to_dash')
def send_to_dash(message):
    logger.info("message")
    logger.info(message)
    json_message = json.loads(message)
    new_message = {"hello": "from etd-dash-service"}
    if "feature_flags" in json_message:
        feature_flags = json_message["feature_flags"]
        new_message["feature_flags"] = feature_flags
        if "dash_feature_flag" in feature_flags and \
                feature_flags["dash_feature_flag"] == "on":
            # Send to DASH
            logger.debug("FEATURE IS ON>>>>>SEND TO DASH")
        else:
            # Feature is off so do hello world
            logger.debug("FEATURE FLAGS FOUND")
            logger.debug(json_message['feature_flags'])

    app.send_task("etd-alma-service.tasks.send_to_alma", args=[new_message],
                  kwargs={}, queue=os.getenv('PUBLISH_QUEUE_NAME'))
