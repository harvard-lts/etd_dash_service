from celery import Celery
import os
import logging
from etd.logging_config import configure_logger

app = Celery()
app.config_from_object('celeryconfig')
configure_logger()
logger = logging.getLogger('etd')
#app.log.setup(logger=celery_logger)

@app.task(serializer='json', name='etd-dash-service.tasks.send_to_dash')
def send_to_dash(message):
    logger.info("message")
    logger.info(message)
    new_message = {"hello": "from etd-dash-service"}
    app.send_task("etd-alma-service.tasks.send_to_alma", args=[new_message],
                  kwargs={}, queue=os.getenv('PUBLISH_QUEUE_NAME'))

           
