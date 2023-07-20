from celery import Celery
import os

app1 = Celery('tasks')
app1.config_from_object('celeryconfig')

arguments = {"hello": "world", "feature_flags": {
            'dash_feature_flag': "on",
            'alma_feature_flag': "off",
            'send_to_drs_feature_flag': "off",
            'drs_holding_record_feature_flag': "off"}}

res = app1.send_task('etd-dash-service.tasks.send_to_dash',
                     args=[arguments], kwargs={},
                     queue=os.getenv("CONSUME_QUEUE_NAME"))
