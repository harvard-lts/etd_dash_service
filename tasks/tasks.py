from celery import Celery
import os
import json

app = Celery()
app.config_from_object('celeryconfig')


@app.task(serializer='json', name='etd-dash-service.tasks.send_to_dash')
def send_to_dash(message):
    print("message")
    print(message)
    json_message = json.loads(message)
    new_message = {"hello": "from etd-dash-service"}
    if "feature_flags" in json_message:
        feature_flags = json_message["feature_flags"]
        new_message["feature_flags"] = feature_flags
        if "dash_feature_flag" in feature_flags and \
                feature_flags["dash_feature_flag"] == "on":
            # Send to DASH
            print("FEATURE IS ON>>>>>SEND TO DASH")
        else:
            # Feature is off so do hello world
            print("FEATURE FLAGS FOUND")
            print(json_message['feature_flags'])

    app.send_task("etd-alma-service.tasks.send_to_alma", args=[new_message],
                  kwargs={}, queue=os.getenv('PUBLISH_QUEUE_NAME'))
