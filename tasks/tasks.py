from celery import Celery
import os

app = Celery()
app.config_from_object('celeryconfig')


@app.task(serializer='json', name='etd-dash-service.tasks.send_to_dash')
def send_to_dash(message):
    message = {"Hello": "World"}
    print("Hello World")
    app.send_task("tasks.tasks.do_task", args=[message], kwargs={},
            queue=os.getenv('NEXT_QUEUE_NAME'))


if __name__ == '__main__':
    app.start()
