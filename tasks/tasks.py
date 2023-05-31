from celery import Celery

app = Celery()
app.config_from_object('celeryconfig')

@app.task(name='etd-dash-service.tasks.send_to_dash')
def send_to_dash():
    message = {"Hello":"World"}
    print("Hello World")
    celeryapp.execute.send_task("tasks.tasks.do_task", args=[message], kwargs={}, queue=os.getenv('NEXT_QUEUE_NAME'))
