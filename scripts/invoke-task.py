from celery import Celery

app1 = Celery('tasks')
app1.config_from_object('celeryconfig')

# res = app1.send_task('tasks.tasks.send_to_dash', args=[{"hello":"world"}],
#   kwargs={}, queue="etd_submission_ready")
res = app1.send_task('etd-dash-service.tasks.send_to_dash',
            args=[{"hello": "world"}], kwargs={}, queue="etd_submission_ready")
