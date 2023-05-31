# etd_dash_service
A Python service that moves ETD data into DASH.

<img src="https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/ives1227/3793013399189da4ff780f263984506c/raw/covbadge.json">

### References

- Coverage badge adapted from [Ned Batchelder](https://nedbatchelder.com/blog/202209/making_a_coverage_badge.html)


### Run hello world example

- Clone github 
- Checkout ETD-166 branch
- celeryconfig.py  
`cp celeryconfig.py.example celeryconfig.py`
- replace rabbit connect value with dev values
- .env  
`cp .env.example .env`
- Start up docker  
`docker-compose -f docker-compose-local.yml up --build -d --force-recreate`

- bring up DEV ETD Rabbit UI
- look for `etd_submission_ready` queue

- run invoke task python script  (celery must be installed locally)  
`pip install celery`  
`python3 scripts/invoke-task.py`

- look for `etd_in_storage` queue, and get the message
- and/or tail ./logs/etd/<containerid>supervisord_queuelistener_stderr.log to see activity

- Known issue: dropping a message directly on the `etd_submission_ready` queue not being properly read by task, nor progressing it to the next queue. Must run script.


