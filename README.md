# ETD DASH Service
A Python service that moves ETD data into DASH.

<a href="https://github.com/harvard-lts/etd_dash_service/actions/workflows/pytest.yml"><img src="https://github.com/harvard-lts/etd_dash_service/actions/workflows/pytest.yml/badge.svg"></a>

<a href="https://github.com/harvard-lts/etd_dash_service/actions/workflows/pytest.yml"><img src="https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/ives1227/3793013399189da4ff780f263984506c/raw/covbadge.json"></a>

### References

- Coverage badge adapted from [Ned Batchelder](https://nedbatchelder.com/blog/202209/making_a_coverage_badge.html)

### Deployment
## Dev
Dev deployment will occur using Jenkins.  To trigger the development deployment, commit and push to the 'trial' or 'main' branch.

## QA
QA is hosted on servers that contain L4 data.  Jenkins is not permitted to deploy to these servers so for QA, Jenkins will only perform the build.  To deploy:
1. Commit and push to 'main'.
2. If any IF changes happened, use ansible deploy commands from the [ETD-IF](https://github.huit.harvard.edu/LTS/ETD-IF/blob/main/README.md) project.  Otherwise, manually restart the stack on the server that hosts QA.  

## Prod
Deploying to prod requires that the code be tagged in 'main'.  That means the code should be stable and ready for a release. 
1. Create the tag and push to the repo if this hasn't been done.
2.Open [Blue Ocean](https://ci.lib.harvard.edu/blue/organizations/jenkins/ETD%20DASH%20Service/)
3.Click on the "Branches" tab.
NOTE: you should see a pipeline with your new tag.  (if not, click on the "scan repository now" link in the sidebar.) 
4.Click on the green play (triangle) button to start the build
5.Follow the build progress using the blue ocean view
6.The build process should end with a green status. the docker image is now ready for deployment to prod.
7.Work with ops to deploy to prod using the ETD-IF project.

### Run hello world example locally

- Clone this repo from github 
- Create the .env by copying the .example.env
`cp .env.example .env`
- Replace rabbit connect value with dev values (found in 1Password LTS-ETD)
- Replace the `CONSUME_QUEUE_NAME` and `PUBLISH_QUEUE_NAME` with a unique name for local testing (eg - add your initials to the end of the queue names)
- Start up docker  
`docker-compose -f docker-compose-local.yml up --build -d --force-recreate`

- Bring up [DEV ETD Rabbit UI](https://b-7ecc68cb-6f33-40d6-8c57-0fbc0b84fa8c.mq.us-east-1.amazonaws.com/)
- Look for `CONSUME_QUEUE_NAME` queue

- Exec into the docker container
`docker exec -it etd-dash-service bash`
- Run invoke task python script
`python3 scripts/invoke-task.py`

- Look for `PUBLISH_QUEUE_NAME` queue, and get the message in the RabbitMQ UI
- and/or tail <NEED LOG INFO> to see activity


### Manually placing a message on the queue

- Open the queue in the RabbitMQ UI
- Click on the `CONSUME_QUEUE_NAME` queue (the name that you assigned this env value to)
- Open Publish Message
- Set a property of `content_type` to `application/json`
- Set the Payload to the following JSON content
`{"id": "da28b429-e006-49a5-ae77-da41b925bd85","task": "etd-dash-service.tasks.send_to_dash","args": [{"hello":"world"}]}`


