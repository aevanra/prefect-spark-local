#! /usr/bin/env bash

set -a
source .env
set +a

export PYTHONPATH=$PYHONPATH:$(pwd)

docker compose up --scale spark-worker=3 -d

if [ ! -f "./base_job_template.json" ]; then
    echo "Rendering Base Job Template"
    envsubst < "./base_job_template.template.json" > "./base_job_template.json"
else
    echo "Base Job Template exists"
fi

prefect work-pool create --set-as-default --base-job-template=./base_job_template.json --type docker "default"

echo "Default work pool created -- a worker will need to be run somewhere"
echo 'To run a worker, use `prefect worker start --pool "default"`'
