import datetime as dt
import json
import subprocess
import sys
import os
import requests
import logging
from os.path import expanduser
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

home = expanduser("~")
DAG_NAME = 'fraud_detection'

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger()

def stopStartStreamingJob():
    stop_streaming = 'touch /tmp/shutdownmarker'
    os.system(stop_streaming)
    shutdown_flag = False

    while not shutdown_flag:
        URL = "http://localhost:8080/json"
        r = requests.get(url=URL)
        data = json.loads(r.content)
        activeapps = data['activeapps']
        if not activeapps:
            logger.info("No Active apps, Streaming app is shutdown")
            shutdown_flag = True
        else:
            logger.info("List is not empty")
            activeFlag = False
            for app in activeapps:
                if app['name'] == 'RealTime Creditcard FraudDetection':
                    logger.info("Streaming Job is still running")
                    activeFlag = True
                    break
            if not activeFlag:
                logger.info("Streaming Job not in activeapps list.")
                shutdown_flag = True
            else:
                logger.info("Streaming Job is still Running")

    remove_shutdown_marker = 'rm -rf /tmp/shutdownmarker'
    os.system(remove_shutdown_marker)

    start_streaming = (
        'spark-submit --class com.datamantra.spark.jobs.RealTimeFraudDetection.DstreamFraudDetection '
        '--name "RealTime Creditcard FraudDetection" '
        '--master spark://datamantra:6066 --deploy-mode cluster --total-executor-cores 1 '
        + home + '/frauddetection/spark/fruaddetection-spark.jar '
        + home + '/frauddetection/spark/application-local.conf'
    )
    os.system(start_streaming)

default_args = {
    'owner': 'me',
    'start_date': dt.datetime(2018, 6, 1, 5),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

with DAG(
    DAG_NAME,
    default_args=default_args,
    schedule_interval='*/20 * * * *',
) as dag:

    remove_model = BashOperator(
        task_id='remove_model',
        bash_command='rm -rf ' + home + '/frauddetection/spark/training',
        default_args=default_args
    )

    create_model = BashOperator(
        task_id='create_model',
        bash_command=(
            'spark-submit --class com.datamantra.spark.jobs.FraudDetectionTraining '
            '--name "Fraud Detection Spark ML Training" '
            '--master spark://datamantra:7077 --total-executor-cores 1 '
            + home + '/frauddetection/spark/fruaddetection-spark.jar '
            + home + '/frauddetection/spark/application-local.conf'
        ),
        default_args=default_args
    )

    sleep = BashOperator(
        task_id='sleep',
        bash_command='sleep 5'
    )

    stop_start_streaming = PythonOperator(
        task_id='stop_start_streaming',
        python_callable=stopStartStreamingJob
    )
