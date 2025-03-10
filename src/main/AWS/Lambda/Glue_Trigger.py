import json
import boto3
import time
from datetime import datetime, timedelta

# Initialize AWS clients
glue_client = boto3.client('glue')
sns_client = boto3.client('sns')

# SNS Topic ARN (Replace with your SNS Topic ARN)
SNS_TOPIC_ARN = "arn:aws:sns:us-west-2:503561435342:healthtopic"

# Glue Job Name
GLUE_JOB_NAME = "Project_Health"

def lambda_handler(event, context):

    try:
        job_id = start_glue_job()
        if job_id:
            send_sns_notification("STARTED", job_id)
            monitor_glue_job(job_id)  # Monitor Glue job status
    except Exception as e:
        print(f"Error processing SQS message: {e}")

def start_glue_job():
    try:
        response = glue_client.start_job_run(JobName=GLUE_JOB_NAME)
        job_id = response['JobRunId']
        print(f"Glue job started with run ID: {job_id}")
        return job_id
    except Exception as e:
        print(f"Error starting Glue job: {e}")
        return None

def monitor_glue_job(job_id):
    """Poll the Glue job status until it completes."""
    while True:
        try:
            response = glue_client.get_job_run(JobName=GLUE_JOB_NAME, RunId=job_id)
            job_status = response["JobRun"]["JobRunState"]
            print(f"Glue job {job_id} status: {job_status}")

            if job_status in ["SUCCEEDED", "FAILED", "STOPPED"]:
                send_sns_notification(job_status, job_id)
                break  # Exit loop after final status

            time.sleep(30)  # Wait before checking again

        except Exception as e:
            print(f"Error checking Glue job status: {e}")
            send_sns_notification("FAILED", job_id)
            break

def send_sns_notification(status, job_id):
    """Send SNS notifications for job start, success, and failure."""
    messages = {
        "STARTED": f"Glue job '{GLUE_JOB_NAME}' has started. JobRunId: {job_id}",
        "SUCCEEDED": f"Glue job '{GLUE_JOB_NAME}' completed successfully. JobRunId: {job_id}",
        "FAILED": f"Glue job '{GLUE_JOB_NAME}' failed. JobRunId: {job_id}",
        "STOPPED": f"Glue job '{GLUE_JOB_NAME}' was stopped. JobRunId: {job_id}",
    }

    message = messages.get(status, f"Glue job '{GLUE_JOB_NAME}' status: {status}. JobRunId: {job_id}")

    try:
        response = sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=message,
            Subject=f"Glue Job {status} Notification"
        )
        print(f"SNS notification sent: {response['MessageId']}")
    except Exception as e:
        print(f"Error sending SNS notification: {e}")
