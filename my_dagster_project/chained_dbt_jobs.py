# my_dagster_project/workato_job.py
import os
import sys
import requests
import argparse
import smtplib
import time
from email.message import EmailMessage
from datetime import datetime
from typing import Dict, Optional
from dotenv import load_dotenv

from dagster import (
    op,
    job,
    Config,
    In,
    Out,
    graph,
    ResourceDefinition,
    OpExecutionContext,
    get_dagster_logger,
    resource,
    ConfigurableResource,
    Definitions
)

# Load environment variables from .env file
load_dotenv()

# EmailService definition assumed
from my_dagster_project.services.email_service import EmailService

class WorkatoConfig(ConfigurableResource):
    api_key: str
    trigger_url: str
    status_url_template: str

class EmailResource(ConfigurableResource):
    sender_email: str
    password: str
    recipient_email: str
    subject: str
    message: str
    smtp_server: str
    smtp_port: int

@op(out=Out(str))
def trigger_workato_job(context: OpExecutionContext, config: WorkatoConfig) -> str:
    headers = {
        "Authorization": f"Bearer {config.api_key}",
        "Content-Type": "application/json"
    }
    try:
        response = requests.post(config.trigger_url, headers=headers)
        response.raise_for_status()
        job_id = response.json()["jobid"]
        context.log.info(f"Triggered Workato Job ID: {job_id}")
        return job_id
    except Exception as e:
        context.log.error(f"Error triggering Workato job: {e}")
        raise

@op(ins={"job_id": In(str)}, out=Out(Dict))
def monitor_workato_job(context: OpExecutionContext, job_id: str, config: WorkatoConfig) -> Dict:
    url = config.status_url_template.replace("{jobid}", job_id)
    headers = {"Authorization": f"Bearer {config.api_key}"}
    start_time = time.time()
    max_duration = 1800
    check_interval = 10

    while (time.time() - start_time) < max_duration:
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            status = data.get("Status")
            context.log.info(f"Workato job status: {status}")
            if status.lower() in ["success", "error", "failed", "cancelled"]:
                return data
            time.sleep(check_interval)
        except Exception as e:
            context.log.error(f"Error checking job status: {e}")
            time.sleep(check_interval)
    raise Exception("Monitoring timed out")

@op(ins={"job_status": In(Dict)}, out=Out(str))
def send_email_notification(context: OpExecutionContext, job_status: Dict, email_config: EmailResource):
    status = job_status.get("Status", "Unknown")
    job_id = job_status.get("JobID")
    start = job_status.get("JobStartTime")
    end = job_status.get("JobEndTIme")
    is_success = status.lower() == "success"

    message = f"""
    Workato Job Notification - UAT

    Status: {status}
    Job ID: {job_id}
    Start: {start}
    End: {end}

    This is an automated notification from Dagster.
    """
    subject = f"Workato Job {'✅ Success' if is_success else '❌ Failed'} - UAT"

    email_service = EmailService(
        sender_email=email_config.sender_email,
        password=email_config.password,
        smtp_server=email_config.smtp_server,
        smtp_port=email_config.smtp_port
    )

    try:
        email_service.send_email(
            recipient_email=email_config.recipient_email,
            subject=subject,
            message=message,
            job_result=job_status
        )
        return "Email sent"
    except Exception as e:
        context.log.error(f"Email failed: {e}")
        raise

@graph
def workato_with_email():
    job_id = trigger_workato_job()
    job_status = monitor_workato_job(job_id)
    send_email_notification(job_status)

@job
def workato_notification_job():
    workato_with_email()

def create_definitions():
    config = WorkatoConfig(
        api_key=os.getenv("WORKATO_API_KEY"),
        trigger_url="https://apim.workato.com/keithb226/mts-dm-api-v1/mts-dm-api-trigger-sales-data-load",
        status_url_template="https://apim.workato.com/keithb226/mts-dm-api-v1/job/{jobid}"
    )
    email = EmailResource(
        sender_email=os.getenv("EMAIL_SENDER"),
        password=os.getenv("EMAIL_PASSWORD"),
        recipient_email=os.getenv("EMAIL_RECIPIENT"),
        subject="Dagster Workato Job Notification",
        message="Triggered Workato job. Check details.",
        smtp_server="smtp.office365.com",
        smtp_port=587
    )
    return Definitions(
        jobs=[workato_notification_job],
        resources={"config": config, "email_config": email}
    )

defs = create_definitions()
