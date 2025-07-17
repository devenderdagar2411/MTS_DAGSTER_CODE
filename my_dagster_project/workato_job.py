# Make sure Dagster can discover the Definitions object
__all__ = ["defs"]
# my_dagster_project/workato_job.py
import os
import sys
import requests
import argparse
import smtplib
import time
import re
from email.message import EmailMessage

from datetime import datetime
from typing import Dict, Optional, List
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

# Add the project root to the Python path to resolve sibling imports like 'resources'
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import your EmailService class
from my_dagster_project.services.email_service import EmailService

# Define configurable resources using modern Dagster approach
class WorkatoResource(ConfigurableResource):
    api_key: str
    trigger_url: str
    status_url_base: str
    skip_workato: bool = False
    environment: str = "UAT"  # Default environment

class EmailResource(ConfigurableResource):
    sender_email: str
    password: str
    recipient_email: str
    subject: str
    message: str
    smtp_server: str
    smtp_port: int

@op(out=Out(str))
def trigger_workato_job(context: OpExecutionContext, workato_config: WorkatoResource):
    """Trigger a Workato job run or generate mock job ID."""
    
    # Debug: Print credentials for troubleshooting
    context.log.info(f"API Key: {workato_config.api_key[:5]}...{workato_config.api_key[-5:]} (redacted middle)")
    context.log.info(f"Trigger URL: {workato_config.trigger_url}")
    context.log.info(f"Environment: {workato_config.environment}")
    
    # Check if we should skip Workato job execution
    if workato_config.skip_workato:
        context.log.info("Skipping Workato job execution and using mock job ID")
        mock_job_id = "j-MOCK123-TEST456"
        return mock_job_id
    
    # Validate credentials before making API call
    if not workato_config.api_key:
        raise Exception("Workato API key is missing. Please check your .env file.")
    if not workato_config.trigger_url:
        raise Exception("Workato trigger URL is missing. Please check your .env file.")
    
    context.log.info(f"Using Workato API URL: {workato_config.trigger_url}")
    
    headers = {
        "API-TOKEN": workato_config.api_key,
        "Content-Type": "application/json"
    }
    
    # Retry logic for triggering job
    max_retries = 3
    retry_delay = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            # Execute the API call
            context.log.info(f"Triggering Workato job (attempt {attempt + 1}/{max_retries})")
            resp = requests.post(workato_config.trigger_url, headers=headers, timeout=30)
            
            # Log response status and content for debugging
            context.log.info(f"Workato API Response Status: {resp.status_code}")
            try:
                json_response = resp.json()
                context.log.info(f"API Response: {str(json_response)}")
            except Exception as json_err:
                context.log.error(f"Failed to parse JSON response: {json_err}")
                context.log.info(f"Raw response content: {resp.text}")
            
            resp.raise_for_status()
            
            job_id = resp.json().get("jobid")
            if not job_id:
                raise Exception("No job ID returned from Workato API")
                
            context.log.info(f"Workato job triggered successfully with job_id: {job_id}")
            return job_id
            
        except requests.exceptions.HTTPError as http_err:
            context.log.error(f"HTTP error occurred (attempt {attempt + 1}/{max_retries}): {http_err}")
            context.log.error(f"Response content: {resp.text}")
            if attempt == max_retries - 1:
                raise
            context.log.info(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
        except requests.exceptions.RequestException as req_err:
            context.log.error(f"Request error occurred (attempt {attempt + 1}/{max_retries}): {req_err}")
            if attempt == max_retries - 1:
                raise
            context.log.info(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
        except Exception as e:
            context.log.error(f"Error triggering Workato job (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt == max_retries - 1:
                raise
            context.log.info(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)

@op(ins={"workato_job_id": In(str)}, out=Out(Dict))
def monitor_workato_job(context: OpExecutionContext, workato_job_id: str, workato_config: WorkatoResource) -> Dict:
    """Monitor Workato job execution and return final status"""
    if workato_config.skip_workato:
        return {
            "status": "skipped", 
            "job_id": workato_job_id,
            "environment": workato_config.environment
        }

    initial_delay = 180  # 30 seconds initial delay
    check_interval = 60  # 15 seconds between status checks
    max_duration = 1800  # 30 minutes maximum total duration

    url = f"{workato_config.status_url_base}/{workato_job_id}"
    headers = {
        "API-TOKEN": workato_config.api_key,
        "Content-Type": "application/json"
    }

    # Print monitoring start message with clear formatting
    print("\n" + "="*50)
    print("Starting Workato Job Monitoring")
    print("="*50)
    print(f"Job ID: {workato_job_id}")
    print(f"Environment: {workato_config.environment}")
    print(f"Initial delay: {initial_delay} seconds")
    print(f"Check interval: {check_interval} seconds")
    print(f"Status URL: {url}")
    print("="*50 + "\n")

    # Initial delay to allow job to start
    print(f"Waiting {initial_delay} seconds for job to initialize...")
    time.sleep(initial_delay)
    print("Starting job monitoring...\n")

    start_time = time.time()
    last_status = None
    error_found = False
    job_start_time = None
    job_end_time = None

    def print_timestamp_message(message, error=False):
        timestamp = datetime.now().strftime("%H:%M:%S")
        prefix = "❌" if error else "  "
        print(f"{timestamp}  {prefix} {message}")

    while (time.time() - start_time) < max_duration:
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            
            current_status = data.get("Status", "").lower()
            job_start_time = data.get("JobStartTime")
            job_end_time = data.get("JobEndTime")
            
            # Print status changes
            if current_status != last_status:
                status_message = f"Status: {current_status.upper()}"
                print_timestamp_message(status_message)
                last_status = current_status
                
                # Log timing information if available
                if job_start_time:
                    print_timestamp_message(f"Job started at: {job_start_time}")
                if job_end_time:
                    print_timestamp_message(f"Job ended at: {job_end_time}")
            
            # Handle job completion
            if current_status in ["success", "completed", "error", "failed", "cancelled"]:
                is_success = current_status in ["success", "completed"]
                
                print("\n" + "="*50)
                if is_success:
                    print("✅ Workato Job Completed Successfully!")
                else:
                    print("❌ Workato Job Failed!")
                    error_found = True
                
                print("="*50 + "\n")
                
                # Calculate duration if both start and end times are available
                duration = None
                if job_start_time and job_end_time:
                    try:
                        start_dt = datetime.fromisoformat(job_start_time.replace('Z', '+00:00'))
                        end_dt = datetime.fromisoformat(job_end_time.replace('Z', '+00:00'))
                        duration = (end_dt - start_dt).total_seconds()
                    except Exception as e:
                        context.log.warning(f"Could not calculate job duration: {e}")
                
                result = {
                    "status": current_status,
                    "job_id": workato_job_id,
                    "job_start_time": job_start_time,
                    "job_end_time": job_end_time,
                    "duration_seconds": duration,
                    "environment": workato_config.environment,
                    "is_success": is_success,
                    "error_details": "" if is_success else f"Workato job failed with status: {current_status}",
                    "status_url": url
                }
                
                # If job failed, add error details to the result
                if not is_success:
                    print("\n=== Workato Job Error Information ===")
                    error_message = f"Workato job failed with status: {current_status}"
                    if job_start_time:
                        error_message += f"\nJob started at: {job_start_time}"
                    if job_end_time:
                        error_message += f"\nJob ended at: {job_end_time}"
                    error_message += f"\nEnvironment: {workato_config.environment}"
                    error_message += f"\nStatus URL: {url}"
                    
                    print(error_message)
                    result["error_details"] = error_message
                
                return result
            
            time.sleep(check_interval)
            
        except requests.exceptions.HTTPError as http_err:
            print_timestamp_message(f"HTTP error while checking job status: {http_err}", error=True)
            # If it's a 404, the job might not exist yet, continue trying
            if resp.status_code == 404:
                print_timestamp_message("Job not found yet, continuing to monitor...", error=False)
            time.sleep(check_interval)
        except requests.exceptions.RequestException as req_err:
            print_timestamp_message(f"Request error while checking job status: {req_err}", error=True)
            time.sleep(check_interval)
        except Exception as e:
            if "Workato job failed" in str(e):
                raise  # Re-raise Workato failures to trigger email
            print_timestamp_message(f"Error checking job status: {str(e)}", error=True)
            time.sleep(check_interval)
    
    timeout_msg = f"Job monitoring timed out after {max_duration} seconds"
    print_timestamp_message(timeout_msg, error=True)
    raise Exception(timeout_msg)

@op(ins={"job_status": In(Dict)}, out=Out(str))
def send_email_notification(context: OpExecutionContext, job_status: Dict, email_config: EmailResource, workato_config: WorkatoResource):
    """Send email notification with job status details"""
    max_retries = 3
    retry_delay = 5  # seconds
    
    # Prepare email message
    if workato_config.skip_workato:
        message_text = email_config.message
        context.log.info("Using custom email message (Workato skipped)")
    else:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        status = job_status.get("status", "unknown")
        status_emoji = "✅" if status in ["success", "completed"] else "❌"
        environment = job_status.get("environment", workato_config.environment)
        
        # Calculate duration display
        duration_display = "N/A"
        if job_status.get("duration_seconds"):
            duration_seconds = job_status.get("duration_seconds")
            duration_minutes = duration_seconds // 60
            duration_seconds = duration_seconds % 60
            duration_display = f"{int(duration_minutes)}m {int(duration_seconds)}s"
        
        message_text = f"""
        Workato Job Run Notification {status_emoji}

        Environment: {environment}
        Job Status: {status.upper()}
        Job ID: {job_status.get('job_id')}
        
        Job Start Time: {job_status.get('job_start_time', 'N/A')}
        Job End Time: {job_status.get('job_end_time', 'N/A')}
        Duration: {duration_display}
        Current Time: {current_time}

        Status Check URL: {job_status.get('status_url', 'N/A')}
        """
        
        # Add error details if job failed
        if status not in ["success", "completed"]:
            message_text += f"""
        
        Error Details:
        {job_status.get('error_details', 'No error details available')}
        """
        
        message_text += f"\n\nThis is an automated notification from Dagster for {environment} environment."
        context.log.info(f"Generated Workato job notification for status: {status} in {environment}")
    
    # Create email service instance
    email_service = EmailService(
        sender_email=email_config.sender_email,
        password=email_config.password,
        smtp_server=email_config.smtp_server,
        smtp_port=email_config.smtp_port
    )
    
    context.log.info(f"Sending job status email to {email_config.recipient_email}")
    
    # Retry logic for sending email
    for attempt in range(max_retries):
        try:
            # Send email with job status
            email_service.send_email(
                recipient_email=email_config.recipient_email,
                subject=f"[{workato_config.environment}] {email_config.subject}",
                message=message_text,
                job_result=job_status
            )
            return f"Email notification sent successfully to {email_config.recipient_email}"
            
        except Exception as e:
            if attempt < max_retries - 1:
                context.log.warning(f"Email send attempt {attempt + 1} failed: {str(e)}")
                context.log.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                error_msg = f"Failed to send email notification after {max_retries} attempts: {str(e)}"
                context.log.error(error_msg)
                raise Exception(error_msg)

@graph
def workato_with_email_notification():
    """Graph that combines Workato job triggering with email notification"""
    job_id = trigger_workato_job()
    job_status = monitor_workato_job(job_id)
    send_email_notification(job_status)

# The job now uses the graph and specifies the required resources
@job
def workato_notification_job():
    """Job that triggers a Workato job and sends email notification"""
    workato_with_email_notification()

def load_env_config():
    """Load configuration from environment variables"""
    
    # Check for required environment variables
    required_vars = [
        "WORKATO_API_KEY",
        "WORKATO_TRIGGER_URL",
        "WORKATO_STATUS_URL_BASE",
        "EMAIL_SENDER",
        "EMAIL_PASSWORD",
        "EMAIL_RECIPIENT"
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"Error: Missing required environment variables: {', '.join(missing_vars)}")
        print("Please check your .env file or set these environment variables.")
        return None
    
    return {
        "workato_config": {
            "api_key": os.getenv("WORKATO_API_KEY"),
            "trigger_url": os.getenv("WORKATO_TRIGGER_URL"),
            "status_url_base": os.getenv("WORKATO_STATUS_URL_BASE"),
            "skip_workato": os.getenv("SKIP_WORKATO", "false").lower() == "true",
            "environment": os.getenv("WORKATO_ENVIRONMENT", "UAT")
        },
        "email_config": {
            "sender_email": os.getenv("EMAIL_SENDER"),
            "password": os.getenv("EMAIL_PASSWORD"),
            "recipient_email": os.getenv("EMAIL_RECIPIENT"),
            "subject": os.getenv("EMAIL_SUBJECT", "Dagster Workato Job Notification"),
            "message": os.getenv("EMAIL_MESSAGE", "This is an automated notification from Dagster."),
            "smtp_server": os.getenv("SMTP_SERVER", "smtp.office365.com"),
            "smtp_port": int(os.getenv("SMTP_PORT", "587"))
        }
    }

# Create the Definitions object that Dagster will use
def create_definitions():
    """Create Dagster definitions with proper resource configuration"""
    # Load configuration from environment
    config_data = load_env_config()
    if config_data is None:
        # Return empty definitions if config is missing
        return Definitions(
            jobs=[],
            resources={}
        )
    
    # Get configurations from environment
    workato_config_dict = config_data.get("workato_config", {})
    email_config_dict = config_data.get("email_config", {})
    
    # Create resource instances
    workato_resource = WorkatoResource(
        api_key=workato_config_dict["api_key"],
        trigger_url=workato_config_dict["trigger_url"],
        status_url_base=workato_config_dict["status_url_base"],
        skip_workato=workato_config_dict["skip_workato"],
        environment=workato_config_dict["environment"]
    )
    
    email_resource = EmailResource(
        sender_email=email_config_dict["sender_email"],
        password=email_config_dict["password"],
        recipient_email=email_config_dict["recipient_email"],
        subject=email_config_dict["subject"],
        message=email_config_dict["message"],
        smtp_server=email_config_dict["smtp_server"],
        smtp_port=email_config_dict["smtp_port"]
    )
    
    return Definitions(
        jobs=[workato_notification_job],
        resources={
            "workato_config": workato_resource,
            "email_config": email_resource
        }
    )

# Create the definitions for Dagster to discover
defs = create_definitions()

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run a Dagster job that triggers a Workato job and sends an email notification")
    parser.add_argument("--skip-workato", action="store_true", help="Skip Workato job execution and only send email")
    parser.add_argument("--email-only", action="store_true", help="Same as --skip-workato, for easier understanding")
    parser.add_argument("--environment", default="UAT", help="Environment (default: UAT)")
    args = parser.parse_args()

    # Load configuration from environment
    config_data = load_env_config()
    
    # If configuration is missing, exit early
    if config_data is None:
        print("Exiting - please check your .env file or environment variables.")
        sys.exit(1)
    
    # Get configurations from environment
    workato_config_dict = config_data.get("workato_config", {})
    email_config_dict = config_data.get("email_config", {})
    
    # Override skip_workato from command line args if specified
    if args.skip_workato or args.email_only:
        workato_config_dict["skip_workato"] = True
        print("Workato job execution will be skipped (email-only mode).")
    
    # Override environment from command line args if specified
    if args.environment:
        workato_config_dict["environment"] = args.environment
    
    # Create resource instances
    workato_resource = WorkatoResource(
        api_key=workato_config_dict["api_key"],
        trigger_url=workato_config_dict["trigger_url"],
        status_url_base=workato_config_dict["status_url_base"],
        skip_workato=workato_config_dict["skip_workato"],
        environment=workato_config_dict["environment"]
    )
    
    email_resource = EmailResource(
        sender_email=email_config_dict["sender_email"],
        password=email_config_dict["password"],
        recipient_email=email_config_dict["recipient_email"],
        subject=email_config_dict["subject"],
        message=email_config_dict["message"],
        smtp_server=email_config_dict["smtp_server"],
        smtp_port=email_config_dict["smtp_port"]
    )
    
    # Show execution mode
    if workato_resource.skip_workato:
        print(f"Running in email-only mode for {workato_resource.environment} environment (skipping Workato job)...")
    else:
        print(f"Running with Workato job execution for {workato_resource.environment} environment...")
    
    # Create a temporary definitions object for direct execution
    temp_defs = Definitions(
        jobs=[workato_notification_job],
        resources={
            "workato_config": workato_resource,
            "email_config": email_resource
        }
    )
    
    print("\nStarting job execution...")
    
    try:
        # Execute the job with our resource configuration
        result = temp_defs.get_job_def("workato_notification_job").execute_in_process()
        if result.success:
            print("\n✅ Job completed successfully!")
        else:
            print("\n❌ Job failed!")
            if result.failure_data:
                print(f"Error: {result.failure_data.error}")
            sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error running job: {str(e)}")
        sys.exit(1)
