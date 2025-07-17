# my_dagster_project/dbt_cloud_job.py
import os
import sys
import requests
import argparse
import smtplib
import time
import re # For regex in monitor_dbt_job
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
    OpExecutionContext, # Added for type hinting
    get_dagster_logger, # Added for logging within ops
    resource,
    ConfigurableResource,
    Definitions
)

# Load environment variables from .env file
load_dotenv()

# Add the project root to the Python path to resolve sibling imports like 'resources'
# This is crucial when running with `dagster dev` or `execute_in_process`
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import your EmailService class (assuming it's still located here)
from my_dagster_project.services.email_service import EmailService

# Define configurable resources using modern Dagster approach
class DBTCloudResource(ConfigurableResource):
    api_token: str
    account_id: str
    job_id: str
    skip_dbt: bool = False

class EmailResource(ConfigurableResource):
    sender_email: str
    password: str
    recipient_email: str
    subject: str
    message: str
    smtp_server: str
    smtp_port: int

@op(out=Out(int))
def trigger_dbt_cloud_job(context: OpExecutionContext, dbt_config: DBTCloudResource):
    """Trigger a dbt Cloud job run or generate mock run ID."""
    
    # Debug: Print credentials for troubleshooting
    context.log.info(f"API Token: {dbt_config.api_token[:5]}...{dbt_config.api_token[-5:]} (redacted middle)")
    context.log.info(f"Account ID: {dbt_config.account_id}")
    context.log.info(f"Job ID: {dbt_config.job_id}")
    
    # Check if we should skip DBT Cloud job execution
    if dbt_config.skip_dbt:
        context.log.info("Skipping DBT Cloud job execution and using mock run ID")
        mock_run_id = 9999
        return mock_run_id
    
    # Validate credentials before making API call
    if not dbt_config.api_token:
        raise Exception("DBT Cloud API token is missing. Please check your .env file.")
    if not dbt_config.account_id:
        raise Exception("DBT Cloud Account ID is missing. Please check your .env file.")
    if not dbt_config.job_id:
        raise Exception("DBT Cloud Job ID is missing. Please check your .env file.")
    
    # Use the correct URL format for your DBT Cloud instance
    url = f"https://xn636.us1.dbt.com/api/v2/accounts/{dbt_config.account_id}/jobs/{dbt_config.job_id}/run"
    context.log.info(f"Using DBT Cloud API URL: {url}")
    
    payload = {"cause": f"Triggered from Dagster run {context.run_id}"}
    headers = {"Authorization": f"Token {dbt_config.api_token}", "Content-Type": "application/json"}
    
    try:
        # Execute the API call
        context.log.info(f"Triggering DBT Cloud job with job_id: {dbt_config.job_id}")
        resp = requests.post(url, json=payload, headers=headers)
        
        # Log response status and content for debugging
        context.log.info(f"DBT API Response Status: {resp.status_code}")
        try:
            json_response = resp.json()
            context.log.info(f"API Response: {str(json_response)}")
        except Exception as json_err:
            context.log.error(f"Failed to parse JSON response: {json_err}")
            context.log.info(f"Raw response content: {resp.text}")
        
        resp.raise_for_status()
        
        run_id = resp.json()["data"]["id"]
        context.log.info(f"DBT Cloud job triggered successfully with run_id: {run_id}")
        return run_id
        
    except requests.exceptions.HTTPError as http_err:
        context.log.error(f"HTTP error occurred: {http_err}")
        context.log.error(f"Response content: {resp.text}")
        raise
    except Exception as e:
        context.log.error(f"Error triggering DBT Cloud job: {e}")
        raise

@op(ins={"dbt_run_id": In(int)}, out=Out(Dict))
def monitor_dbt_job(context: OpExecutionContext, dbt_run_id: int, dbt_config: DBTCloudResource) -> Dict:
    """Monitor DBT job execution and return final status"""
    if dbt_config.skip_dbt:
        return {"status": "skipped", "run_id": dbt_run_id}

    # Always initialize project_id from environment at the start
    project_id = os.getenv("DBT_CLOUD_PROJECT_ID", "70471823469423")

    initial_delay = 60  # 1 minute initial delay
    check_interval = 10  # 10 seconds between status checks
    max_duration = 1800  # 30 minutes maximum total duration

    url = f"https://xn636.us1.dbt.com/api/v2/accounts/{dbt_config.account_id}/runs/{dbt_run_id}/"
    headers = {"Authorization": f"Token {dbt_config.api_token}", "Content-Type": "application/json"}

    # Print monitoring start message with clear formatting
    print("\n" + "="*50)
    print("Starting DBT Job Monitoring")
    print("="*50)
    print(f"Run ID: {dbt_run_id}")
    print(f"Account ID: {dbt_config.account_id}")
    print(f"Job ID: {dbt_config.job_id}")
    print(f"Initial delay: {initial_delay} seconds")
    print(f"Check interval: {check_interval} seconds")
    print(f"DBT Cloud URL: https://xn636.us1.dbt.com/deploy/{dbt_config.account_id}/pipeline/runs/{dbt_run_id}")
    print("="*50 + "\n")

    # Initial delay to allow job to start
    print(f"Waiting {initial_delay} seconds for job to initialize...")
    time.sleep(initial_delay)
    print("Starting job monitoring...\n")

    start_time = time.time()
    last_known_logs = ""
    last_status = None
    error_found = False

    def print_timestamp_message(message, error=False):
        timestamp = datetime.now().strftime("%H:%M:%S")
        prefix = "❌" if error else "  "
        print(f"{timestamp}  {prefix} {message}")

    while (time.time() - start_time) < max_duration:
        try:
            resp = requests.get(url, headers=headers)
            resp.raise_for_status()
            data = resp.json().get("data", {})
            
            current_status = data.get("status_humanized", "").lower()  # Use status_humanized for better messages
            current_logs = data.get("logs", "")
            
            # Get detailed error message if available
            error_message = data.get("status_message", "")
            
            # Print status changes
            if current_status != last_status:
                status_message = f"Status: {current_status.upper()}"
                print_timestamp_message(status_message)
                last_status = current_status
            
            # Check for and display new logs
            if current_logs and current_logs != last_known_logs:
                new_log_content = current_logs[len(last_known_logs):] if last_known_logs else current_logs
                log_lines = new_log_content.strip().split('\n')
                
                for line in log_lines:
                    line = line.strip()
                    if line:
                        # Check for error indicators
                        is_error = any(err in line.lower() for err in [
                            "error", "failed", "invalid", 
                            "compilation error", "dbt command failed"
                        ])
                        
                        if is_error and not error_found:
                            error_found = True
                            print("\n" + "!"*50)
                            print("!! ERROR DETECTED !!")
                            print("!"*50 + "\n")
                        
                        print_timestamp_message(line, error=is_error)
                
                last_known_logs = current_logs
            
            # Handle job completion
            if current_status in ["success", "error", "failed", "cancelled"]:
                is_success = current_status == "success"
                
                print("\n" + "="*50)
                if is_success:
                    print("✅ DBT Job Completed Successfully!")
                else:
                    print("❌ DBT Job Failed!")
                    print("\nError Details:")
                    
                    # Extract and format error information
                    error_details = []
                    
                    # Add the main status message if available
                    if error_message:
                        error_details.append(f"DBT Status: {error_message}")
                    
                    # Add any error from data
                    if "error" in data:
                        error_details.append(f"Error: {data['error']}")
                        
                    # Get project_id from environment variable
                    project_id = os.getenv("DBT_CLOUD_PROJECT_ID", "70471823469423")
                    
                    # Look for specific error patterns in logs
                    if current_logs:
                        log_lines = current_logs.split('\n')
                        in_error_section = False
                        context_lines = []
                        error_details.append("\nDetailed Error Information:")
                        
                        for line in log_lines:
                            line = line.strip()
                            if any(err in line.lower() for err in [
                                "error:", "failed:", "compilation error:",
                                "invalid identifier", "dbt command failed",
                                "database error", "could not find"
                            ]):
                                if not in_error_section:
                                    in_error_section = True
                                    if context_lines:
                                        error_details.extend(context_lines[-2:])  # Add last 2 lines of context
                                        context_lines = []
                                error_details.append(line)
                            elif in_error_section and line:
                                error_details.append(line)
                                if len(line) < 3 or "----" in line:  # End of error section
                                    in_error_section = False
                            else:
                                context_lines.append(line)
                                if len(context_lines) > 5:
                                    context_lines.pop(0)
                    
                    if error_details:
                        print("\n=== DBT Cloud Error Details ===")
                        print("\n".join(error_details))
                        print("\nView full logs at:")
                        print(f"https://xn636.us1.dbt.com/deploy/{dbt_config.account_id}/projects/{project_id}/runs/{dbt_run_id}")
                    
                print("="*50 + "\n")
                
                result = {
                    "status": current_status,
                    "run_id": dbt_run_id,
                    "finished_at": data.get("finished_at"),
                    "job_id": data.get("job_id"),
                    "git_branch": data.get("git_branch"),
                    "project_id": project_id,
                    "error_details": "\n".join(error_details) if not is_success else "",
                    "status_message": error_message,
                    "dbt_logs": current_logs,
                    "run_url": f"https://xn636.us1.dbt.com/deploy/{dbt_config.account_id}/projects/{project_id}/runs/{dbt_run_id}",
                    "is_success": is_success
                }
                
                # If job failed, add error details to the result
                if not is_success:
                    print("\n=== Detailed Error Information ===")
                    print(result["error_details"])
                    print(f"\nView full logs at: {result['run_url']}")
                    
                    # Prepare comprehensive error message for the result
                    error_message = f"DBT Cloud job failed:\n\n{'='*50}\n{result['error_details']}\n{'='*50}"
                    error_message += f"\n\nView full logs at: {result['run_url']}"
                    result["error_details"] = error_message
                
                return result
            
            time.sleep(check_interval)
            
        except requests.exceptions.RequestException as e:
            print_timestamp_message(f"HTTP error while checking job status: {str(e)}", error=True)
            time.sleep(check_interval)
        except Exception as e:
            if "DBT Cloud job failed" in str(e):
                raise  # Re-raise DBT failures to trigger email
            print_timestamp_message(f"Error checking job status: {str(e)}", error=True)
            time.sleep(check_interval)
    
    timeout_msg = f"Job monitoring timed out after {max_duration} seconds"
    print_timestamp_message(timeout_msg, error=True)
    raise Exception(timeout_msg)

@op(ins={"job_status": In(Dict)}, out=Out(str))
def send_email_notification(context: OpExecutionContext, job_status: Dict, email_config: EmailResource, dbt_config: DBTCloudResource):
    """Send email notification with job status details"""
    max_retries = 3
    retry_delay = 5  # seconds
    
    # Prepare email message
    if dbt_config.skip_dbt:
        message_text = email_config.message
        context.log.info("Using custom email message (DBT skipped)")
    else:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        status = job_status.get("status", "unknown")
        status_emoji = "✅" if status == "success" else "❌"
        
        # Get the DBT logs if available
        dbt_logs = job_status.get("dbt_logs", "")
        if dbt_logs:
            # Extract the most relevant parts of the logs
            log_lines = dbt_logs.split('\n')
            important_logs = []
            
            # Look for the most important information in the logs
            error_section = False
            for line in log_lines:
                line = line.strip()
                # Always include lines with key information
                if any(key in line.lower() for key in ['error', 'failed', 'invalid', 'compilation error']):
                    error_section = True
                    important_logs.append(line)
                # Include context around errors
                elif error_section and line:
                    important_logs.append(line)
                    if len(line) < 3:  # Empty or separator line ends the error section
                        error_section = False
            
            # Combine the important log lines
            dbt_logs = "\n".join(important_logs)
            
            # Truncate logs if they're too long (email size limitation)
            max_log_length = 10000
            if len(dbt_logs) > max_log_length:
                dbt_logs = dbt_logs[:max_log_length] + "\n... (logs truncated, see DBT Cloud for full logs)"
        
        message_text = f"""
        DBT Cloud Job Run Notification {status_emoji}

        Job Status: {status.upper()}
        Status Message: {job_status.get('status_message', 'No status message')}
        Run ID: {job_status.get('run_id')}
        JOB_ID: {job_status.get('job_id', dbt_config.job_id)}
        GIT_BRANCH: {job_status.get('git_branch', 'N/A')}
        Account ID: {dbt_config.account_id}

        Finished At: {job_status.get('finished_at')}
        Current Time: {current_time}

        View in DBT Cloud: {job_status.get('run_url')}

        === Execution Details ===
        {job_status.get('error_details', 'No execution details available')}
        """
        
        # Add error details and logs if job failed
        if status != "success":
            message_text += f"""
        
        Error Details:
        {job_status.get('error_details', 'No error details available')}
        
        DBT Logs:
        {dbt_logs if dbt_logs else 'No DBT logs available'}
        """
        
        message_text += "\n\nThis is an automated notification from Dagster."
        context.log.info(f"Generated DBT job notification for status: {status}")
    
    # Create email service instance
    email_service = EmailService(
        sender_email=email_config.sender_email,
        password=email_config.password,
        smtp_server=email_config.smtp_server,
        smtp_port=email_config.smtp_port
    )
    
    context.log.info(f"Sending job status email to {email_config.recipient_email}")
    
    try:
        # Send email with job status
        email_service.send_email(
            recipient_email=email_config.recipient_email,
            subject=email_config.subject,
            message=message_text,
            job_result=job_status
        )
        return f"Email notification sent successfully to {email_config.recipient_email}"
    except Exception as e:
        error_msg = f"Failed to send email notification: {str(e)}"
        context.log.error(error_msg)
        raise Exception(error_msg)

@graph
def dbt_with_email_notification():
    """Graph that combines DBT job triggering with email notification"""
    run_id = trigger_dbt_cloud_job()
    job_status = monitor_dbt_job(run_id)
    send_email_notification(job_status)

# The job now uses the graph and specifies the required resources
@job
def dbt_notification_job():
    """Job that triggers a DBT Cloud job and sends email notification"""
    dbt_with_email_notification()

def load_env_config():
    """Load configuration from environment variables"""
    
    # Check for required environment variables
    required_vars = [
        "DBT_CLOUD_API_TOKEN",
        "DBT_CLOUD_ACCOUNT_ID", 
        "DBT_CLOUD_JOB_ID",
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
        "dbt_config": {
            "api_token": os.getenv("DBT_CLOUD_API_TOKEN"),
            "account_id": os.getenv("DBT_CLOUD_ACCOUNT_ID"),
            "job_id": os.getenv("DBT_CLOUD_JOB_ID"),
            "skip_dbt": os.getenv("SKIP_DBT", "false").lower() == "true"
        },
        "email_config": {
            "sender_email": os.getenv("EMAIL_SENDER"),
            "password": os.getenv("EMAIL_PASSWORD"),
            "recipient_email": os.getenv("EMAIL_RECIPIENT"),
            "subject": os.getenv("EMAIL_SUBJECT", "Dagster DBT Job Notification"),
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
    dbt_config_dict = config_data.get("dbt_config", {})
    email_config_dict = config_data.get("email_config", {})
    
    # Create resource instances
    dbt_resource = DBTCloudResource(
        api_token=dbt_config_dict["api_token"],
        account_id=dbt_config_dict["account_id"],
        job_id=dbt_config_dict["job_id"],
        skip_dbt=dbt_config_dict["skip_dbt"]
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
        jobs=[dbt_notification_job],
        resources={
            "dbt_config": dbt_resource,
            "email_config": email_resource
        }
    )

# Create the definitions for Dagster to discover
defs = create_definitions()

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run a Dagster job that triggers a DBT Cloud job and sends an email notification")
    parser.add_argument("--skip-dbt", action="store_true", help="Skip DBT Cloud job execution and only send email")
    parser.add_argument("--email-only", action="store_true", help="Same as --skip-dbt, for easier understanding")
    args = parser.parse_args()

    # Load configuration from environment
    config_data = load_env_config()
    
    # If configuration is missing, exit early
    if config_data is None:
        print("Exiting - please check your .env file or environment variables.")
        sys.exit(1)
    
    # Get configurations from environment
    dbt_config_dict = config_data.get("dbt_config", {})
    email_config_dict = config_data.get("email_config", {})
    
    # Override skip_dbt from command line args if specified
    if args.skip_dbt or args.email_only:
        dbt_config_dict["skip_dbt"] = True
        print("DBT Cloud job execution will be skipped (email-only mode).")
    
    # Create resource instances
    dbt_resource = DBTCloudResource(
        api_token=dbt_config_dict["api_token"],
        account_id=dbt_config_dict["account_id"],
        job_id=dbt_config_dict["job_id"],
        skip_dbt=dbt_config_dict["skip_dbt"]
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
    if dbt_resource.skip_dbt:
        print("Running in email-only mode (skipping DBT Cloud job)...")
    else:
        print("Running with DBT Cloud job execution...")
    
    # Create a temporary definitions object for direct execution
    temp_defs = Definitions(
        jobs=[dbt_notification_job],
        resources={
            "dbt_config": dbt_resource,
            "email_config": email_resource
        }
    )
    
    print("\nStarting job execution...")
    
    try:
        # Execute the job with our resource configuration
        result = temp_defs.get_job_def("dbt_notification_job").execute_in_process()
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