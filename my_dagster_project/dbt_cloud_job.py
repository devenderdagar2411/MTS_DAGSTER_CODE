# my_dagster_project/glue_endpoint_job.py
import os
import sys
import yaml # Used for loading config.yaml in __main__ block
import requests
import argparse
import smtplib
import time
import re # For regex in monitor_dbt_job
from email.message import EmailMessage
from datetime import datetime
from typing import Dict, Optional, List

from dagster import (
    op,
    job,
    Config,
    In,
    Out,
    graph,
    ResourceDefinition,
    OpExecutionContext, # Added for type hinting
    get_dagster_logger # Added for logging within ops
)

# Add the project root to the Python path to resolve sibling imports like 'resources'
# This is crucial when running with `dagster dev` or `execute_in_process`
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import your EmailService class (assuming it's still located here)
from my_dagster_project.services.email_service import EmailService

# Import your resource definitions from the new resources.py file
# We are importing the @resource decorated function directly
from resources.resources import email_service_resource

# Email configuration class using dagster Config
class EmailConfig(Config):
    sender_email: str = "ateeqh.rehman@dynpro.com"
    password: str = "Habeeb@123"  # Use an app password, not your main password
    recipient_email: str = "sharwari.shinde@dynpro.com"
    subject: str = "Dagster Notification"
    message: str = "This is a test notification from Dagster! Sent on July 14, 2025."
    smtp_server: str = "smtp.office365.com"
    smtp_port: int = 587

# DBT Cloud configuration (defaults to environment variables if available)
class DBTConfig(Config):
    api_token: str = os.getenv("DBT_CLOUD_API_TOKEN", "YOUR_DBT_CLOUD_API_TOKEN")
    account_id: str = os.getenv("DBT_CLOUD_ACCOUNT_ID", "YOUR_DBT_CLOUD_ACCOUNT_ID")
    job_id: str = os.getenv("DBT_CLOUD_JOB_ID", "YOUR_DBT_CLOUD_JOB_ID")
    skip_dbt: bool = False  # Set to True to skip DBT Cloud job execution

@op(out=Out(int), required_resource_keys={"dbt_config"})
def trigger_dbt_cloud_job(context: OpExecutionContext):
    """Trigger a dbt Cloud job run or generate mock run ID."""
    config = context.resources.dbt_config
    
    # Debug: Print credentials for troubleshooting
    context.log.info(f"API Token: {config.api_token[:5]}...{config.api_token[-5:]} (redacted middle)")
    context.log.info(f"Account ID: {config.account_id}")
    context.log.info(f"Job ID: {config.job_id}")
    
    # Check if we should skip DBT Cloud job execution
    if config.skip_dbt:
        context.log.info("Skipping DBT Cloud job execution and using mock run ID")
        mock_run_id = 9999
        return mock_run_id
    
    # Validate credentials before making API call
    if not config.api_token or config.api_token == "YOUR_DBT_CLOUD_API_TOKEN":
        raise Exception("DBT Cloud API token is missing or default. Please update your config.yaml.")
    if not config.account_id or config.account_id == "YOUR_DBT_CLOUD_ACCOUNT_ID":
        raise Exception("DBT Cloud Account ID is missing or default. Please update your config.yaml.")
    if not config.job_id or config.job_id == "YOUR_DBT_CLOUD_JOB_ID":
        raise Exception("DBT Cloud Job ID is missing or default. Please update your config.yaml.")
    
    # Use the correct URL format for your DBT Cloud instance
    url = f"https://xn636.us1.dbt.com/api/v2/accounts/{config.account_id}/jobs/{config.job_id}/run"
    context.log.info(f"Using DBT Cloud API URL: {url}")
    
    payload = {"cause": f"Triggered from Dagster run {context.run_id}"}
    headers = {"Authorization": f"Token {config.api_token}", "Content-Type": "application/json"}
    
    try:
        # Execute the API call
        context.log.info(f"Triggering DBT Cloud job with job_id: {config.job_id}")
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

# This op is not used in the graph, but kept for completeness if you decide to use it.
@op(ins={"run_id": In(int)}, out=Out(Dict), required_resource_keys={"dbt_config"})
def get_dbt_job_status(context: OpExecutionContext, run_id: int) -> Dict:
    """Poll DBT Cloud job status until completion or timeout"""
    config = context.resources.dbt_config
    max_retries = 30  # Maximum number of status check attempts
    retry_interval = 20  # Seconds between status checks
    
    url = f"https://xn636.us1.dbt.com/api/v2/accounts/{config.account_id}/runs/{run_id}/"
    headers = {"Authorization": f"Token {config.api_token}", "Content-Type": "application/json"}
    
    for attempt in range(max_retries):
        try:
            context.log.info(f"Checking job status (attempt {attempt + 1}/{max_retries})...")
            resp = requests.get(url, headers=headers)
            resp.raise_for_status()
            status_data = resp.json()["data"]
            
            # --- FIX: Use 'status_humanized' for string comparison ---
            status = status_data.get("status_humanized", "unknown").lower()
            
            if status in ["success", "failed", "cancelled", "error"]:
                context.log.info(f"Job completed with status: {status}")
                return {
                    "status": status,
                    "run_id": run_id,
                    "finished_at": status_data.get("finished_at"),
                    "status_message": status_data.get("status_message"),
                    "job_name": status_data.get("job_name"),
                    "environment": status_data.get("environment"),
                    "error_details": status_data.get("status_message")
                }
            
            context.log.info(f"Current status: {status}, waiting {retry_interval} seconds...")
            time.sleep(retry_interval)
            
        except Exception as e:
            context.log.error(f"Error checking job status: {e}")
            if attempt == max_retries - 1:
                raise
            time.sleep(retry_interval)
    
    raise Exception(f"Job status check timed out after {max_retries * retry_interval} seconds")

def get_dbt_run_logs(context: OpExecutionContext, account_id: str, run_id: int, api_token: str) -> str:
    """Fetch the logs from a DBT Cloud run"""
    artifacts_url = f"https://xn636.us1.dbt.com/api/v2/accounts/{account_id}/runs/{run_id}/artifacts/"
    run_url = f"https://xn636.us1.dbt.com/api/v2/accounts/{account_id}/runs/{run_id}/"
    headers = {"Authorization": f"Token {api_token}", "Content-Type": "application/json"}
    
    try:
        # First, get the run details to check for immediate errors
        run_resp = requests.get(run_url, headers=headers)
        run_resp.raise_for_status()
        run_data = run_resp.json().get("data", {})
        
        # Get raw logs from the run details
        raw_logs = run_data.get("logs", "")
        if raw_logs:
            context.log.info("Found raw logs in run details")
        
        full_logs = []
        if raw_logs:
            full_logs.append("=== DBT Execution Logs ===")
            full_logs.append(raw_logs)
            full_logs.append("\n")
            
        # Get list of artifacts
        artifacts_resp = requests.get(artifacts_url, headers=headers)
        artifacts_resp.raise_for_status()
        artifacts = artifacts_resp.json().get("data", [])
        
        # Look for specific log files
        for artifact in artifacts:
            artifact_path = artifact.get("path", "")
            if any(log_type in artifact_path.lower() for log_type in ["logs.txt", "run_results.json", "manifest.json", "compiled"]):
                # Get the content of each artifact
                artifact_url = f"{artifacts_url}{artifact_path}"
                artifact_resp = requests.get(artifact_url, headers=headers)
                if artifact_resp.status_code == 200:
                    try:
                        if artifact_path.endswith('.json'):
                            content = artifact_resp.json()
                            if "results" in content:  # run_results.json
                                results = content.get("results", [])
                                
                                failed_results = [r for r in results if r.get("status") != "success"]
                                
                                if failed_results:
                                    full_logs.append("\n=== Failed Model Details ===")
                                    for result in failed_results:
                                        full_logs.append(f"\nModel: {result.get('unique_id')}")
                                        full_logs.append(f"Status: {result.get('status')}")
                                        
                                        # Extract detailed error information
                                        if "message" in result:
                                            full_logs.append(f"Error Message: {result.get('message')}")
                                        if "compiled_sql" in result:
                                            full_logs.append("\nCompiled SQL:")
                                            full_logs.append(result.get('compiled_sql'))
                        
                        elif "compiled" in artifact_path.lower() and artifact_path.endswith('.sql'):
                            # This is a compiled SQL file that might have caused an error
                            full_logs.append(f"\n=== Compiled SQL for {artifact_path} ===")
                            full_logs.append(artifact_resp.text)
                            
                        elif artifact_path.endswith('logs.txt'):
                            # Process the main log file
                            log_content = artifact_resp.text
                            full_logs.append("\n=== DBT Execution Log ===")
                            full_logs.append(log_content)
                            
                            # Look for specific error patterns in the logs
                            error_lines = [line for line in log_content.split('\n') 
                                           if any(error_term in line.lower() 
                                                  for error_term in ['error', 'failed', 'compilation error', 'invalid'])]
                            if error_lines:
                                full_logs.append("\n=== Extracted Error Messages ===")
                                full_logs.extend(error_lines)
                            
                    except Exception as e:
                        context.log.warning(f"Error processing artifact {artifact_path}: {str(e)}")
                        if artifact_path.endswith('.txt'):
                            full_logs.append(artifact_resp.text)
        
        log_content = "\n".join(full_logs)
        
        # If we have no logs but have an error message in the run data, use that
        if not log_content.strip() and run_data.get("status_message"):
            log_content = f"DBT Error Message: {run_data.get('status_message')}"
        
        return log_content if log_content.strip() else "No logs available"
    except Exception as e:
        context.log.error(f"Error fetching DBT run logs: {str(e)}")
        return f"Error fetching logs: {str(e)}"

@op(ins={"dbt_run_id": In(int)}, out=Out(Dict), required_resource_keys={"dbt_config"})
def monitor_dbt_job(context: OpExecutionContext, dbt_run_id: int) -> Dict:
    """Monitor DBT job execution and return final status"""
    if context.resources.dbt_config.skip_dbt:
        return {"status": "skipped", "run_id": dbt_run_id}
    
    config = context.resources.dbt_config
    initial_delay = 60  # 1 minute initial delay
    check_interval = 10  # 10 seconds between status checks
    max_duration = 1800  # 30 minutes maximum total duration
    
    url = f"https://xn636.us1.dbt.com/api/v2/accounts/{config.account_id}/runs/{dbt_run_id}/"
    headers = {"Authorization": f"Token {config.api_token}", "Content-Type": "application/json"}
    
    # Print monitoring start message with clear formatting
    print("\n" + "="*50)
    print("Starting DBT Job Monitoring")
    print("="*50)
    print(f"Run ID: {dbt_run_id}")
    print(f"Account ID: {config.account_id}")
    print(f"Job ID: {config.job_id}")
    print(f"Initial delay: {initial_delay} seconds")
    print(f"Check interval: {check_interval} seconds")
    print(f"DBT Cloud URL: https://xn636.us1.dbt.com/deploy/{config.account_id}/pipeline/runs/{dbt_run_id}")
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
                        
                    # Get detailed run information
                    try:
                        run_details_url = f"https://xn636.us1.dbt.com/api/v2/accounts/{config.account_id}/runs/{dbt_run_id}/"
                        run_details_resp = requests.get(run_details_url, headers=headers)
                        if run_details_resp.status_code == 200:
                            run_details = run_details_resp.json().get("data", {})
                            if "status_humanized" in run_details:
                                error_details.append(f"Status Details: {run_details['status_humanized']}")
                            if "status_message" in run_details:
                                error_details.append(f"Status Message: {run_details['status_message']}")
                    except Exception as e:
                        error_details.append(f"Failed to fetch detailed status: {str(e)}")
                    
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
                        print(f"https://xn636.us1.dbt.com/deploy/{config.account_id}/pipeline/runs/{dbt_run_id}")
                    
                print("="*50 + "\n")
                
                result = {
                    "status": current_status,
                    "run_id": dbt_run_id,
                    "finished_at": data.get("finished_at"),
                    "job_name": data.get("job_name"),
                    "environment": data.get("environment"),
                    "error_details": "\n".join(error_details) if not is_success else "",
                    "status_message": error_message,
                    "dbt_logs": current_logs,
                    "run_url": f"https://xn636.us1.dbt.com/deploy/{config.account_id}/pipeline/runs/{dbt_run_id}",
                    "is_success": is_success
                }
                
                # Prepare the result with all information
                result = {
                    "status": current_status,
                    "run_id": dbt_run_id,
                    "finished_at": data.get("finished_at"),
                    "job_name": data.get("job_name"),
                    "environment": data.get("environment"),
                    "error_details": "\n".join(error_details) if not is_success else "",
                    "status_message": error_message,
                    "dbt_logs": current_logs,
                    "run_url": f"https://xn636.us1.dbt.com/deploy/{config.account_id}/pipeline/runs/{dbt_run_id}",
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

@op(ins={"job_status": In(Dict)}, out=Out(str), required_resource_keys={"email_config", "dbt_config"})
def prepare_email_message(context: OpExecutionContext, job_status: Dict):
    """Prepare the email message content with DBT job details or custom message"""
    email_config = context.resources.email_config
    dbt_config = context.resources.dbt_config
    
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
        Job Name: {job_status.get('job_name')}
        Environment: {job_status.get('environment')}
        Account ID: {dbt_config.account_id}
        Job ID: {dbt_config.job_id}
        
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
    
    context.log.info(f"Email subject: {email_config.subject}")
    return message_text

@op(ins={"job_status": In(Dict)}, out=Out(str), required_resource_keys={"email_config"})
def send_email_notification(context: OpExecutionContext, job_status: Dict):
    """Send email notification with job status details"""
    config = context.resources.email_config
    max_retries = 3
    retry_delay = 5  # seconds
    
    # Create email service instance
    email_service = EmailService(
        sender_email=config.sender_email,
        password=config.password,
        smtp_server=config.smtp_server,
        smtp_port=config.smtp_port
    )
    
    context.log.info(f"Sending job status email to {config.recipient_email}")
    
    try:
        # Send email with job status
        email_service.send_email(
            recipient_email=config.recipient_email,
            subject=config.subject,
            message=config.message,
            job_result=job_status
        )
        return f"Email notification sent successfully to {config.recipient_email}"
    except Exception as e:
        error_msg = f"Failed to send email notification: {str(e)}"
        context.log.error(error_msg)
        raise Exception(error_msg)

def test_email_config(email_config_instance):
    """Test email configuration by sending a test email"""
    print("\nTesting email configuration...")
    try:
        # Create SMTP connection
        with smtplib.SMTP(email_config_instance.smtp_server, email_config_instance.smtp_port, timeout=30) as server:
            server.set_debuglevel(1)  # Enable debug output
            print(f"Connected to SMTP server: {email_config_instance.smtp_server}:{email_config_instance.smtp_port}")
            
            # Initial EHLO
            server.ehlo()
            
            # Start TLS
            if not server.has_extn('starttls'):
                raise Exception("STARTTLS not supported by server")
            
            server.starttls()
            print("TLS connection established")
            
            # EHLO again after TLS
            server.ehlo()
            
            # Attempt login
            try:
                server.login(email_config_instance.sender_email, email_config_instance.password)
                print("Successfully logged in to SMTP server")
            except smtplib.SMTPAuthenticationError as auth_err:
                print(f"\n❌ Authentication failed. Error code: {auth_err.smtp_code}")
                print(f"Error message: {auth_err.smtp_error.decode() if hasattr(auth_err.smtp_error, 'decode') else auth_err.smtp_error}")
                raise
            
            # Create test message
            msg = EmailMessage()
            msg.set_content("This is a test email from Dagster")
            msg["Subject"] = "Dagster Email Test"
            msg["From"] = email_config_instance.sender_email
            msg["To"] = email_config_instance.recipient_email
            
            # Send test email
            server.send_message(msg)
            print("\n✅ Test email sent successfully!")
            return True
            
    except smtplib.SMTPAuthenticationError as auth_err:
        print(f"\n❌ Authentication failed: {str(auth_err)}")
        print("Please check your email credentials (sender email and password)")
        return False
    except Exception as e:
        print(f"\n❌ Email test failed: {str(e)}")
        return False

@graph
def dbt_with_email_notification():
    """Graph that combines DBT job triggering with email notification"""
    run_id = trigger_dbt_cloud_job()
    job_status = monitor_dbt_job(run_id)
    send_email_notification(job_status)  # Send email with job status directly

# The job now uses the graph and specifies the required resources
@job
def dbt_notification_job():
    """Job that triggers a DBT Cloud job and sends email notification"""
    dbt_with_email_notification()

def load_or_create_config():
    """Load configuration from YAML file or create default"""
    # This path is relative to where the script is run from (the project root)
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config.yaml")
    config = {}
    
    # Try to load existing config
    if os.path.exists(config_path):
        try:
            with open(config_path, "r") as f:
                config = yaml.safe_load(f)
            print(f"Configuration loaded from {config_path}")
        except Exception as e:
            print(f"Error loading configuration: {e}")
    else:
        # Create default config if file doesn't exist
        config = {
            "dbt_config": {
                "api_token": "dbtu_eSYPMSIP9HgMU6DfDOs2kz-grNCJvDgIw0VB_ZZFn2bpxbd2ng", # Replace with your actual token
                "account_id": "70471823461483", # Replace with your actual account ID
                "job_id": "70471823472253",     # Replace with your actual job ID
                "skip_dbt": False
            },
            "email_config": {
                "sender_email": "devender.dagar@dynpro.com",
                "password": "Buntydev@34",  # Use an app password, not your main password
                "recipient_email": "sharwari.shinde@dynpro.com",
                "subject": "Dagster Notification",
                "message": "This is a test notification from Dagster! Sent on July 14, 2025.",
                "smtp_server": "smtp.office365.com",
                "smtp_port": 587
            }
        }
        
        # Write default config to file
        try:
            with open(config_path, "w") as f:
                yaml.dump(config, f, default_flow_style=False)
            print(f"Default configuration created at {config_path}")
            print("Please update the configuration with your actual values before running again.")
            return None # Indicate that user needs to update config
        except Exception as e:
            print(f"Error creating default configuration: {e}")
    
    return config

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run a Dagster job that triggers a DBT Cloud job and sends an email notification")
    parser.add_argument("--skip-dbt", action="store_true", help="Skip DBT Cloud job execution and only send email")
    parser.add_argument("--email-only", action="store_true", help="Same as --skip-dbt, for easier understanding")
    args = parser.parse_args()

    # Load or create configuration
    config_data = load_or_create_config()
    
    # If default config was just created, exit early
    if config_data is None:
        print("Exiting - please update the configuration file with your credentials first.")
        sys.exit(0)
    
    # Get configurations from file
    dbt_config_dict = config_data.get("dbt_config", {})
    email_config_dict = config_data.get("email_config", {})
    
    # Override skip_dbt from command line args if specified
    if args.skip_dbt or args.email_only:
        dbt_config_dict["skip_dbt"] = True
        print("DBT Cloud job execution will be skipped (email-only mode).")
    
    # Create config objects
    dbt_config_instance = DBTConfig(
        api_token=dbt_config_dict.get("api_token"),
        account_id=dbt_config_dict.get("account_id"),
        job_id=dbt_config_dict.get("job_id"),
        skip_dbt=dbt_config_dict.get("skip_dbt", False)
    )
    
    email_config_instance = EmailConfig(
        sender_email=email_config_dict.get("sender_email"),
        password=email_config_dict.get("password"),
        recipient_email=email_config_dict.get("recipient_email"),
        subject=email_config_dict.get("subject"),
        message=email_config_dict.get("message", "This is a notification from Dagster."),
        smtp_server=email_config_dict.get("smtp_server"),
        smtp_port=email_config_dict.get("smtp_port")
    )
    
    # Show execution mode
    if dbt_config_instance.skip_dbt:
        print("Running in email-only mode (skipping DBT Cloud job)...")
    else:
        # Check if DBT configurations have default values
        if dbt_config_instance.api_token == "YOUR_DBT_CLOUD_API_TOKEN":
            print("Warning: Default DBT Cloud API token detected.")
            print("Please update the configuration file with your actual DBT Cloud credentials.")
            sys.exit(1)
    
    # Check email credentials
    if email_config_instance.password == "YOUR_APP_PASSWORD":
        print("Warning: Default email password detected.")
        print("Please update the configuration file with your actual email credentials.")
        sys.exit(1)
    
    # Define resource overrides with our config instances
    resource_overrides = {
        "dbt_config": ResourceDefinition.hardcoded_resource(dbt_config_instance),
        "email_config": ResourceDefinition.hardcoded_resource(email_config_instance)
    }
    
    print("\nStarting job execution...")
    
    try:
        # Execute the job with our resource configuration
        result = dbt_notification_job.execute_in_process(
            run_config={},
            resources=resource_overrides
        )
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

