# my_dagster_project/glue_endpoint_job.py
import os
import json
import yaml
import requests
import smtplib
from email.message import EmailMessage
from datetime import datetime
from dagster import op, job, Config, In, Out, graph, ResourceDefinition

# Email configuration class
class EmailConfig(Config):
    sender_email: str = "ateeqh.rehman@dynpro.com"
    password: str = "Habeeb@123"
    recipient_email: str = "sharwari.shinde@dynpro.com"
    subject: str = "Glue Job Status Notification"
    smtp_server: str = "smtp.office365.com"
    smtp_port: int = 587

# Glue configuration class
class GlueConfig(Config):
    job_name: str = "glu-ibmi-s3-to-ice-itclvt-UAT"
    job_run_id: str = "jr_2ad018d32dce3d07331593add1a7314e3cac56d38ad8c2b3c92917c9cdbbee4b"
    endpoint_url: str = "https://p0x5s9afkj.execute-api.us-east-1.amazonaws.com/V1/GET-glue-job-status"
    api_key: str = os.getenv("STATUS_API_KEY", "UUiSeD91pf42yKbcDPv6952g8YmiFYGe7nuazbt6")

@op(out=Out(dict), required_resource_keys={"glue_config"})
def call_glue_status_api(context):
    """Call the Glue job status API and return the response"""
    config = context.resources.glue_config
    
    # Prepare request
    payload = {"job_name": config.job_name, "job_run_id": config.job_run_id}
    headers = {"Content-Type": "application/json", "x-api-key": config.api_key}

    try:
        # Make the API call
        context.log.info(f"Checking status for Glue job: {config.job_name}")
        resp = requests.post(config.endpoint_url, json=payload, headers=headers)
        context.log.info(f"HTTP {resp.status_code} – {resp.text}")
        
        # Check for HTTP errors
        resp.raise_for_status()

        # Parse and log response
        data = resp.json()
        context.log.info(f"Parsed JSON:\n{json.dumps(data, indent=2)}")
        return data
    except requests.exceptions.HTTPError as http_err:
        context.log.error(f"HTTP error occurred: {http_err}")
        context.log.error(f"Response content: {resp.text}")
        raise
    except Exception as e:
        context.log.error(f"Error checking Glue job status: {e}")
        raise

@op(ins={"glue_status": In(dict)}, out=Out(str), required_resource_keys={"email_config", "glue_config"})
def prepare_email_message(context, glue_status):
    """Prepare the email message with Glue job status details"""
    email_config = context.resources.email_config
    glue_config = context.resources.glue_config
    
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    job_state = glue_status.get("State", "Unknown")
    
    message_text = f"""
    Glue Job Status Notification

    Job Name: {glue_config.job_name}
    Run ID: {glue_config.job_run_id}
    Status: {job_state}
    Time: {current_time}

    Full Status:
    {json.dumps(glue_status, indent=2)}

    This is an automated notification from Dagster.
    """
    
    context.log.info(f"Prepared email notification for job status: {job_state}")
    return message_text

@op(ins={"message_text": In(str)}, out=Out(str), required_resource_keys={"email_config"})
def send_email_notification(context, message_text):
    """Send the email notification"""
    config = context.resources.email_config
    
    msg = EmailMessage()
    msg.set_content(message_text)
    msg["Subject"] = config.subject
    msg["From"] = config.sender_email
    msg["To"] = config.recipient_email

    try:
        context.log.info(f"Sending email to {config.recipient_email}...")
        with smtplib.SMTP(config.smtp_server, config.smtp_port) as server:
            server.starttls()
            server.login(config.sender_email, config.password)
            server.send_message(msg)
            
        context.log.info("Email notification sent successfully!")
        return f"Email successfully sent to {config.recipient_email}"
    except Exception as e:
        context.log.error(f"Failed to send email: {e}")
        raise

@graph
def glue_status_notification():
    """Graph that combines Glue job status check with email notification"""
    status = call_glue_status_api()
    message = prepare_email_message(status)
    send_email_notification(message)

@job(resource_defs={
    "glue_config": ResourceDefinition.hardcoded_resource(None),
    "email_config": ResourceDefinition.hardcoded_resource(None)
})
def glue_job_runner():
    """Job that checks Glue job status and sends email notification"""
    glue_status_notification()

def load_or_create_config():
    """Load configuration from YAML file or create default"""
    config_path = os.path.join(os.path.dirname(__file__), "glue_config.yaml")
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
            "glue_config": {
                "job_name": "glu-ibmi-s3-to-ice-itclvt-UAT",
                "job_run_id": "jr_2ad018d32dce3d07331593add1a7314e3cac56d38ad8c2b3c92917c9cdbbee4b",
                "endpoint_url": "https://p0x5s9afkj.execute-api.us-east-1.amazonaws.com/V1/GET-glue-job-status",
                "api_key": os.getenv("STATUS_API_KEY", "UUiSeD91pf42yKbcDPv6952g8YmiFYGe7nuazbt6")
            },
            "email_config": {
                "sender_email": "ateeqh.rehman@dynpro.com",
                "password": "Habeeb@123",
                "recipient_email": "sharwari.shinde@dynpro.com",
                "subject": "Glue Job Status Notification",
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
            return None
        except Exception as e:
            print(f"Error creating default configuration: {e}")
    
    return config

if __name__ == "__main__":
    # Load or create configuration
    config = load_or_create_config()
    
    # If default config was just created, exit early
    if config is None:
        print("Exiting - please update the configuration file with your credentials first.")
        exit(0)
    
    # Get configurations from file
    glue_config_dict = config.get("glue_config", {})
    email_config_dict = config.get("email_config", {})
    
    # Create config objects
    glue_config = GlueConfig(
        job_name=glue_config_dict.get("job_name"),
        job_run_id=glue_config_dict.get("job_run_id"),
        endpoint_url=glue_config_dict.get("endpoint_url"),
        api_key=glue_config_dict.get("api_key")
    )
    
    email_config = EmailConfig(
        sender_email=email_config_dict.get("sender_email"),
        password=email_config_dict.get("password"),
        recipient_email=email_config_dict.get("recipient_email"),
        subject=email_config_dict.get("subject"),
        smtp_server=email_config_dict.get("smtp_server"),
        smtp_port=email_config_dict.get("smtp_port")
    )
    
    # Define resource overrides with our config instances
    resource_defs = {
        "glue_config": ResourceDefinition.hardcoded_resource(glue_config),
        "email_config": ResourceDefinition.hardcoded_resource(email_config)
    }
    
    try:
        # Execute job with resource overrides
        result = glue_job_runner.execute_in_process(resources=resource_defs)
        
        if result.success:
            print("✅ Job completed successfully!")
            # Print any output from the ops
            for event in result.all_events:
                if hasattr(event, 'event_specific_data') and hasattr(event.event_specific_data, 'compute_result'):
                    print(f"Output from op {event.solid_handle.path[-1]}: {event.event_specific_data.compute_result}")
        else:
            print("❌ Job failed!")
            for failure in result.all_failures():
                print(f"  Error: {failure}")
    except Exception as e:
        print(f"❌ An error occurred: {e}")
        import traceback
        traceback.print_exc()