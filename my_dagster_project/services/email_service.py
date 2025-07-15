import smtplib
from email.message import EmailMessage
from datetime import datetime
from typing import Optional

class EmailService:
    def __init__(self, sender_email, password, smtp_server, smtp_port):
        self.sender_email = sender_email
        self.password = password
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port

    def create_job_status_email(self, job_result: dict, recipient_email: str) -> EmailMessage:
        """Create an email message with job status details"""
        status = job_result.get("status", "unknown").upper()
        is_success = job_result.get("is_success", False)
        
        # Create HTML content
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        status_emoji = "✅" if is_success else "❌"
        status_color = "#28a745" if is_success else "#dc3545"
        
        # Get job details
        job_id = job_result.get('job_id', 'Unknown Job ID')
        git_branch = job_result.get('git_branch', 'Unknown Branch')
        environment = git_branch
        run_id = job_result.get('run_id', 'N/A')
        finished_at = job_result.get('finished_at', 'N/A')

        html_content = f"""
        <html>
        <body style="font-family: Arial, sans-serif; line-height: 1.6; max-width: 800px; margin: 0 auto;">
            <h2 style="color: {status_color};">DBT Cloud Job Status: {status} {status_emoji}</h2>
            
            <div style="background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin: 10px 0;">
                <h3 style="margin-top: 0;">Job Details</h3>
                <table style="width: 100%; border-collapse: collapse;">
                    <tr>
                        <td style="padding: 8px; border-bottom: 1px solid #dee2e6;"><strong>Job ID:</strong></td>
                        <td style="padding: 8px; border-bottom: 1px solid #dee2e6;">{job_id}</td>
                    </tr>
                    <tr>
                        <td style="padding: 8px; border-bottom: 1px solid #dee2e6;"><strong>Environment:</strong></td>
                        <td style="padding: 8px; border-bottom: 1px solid #dee2e6;">{environment}</td>
                    </tr>
                    <tr>
                        <td style="padding: 8px; border-bottom: 1px solid #dee2e6;"><strong>Run ID:</strong></td>
                        <td style="padding: 8px; border-bottom: 1px solid #dee2e6;">{run_id}</td>
                    </tr>
                    <tr>
                        <td style="padding: 8px; border-bottom: 1px solid #dee2e6;"><strong>Finished At:</strong></td>
                        <td style="padding: 8px; border-bottom: 1px solid #dee2e6;">{finished_at}</td>
                    </tr>
                    <tr>
                        <td style="padding: 8px; border-bottom: 1px solid #dee2e6;"><strong>Current Time:</strong></td>
                        <td style="padding: 8px; border-bottom: 1px solid #dee2e6;">{current_time}</td>
                    </tr>
                </table>
            </div>
        """

        # Add error details if job failed
        if not is_success and job_result.get('error_details'):
            html_content += f"""
            <div style="background-color: #fff3cd; padding: 15px; border-radius: 5px; margin: 10px 0;">
                <h3 style="color: #856404;">Error Details</h3>
                <pre style="background-color: #fff; padding: 10px; border-radius: 3px; white-space: pre-wrap;">{job_result.get('error_details')}</pre>
            </div>
            """

        # Add DBT logs if available
        if job_result.get('dbt_logs'):
            html_content += f"""
            <div style="margin: 10px 0;">
                <h3>DBT Execution Logs</h3>
                <pre style="background-color: #f8f9fa; padding: 10px; border-radius: 3px; white-space: pre-wrap; max-height: 300px; overflow-y: auto;">{job_result.get('dbt_logs')}</pre>
            </div>
            """

        # Add DBT Cloud URL
        if job_result.get('run_url'):
            html_content += f"""
            <p style="margin-top: 20px;">
                <a href="{job_result.get('run_url')}" style="background-color: #007bff; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px;">
                    View in DBT Cloud
                </a>
            </p>
            """

        html_content += """
            <p style="color: #6c757d; font-size: 0.9em; margin-top: 20px;">
                This is an automated notification from Dagster.
            </p>
        </body>
        </html>
        """

        msg = EmailMessage()
        msg.set_content(job_result.get('status_message', 'No message available'))  # Plain text fallback
        msg.add_alternative(html_content, subtype='html')
        
        # Set email headers with detailed job information
        status_prefix = "✅ Success" if is_success else "❌ Failed"
        job_id = job_result.get('job_id', 'Unknown Job ID')
        environment = job_result.get('git_branch', 'Unknown Branch')
        msg["Subject"] = f"DBT Cloud Job {status_prefix} - {job_id} (Environment: {environment})"
        msg["From"] = self.sender_email
        msg["To"] = recipient_email
        
        return msg

    def send_email(self, recipient_email: str, subject: str, message: str, job_result: Optional[dict] = None):
        """Send an email with optional job status details"""
        try:
            if job_result:
                msg = self.create_job_status_email(job_result, recipient_email)
            else:
                msg = EmailMessage()
                msg.set_content(message)
                msg["Subject"] = subject
                msg["From"] = self.sender_email
                msg["To"] = recipient_email

            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                # Enable debug output
                server.set_debuglevel(1)
                
                # Initial EHLO
                server.ehlo()
                
                # Start TLS
                if server.has_extn('starttls'):
                    server.starttls()
                    # Second EHLO after TLS
                    server.ehlo()
                else:
                    print("Warning: STARTTLS not supported by SMTP server")
                
                # Authenticate
                server.login(self.sender_email, self.password)
                
                # Send message
                server.send_message(msg)
                print(f"Email sent successfully to {recipient_email}")
            return True
        except Exception as e:
            error_msg = f"Failed to send email: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)
