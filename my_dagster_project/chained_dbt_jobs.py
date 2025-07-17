# chained_dbt_jobs.py
from dagster import op, job, In, Out, DagsterEventType
from my_dagster_project.dbt_cloud_job import trigger_dbt_cloud_job, monitor_dbt_job, send_email_notification
from my_dagster_project.dbt_cloud_job1 import dbt_cloud_job1_op

@job
def chained_dbt_jobs():
    run_id = trigger_dbt_cloud_job()
    job_status = monitor_dbt_job(run_id)
    send_email_notification(job_status)
    # Chain the second job op, passing job_status to it
    # This will run regardless of success/failure of first job
    dbt_cloud_job1_op(job_status)

# dbt_cloud_job1.py - Updated implementation
from dagster import op

@op(
    ins={"job_status": In(dict)},
    out=Out(dict, is_required=False)
)
def dbt_cloud_job1_op(context, job_status):
    """
    Second DBT job that runs regardless of first job's success/failure
    """
    if job_status.get("is_success", False):
        context.log.info("First job succeeded. Running dbt_cloud_job1...")
        # Your actual logic for successful first job scenario
        try:
            # Example: trigger second DBT job
            # result = trigger_second_dbt_job()
            context.log.info("dbt_cloud_job1 completed successfully after first job success")
            return {"is_success": True, "message": "Second job completed after first job success"}
        except Exception as e:
            context.log.error(f"dbt_cloud_job1 failed after first job success: {str(e)}")
            return {"is_success": False, "message": f"Second job failed: {str(e)}"}
    else:
        context.log.info("First job failed. Running dbt_cloud_job1 with failure handling...")
        # Your actual logic for failed first job scenario
        try:
            # Example: run cleanup or alternative logic
            # result = run_cleanup_job()
            context.log.info("dbt_cloud_job1 completed successfully after first job failure")
            return {"is_success": True, "message": "Second job completed after first job failure"}
        except Exception as e:
            context.log.error(f"dbt_cloud_job1 failed after first job failure: {str(e)}")
            return {"is_success": False, "message": f"Second job failed: {str(e)}"}

# Alternative approach using try-except for more robust error handling
@job
def chained_dbt_jobs_with_error_handling():
    """
    Alternative implementation where second job runs only on first job success
    """
    run_id = trigger_dbt_cloud_job()
    job_status = monitor_dbt_job_with_error_handling(run_id)
    send_email_notification(job_status)
    # Second job will only run if first job succeeded
    dbt_cloud_job1_op(job_status)

@op(
    ins={"run_id": In(str)},
    out=Out(dict)
)
def monitor_dbt_job_with_error_handling(context, run_id):
    """
    Monitor DBT job and return status regardless of success/failure
    """
    try:
        # Your existing monitor logic
        # status = check_dbt_job_status(run_id)
        context.log.info(f"Monitoring DBT job {run_id}")
        
        # Mock implementation - replace with your actual logic
        # If job succeeds:
        # return {"is_success": True, "run_id": run_id, "message": "Job completed successfully"}
        
        # If job fails, you would return:
        # return {"is_success": False, "run_id": run_id, "message": "Job failed", "error": error_details}
        
        # Example return for successful job:
        return {"is_success": True, "run_id": run_id, "message": "Job completed successfully"}
        
    except Exception as e:
        context.log.error(f"Error monitoring DBT job {run_id}: {str(e)}")
        return {"is_success": False, "run_id": run_id, "message": f"Monitoring failed: {str(e)}"}

# Example of how to structure your actual DBT job operations
@op(
    out=Out(str)
)
def trigger_dbt_cloud_job():
    """
    Trigger the first DBT Cloud job
    """
    # Your implementation here
    # return dbt_run_id
    pass

@op(
    ins={"run_id": In(str)},
    out=Out(dict)
)
def monitor_dbt_job(context, run_id):
    """
    Monitor the DBT job and return status
    """
    # Your implementation here
    # return {"is_success": True/False, "run_id": run_id, ...}
    pass

@op(
    ins={"job_status": In(dict)}
)
def send_email_notification(context, job_status):
    """
    Send email notification based on job status
    """
    if job_status.get("is_success", False):
        context.log.info("Sending success notification")
        # Send success email
    else:
        context.log.info("Sending failure notification")
        # Send failure email