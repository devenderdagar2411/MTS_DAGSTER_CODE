from dagster import op, job, graph

@op
def dbt_cloud_job_op(context):
    context.log.info("Running dbt_cloud_job (Job 1)")
    # Simulate success
    return "Job 1 Success"

@op
def dbt_cloud_job2_op(context, previous_result):
    context.log.info(f"Running dbt_cloud_job2 (Job 2) after: {previous_result}")
    # Simulate success
    return "Job 2 Success"

@graph
def chained_dbt_jobs():
    result1 = dbt_cloud_job_op()
    dbt_cloud_job2_op(result1)

@job
def dbt_cloud_job():
    chained_dbt_jobs()

# If you want to run only the second job after the first succeeds, you can also define them as separate jobs and use sensors/schedules for orchestration.

# Example usage:
# result = dbt_cloud_job.execute_in_process()
# print(result.success)
