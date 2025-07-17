# # my_dagster_project/definitions.py
# from dagster import Definitions
# from my_dagster_project.glue_trigger_job import glue_job_runner, AwsGlueResource
# from my_dagster_project.dbt_cloud_job import dbt_trigger_job

# defs = Definitions(
#     jobs=[glue_job_runner, dbt_trigger_job],
#     resources={
#         # 👉 Supply Glue resource config here or via run‑config
#         "glue": AwsGlueResource(
#             job_name="my_glue_job",
#             arguments={"--key": "value"},
#             wait_for_completion=True,
#             timeout_minutes=30,
#         )
#     },
# )


# # my_dagster_project/definitions.py
# from dagster import Definitions
# from my_dagster_project.glue_endpoint_job import glue_job_runner
# from my_dagster_project.dbt_cloud_job   import dbt_trigger_job

# defs = Definitions(jobs=[glue_job_runner, dbt_trigger_job])


# my_dagster_project/definitions.py
from dagster import Definitions
from dotenv import load_dotenv
#from my_dagster_project.glue_endpoint_job     import glue_job_runner
#from my_dagster_project.glue_trigger_job_new  import glue_trigger_job

from my_dagster_project.dbt_cloud_job         import dbt_notification_job
from my_dagster_project.chained_dbt_jobs      import chained_dbt_jobs

from my_dagster_project.dbt_cloud_job import DBTCloudResource, EmailResource
import os
load_dotenv()

defs = Definitions(
    jobs=[
        #glue_job_runner,     # status‑check job (no config needed)
        #glue_trigger_job,    # trigger‑glue job  (no config needed)
        dbt_notification_job,     # dbt Cloud job     (no config needed)
        chained_dbt_jobs,    # chained job dependency (dbt_cloud_job -> dbt_cloud_job1)
    ],
    resources={
        "dbt_config": DBTCloudResource(
            api_token=os.getenv("DBT_CLOUD_API_TOKEN", ""),
            account_id=os.getenv("DBT_CLOUD_ACCOUNT_ID", ""),
            job_id=os.getenv("DBT_CLOUD_JOB_ID", ""),
            skip_dbt=os.getenv("SKIP_DBT", "false").lower() == "true"
        ),
        "email_config": EmailResource(
            sender_email=os.getenv("EMAIL_SENDER", ""),
            password=os.getenv("EMAIL_PASSWORD", ""),
            recipient_email=os.getenv("EMAIL_RECIPIENT", ""),
            subject=os.getenv("EMAIL_SUBJECT", "Dagster DBT Job Notification"),
            message=os.getenv("EMAIL_MESSAGE", "This is an automated notification from Dagster."),
            smtp_server=os.getenv("SMTP_SERVER", "smtp.office365.com"),
            smtp_port=int(os.getenv("SMTP_PORT", "587"))
        )
    }
)

