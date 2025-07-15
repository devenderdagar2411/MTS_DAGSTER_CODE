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
#from my_dagster_project.glue_endpoint_job     import glue_job_runner
#from my_dagster_project.glue_trigger_job_new  import glue_trigger_job
from my_dagster_project.dbt_cloud_job         import dbt_trigger_job

defs = Definitions(
    jobs=[
        #glue_job_runner,     # status‑check job (no config needed)
        #glue_trigger_job,    # trigger‑glue job  (no config needed)
        dbt_trigger_job,     # dbt Cloud job     (no config needed)
    ]
)

