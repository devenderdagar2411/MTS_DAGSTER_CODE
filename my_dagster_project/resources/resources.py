from dagster import resource

@resource
def email_service_resource(init_context):
    # This is just a placeholder resource since the main functionality is in dbt_cloud_job.py
    return None
